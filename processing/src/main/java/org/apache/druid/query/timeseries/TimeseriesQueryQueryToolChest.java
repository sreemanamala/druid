/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.timeseries;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.frame.segment.FrameCursorUtils;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.ResultGranularTimestampComparator;
import org.apache.druid.query.ResultMergeQueryRunner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;

/**
 *
 */
public class TimeseriesQueryQueryToolChest extends QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private static final byte TIMESERIES_QUERY = 0x0;
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<>() {};
  private static final TypeReference<Result<TimeseriesResultValue>> TYPE_REFERENCE =
      new TypeReference<>() {};

  private final TimeseriesQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public TimeseriesQueryQueryToolChest()
  {
    this(DefaultTimeseriesQueryMetricsFactory.instance());
  }

  @Inject
  public TimeseriesQueryQueryToolChest(TimeseriesQueryMetricsFactory queryMetricsFactory)
  {
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeResults(
      QueryRunner<Result<TimeseriesResultValue>> queryRunner
  )
  {
    final QueryRunner<Result<TimeseriesResultValue>> resultMergeQueryRunner = new ResultMergeQueryRunner<>(
        queryRunner,
        this::createResultComparator,
        this::createMergeFn
    )
    {
      @Override
      public Sequence<Result<TimeseriesResultValue>> doRun(
          QueryRunner<Result<TimeseriesResultValue>> baseRunner,
          QueryPlus<Result<TimeseriesResultValue>> queryPlus,
          ResponseContext context
      )
      {
        int limit = ((TimeseriesQuery) queryPlus.getQuery()).getLimit();
        Sequence<Result<TimeseriesResultValue>> result = super.doRun(
            baseRunner,
            // Don't do post aggs until makePostComputeManipulatorFn() is called
            queryPlus.withQuery(((TimeseriesQuery) queryPlus.getQuery()).withPostAggregatorSpecs(ImmutableList.of())),
            context
        );
        if (limit < Integer.MAX_VALUE) {
          return result.limit(limit);
        }
        return result;
      }
    };

    return (queryPlus, responseContext) -> {
      final TimeseriesQuery query = (TimeseriesQuery) queryPlus.getQuery();
      final Sequence<Result<TimeseriesResultValue>> baseResults = resultMergeQueryRunner.run(
          queryPlus.withQuery(
              queryPlus.getQuery()
                       .withOverriddenContext(
                           ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, false)
                       )
          ),
          responseContext
      );

      final Sequence<Result<TimeseriesResultValue>> finalSequence;

      // When granularity = ALL, there is no grouping key for this query.
      // To be more sql-compliant, we should return something (e.g., 0 for count queries) even when
      // the sequence is empty.
      if (query.getGranularity().equals(Granularities.ALL) &&
          // Returns empty sequence if this query allows skipping empty buckets
          !query.isSkipEmptyBuckets() &&
          // Returns empty sequence if bySegment is set because bySegment results are mostly used for
          // caching in historicals or debugging where the exact results are preferred.
          !query.context().isBySegment()) {
        // Usally it is NOT Okay to materialize results via toList(), but Granularity is ALL thus
        // we have only one record.
        final List<Result<TimeseriesResultValue>> val = baseResults.toList();
        finalSequence = val.isEmpty()
                        ? Sequences.simple(Collections.singletonList(getEmptyTimeseriesResultValue(query)))
                        : Sequences.simple(val);
      } else {
        finalSequence = baseResults;
      }

      if (query.isGrandTotal()) {
        // Accumulate grand totals while iterating the sequence.
        final Object[] grandTotals = new Object[query.getAggregatorSpecs().size()];
        final Sequence<Result<TimeseriesResultValue>> mappedSequence = Sequences.map(
            finalSequence,
            resultValue -> {
              for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
                final AggregatorFactory aggregatorFactory = query.getAggregatorSpecs().get(i);
                final Object value = resultValue.getValue().getMetric(aggregatorFactory.getName());
                if (grandTotals[i] == null) {
                  grandTotals[i] = value;
                } else {
                  grandTotals[i] = aggregatorFactory.combine(grandTotals[i], value);
                }
              }
              return resultValue;
            }
        );

        return Sequences.concat(
            ImmutableList.of(
                mappedSequence,
                Sequences.simple(
                    () -> {
                      final Map<String, Object> totalsMap = new HashMap<>();

                      for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
                        totalsMap.put(query.getAggregatorSpecs().get(i).getName(), grandTotals[i]);
                      }

                      final Result<TimeseriesResultValue> result = new Result<>(
                          null,
                          new TimeseriesResultValue(totalsMap)
                      );

                      return Collections.singletonList(result).iterator();
                    }
                )
            )
        );
      } else {
        return finalSequence;
      }
    };
  }

  @Override
  public BinaryOperator<Result<TimeseriesResultValue>> createMergeFn(
      Query<Result<TimeseriesResultValue>> query
  )
  {
    TimeseriesQuery timeseriesQuery = (TimeseriesQuery) query;
    return new TimeseriesBinaryFn(timeseriesQuery.getGranularity(), timeseriesQuery.getAggregatorSpecs());
  }

  @Override
  public Comparator<Result<TimeseriesResultValue>> createResultComparator(Query<Result<TimeseriesResultValue>> query)
  {
    return ResultGranularTimestampComparator.create(query.getGranularity(), ((TimeseriesQuery) query).isDescending());
  }

  /**
   * Returns a {@link TimeseriesResultValue} that corresponds to an empty-set aggregation, which is used in situations
   * where we want to return a single result representing "nothing was aggregated".
   */
  Result<TimeseriesResultValue> getEmptyTimeseriesResultValue(TimeseriesQuery query)
  {
    final Object[] resultArray = getEmptyAggregations(query.getAggregatorSpecs());
    final DateTime start = query.getIntervals().isEmpty() ? DateTimes.EPOCH : query.getIntervals().get(0).getStart();
    TimeseriesResultBuilder bob = new TimeseriesResultBuilder(start);
    for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
      bob.addMetric(query.getAggregatorSpecs().get(i).getName(), resultArray[i]);
    }
    return bob.build();
  }

  @Override
  public TimeseriesQueryMetrics makeMetrics(TimeseriesQuery query)
  {
    TimeseriesQueryMetrics queryMetrics = queryMetricsFactory.makeMetrics();
    queryMetrics.query(query);
    return queryMetrics;
  }

  @Override
  public TypeReference<Result<TimeseriesResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TimeseriesResultValue>, Object, TimeseriesQuery> getCacheStrategy(final TimeseriesQuery query)
  {
    return getCacheStrategy(query, null);
  }


  @Override
  public CacheStrategy<Result<TimeseriesResultValue>, Object, TimeseriesQuery> getCacheStrategy(
      final TimeseriesQuery query,
      @Nullable final ObjectMapper objectMapper
  )
  {
    return new CacheStrategy<>()
    {
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();

      @Override
      public boolean isCacheable(TimeseriesQuery query, boolean willMergeRunners, boolean bySegment)
      {
        return true;
      }

      @Override
      public byte[] computeCacheKey(TimeseriesQuery query)
      {
        return new CacheKeyBuilder(TIMESERIES_QUERY)
            .appendBoolean(query.isDescending())
            .appendBoolean(query.isSkipEmptyBuckets())
            .appendCacheable(query.getGranularity())
            .appendCacheable(query.getDimensionsFilter())
            .appendCacheables(query.getAggregatorSpecs())
            .appendCacheable(query.getVirtualColumns())
            .appendInt(query.getLimit())
            .build();
      }

      @Override
      public byte[] computeResultLevelCacheKey(TimeseriesQuery query)
      {
        final CacheKeyBuilder builder = new CacheKeyBuilder(TIMESERIES_QUERY)
            .appendBoolean(query.isDescending())
            .appendBoolean(query.isSkipEmptyBuckets())
            .appendCacheable(query.getGranularity())
            .appendCacheable(query.getDimensionsFilter())
            .appendCacheables(query.getAggregatorSpecs())
            .appendCacheable(query.getVirtualColumns())
            .appendCacheables(query.getPostAggregatorSpecs())
            .appendInt(query.getLimit())
            .appendString(query.getTimestampResultField())
            .appendBoolean(query.isGrandTotal());
        return builder.build();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TimeseriesResultValue>, Object> prepareForCache(boolean isResultLevelCache)
      {
        return input -> {
          TimeseriesResultValue results = input.getValue();
          final List<Object> retVal = Lists.newArrayListWithCapacity(1 + aggs.size());

          // Timestamp can be null if grandTotal is true.
          if (isResultLevelCache) {
            retVal.add(input.getTimestamp() == null ? null : input.getTimestamp().getMillis());
          } else {
            retVal.add(Preconditions.checkNotNull(input.getTimestamp(), "timestamp of input[%s]", input).getMillis());
          }
          for (AggregatorFactory agg : aggs) {
            retVal.add(results.getMetric(agg.getName()));
          }
          if (isResultLevelCache) {
            for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
              retVal.add(results.getMetric(postAgg.getName()));
            }
          }
          return retVal;
        };
      }

      @Override
      public Function<Object, Result<TimeseriesResultValue>> pullFromCache(boolean isResultLevelCache)
      {
        return new Function<>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          public Result<TimeseriesResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
            final Map<String, Object> retVal = Maps.newLinkedHashMap();

            Iterator<Object> resultIter = results.iterator();

            final Number timestampNumber = (Number) resultIter.next();
            final DateTime timestamp;
            if (isResultLevelCache) {
              timestamp = timestampNumber == null ? null : granularity.toDateTime(timestampNumber.longValue());
            } else {
              timestamp = granularity.toDateTime(Preconditions.checkNotNull(timestampNumber, "timestamp").longValue());
            }

            // If "timestampResultField" is set, we must include a copy of the timestamp in the result.
            // This is used by the SQL layer when it generates a Timeseries query for a group-by-time-floor SQL query.
            // The SQL layer expects the result of the time-floor to have a specific name that is not going to be "__time".
            if (StringUtils.isNotEmpty(query.getTimestampResultField()) && timestamp != null) {
              retVal.put(query.getTimestampResultField(), timestamp.getMillis());
            }

            CacheStrategy.fetchAggregatorsFromCache(
                aggs,
                resultIter,
                isResultLevelCache,
                (aggName, aggPosition, aggValueObject) -> {
                  retVal.put(aggName, aggValueObject);
                }
            );

            if (isResultLevelCache) {
              Iterator<PostAggregator> postItr = query.getPostAggregatorSpecs().iterator();
              while (postItr.hasNext() && resultIter.hasNext()) {
                retVal.put(postItr.next().getName(), resultIter.next());
              }
            }

            return new Result<>(
                timestamp,
                new TimeseriesResultValue(retVal)
            );
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> preMergeQueryDecoration(final QueryRunner<Result<TimeseriesResultValue>> runner)
  {
    return (queryPlus, responseContext) -> {
      return runner.run(queryPlus, responseContext);
    };
  }

  @Override
  public Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makePreComputeManipulatorFn(
      final TimeseriesQuery query,
      final MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn, false);
  }

  @Override
  public Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makePostComputeManipulatorFn(
      TimeseriesQuery query,
      MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn, true);
  }

  @Override
  public RowSignature resultArraySignature(TimeseriesQuery query)
  {
    return query.getResultRowSignature(RowSignature.Finalization.UNKNOWN);
  }

  @Override
  public Sequence<Object[]> resultsAsArrays(
      final TimeseriesQuery query,
      final Sequence<Result<TimeseriesResultValue>> resultSequence
  )
  {
    final List<String> fields = resultArraySignature(query).getColumnNames();
    return Sequences.map(
        resultSequence,
        result -> {
          final Object[] retVal = new Object[fields.size()];

          // Position 0 is always __time.
          retVal[0] = result.getTimestamp().getMillis();

          // Add other fields.
          final Map<String, Object> resultMap = result.getValue().getBaseObject();
          for (int i = 1; i < fields.size(); i++) {
            retVal[i] = resultMap.get(fields.get(i));
          }

          return retVal;
        }
    );
  }

  /**
   * This returns a single frame containing the results of the timeseries query
   */
  @Override
  public Optional<Sequence<FrameSignaturePair>> resultsAsFrames(
      TimeseriesQuery query,
      Sequence<Result<TimeseriesResultValue>> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes
  )
  {
    final RowSignature rowSignature =
        query.getResultRowSignature(query.context().isFinalize(true) ? RowSignature.Finalization.YES : RowSignature.Finalization.NO);
    final Pair<Cursor, Closeable> cursorAndCloseable = IterableRowsCursorHelper.getCursorFromSequence(
        resultsAsArrays(query, resultSequence),
        rowSignature
    );
    final Cursor cursor = cursorAndCloseable.lhs;
    final Closeable closeable = cursorAndCloseable.rhs;

    RowSignature modifiedRowSignature = useNestedForUnknownTypes
                                        ? FrameWriterUtils.replaceUnknownTypesWithNestedColumns(rowSignature)
                                        : rowSignature;
    FrameCursorUtils.throwIfColumnsHaveUnknownType(modifiedRowSignature);

    FrameWriterFactory frameWriterFactory = FrameWriters.makeColumnBasedFrameWriterFactory(
        memoryAllocatorFactory,
        modifiedRowSignature,
        new ArrayList<>()
    );

    Sequence<Frame> frames = FrameCursorUtils.cursorToFramesSequence(cursor, frameWriterFactory).withBaggage(closeable);

    // All frames are generated with the same signature therefore we can attach the row signature
    return Optional.of(frames.map(frame -> new FrameSignaturePair(frame, modifiedRowSignature)));
  }

  private Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makeComputeManipulatorFn(
      final TimeseriesQuery query,
      final MetricManipulationFn fn,
      final boolean calculatePostAggs
  )
  {
    return result -> {
      final TimeseriesResultValue holder = result.getValue();
      final Map<String, Object> values = new HashMap<>(holder.getBaseObject());
      if (calculatePostAggs) {
        // If "timestampResultField" is set, we must include a copy of the timestamp in the result.
        // This is used by the SQL layer when it generates a Timeseries query for a group-by-time-floor SQL query.
        // The SQL layer expects the result of the time-floor to have a specific name that is not going to be "__time".
        // This should be done before computing post aggregators since they can reference "timestampResultField".
        if (StringUtils.isNotEmpty(query.getTimestampResultField()) && result.getTimestamp() != null) {
          final DateTime timestamp = result.getTimestamp();
          values.put(query.getTimestampResultField(), timestamp.getMillis());
        }
        if (!query.getPostAggregatorSpecs().isEmpty()) {
          // put non finalized aggregators for calculating dependent post Aggregators
          for (AggregatorFactory agg : query.getAggregatorSpecs()) {
            values.put(agg.getName(), holder.getMetric(agg.getName()));
          }
          for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
            values.put(postAgg.getName(), postAgg.compute(values));
          }
        }
      }
      for (AggregatorFactory agg : query.getAggregatorSpecs()) {
        values.put(agg.getName(), fn.manipulate(agg, holder.getMetric(agg.getName())));
      }

      return new Result<>(
          result.getTimestamp(),
          new TimeseriesResultValue(values)
      );
    };
  }

  /**
   * Returns a set of values that corresponds to an empty-set aggregation, which is used in situations
   * where we want to return a single result representing "nothing was aggregated". The returned array has
   * one element per {@link AggregatorFactory}.
   */
  public static Object[] getEmptyAggregations(List<AggregatorFactory> aggregatorSpecs)
  {
    final Aggregator[] aggregators = new Aggregator[aggregatorSpecs.size()];
    final RowSignature aggregatorsSignature =
        RowSignature.builder().addAggregators(aggregatorSpecs, RowSignature.Finalization.UNKNOWN).build();
    for (int i = 0; i < aggregatorSpecs.size(); i++) {
      aggregators[i] =
          aggregatorSpecs.get(i)
                         .factorize(
                             RowBasedColumnSelectorFactory.create(
                                 RowAdapters.standardRow(),
                                 () -> new MapBasedRow(null, null),
                                 aggregatorsSignature,
                                 false,
                                 false
                             )
                         );
    }

    final Object[] retVal = new Object[aggregatorSpecs.size()];
    for (int i = 0; i < aggregatorSpecs.size(); i++) {
      retVal[i] = aggregators[i].get();
      aggregators[i].close();
    }
    return retVal;
  }
}
