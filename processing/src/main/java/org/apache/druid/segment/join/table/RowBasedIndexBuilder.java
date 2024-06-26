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

package org.apache.druid.segment.join.table;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for creating {@link IndexedTable.Index} instances.
 *
 * Its main role is to decide which kind of implementation to use.
 */
public class RowBasedIndexBuilder
{
  // Long2ObjectOpenHashMap<IntList> is (very) roughly 15x bigger than int[] per entry.
  private static final long INT_ARRAY_SPACE_SAVINGS_FACTOR = 15;

  // A number that is small enough that we shouldn't worry about making a full array for it. (Yields a 1MB array.)
  private static final long INT_ARRAY_SMALL_SIZE_OK = 250_000;

  private int currentRow = 0;
  private int nonNullKeys = 0;
  private final ColumnType keyType;
  private final Map<Object, IntSortedSet> index;
  private IntSortedSet nullIndex;

  private long minLongKey = Long.MAX_VALUE;
  private long maxLongKey = Long.MIN_VALUE;

  public RowBasedIndexBuilder(ColumnType keyType)
  {
    this.keyType = keyType;

    // Cannot build index on complex types, and non-primitive arrays
    if (keyType.is(ValueType.COMPLEX) || keyType.isArray() && !keyType.isPrimitiveArray()) {
      throw InvalidInput.exception("Cannot join when the join condition has column of type [%s]", keyType);
    }

    if (keyType.is(ValueType.LONG)) {
      // We're specializing the type even though we don't specialize usage in this class, for two reasons:
      //  (1) It's still useful to reduce overall memory footprint.
      //  (2) MapIndex specifically checks for Long2ObjectMap instances and *does* specialize usage.
      final Long2ObjectOpenHashMap<IntList> theMap = new Long2ObjectOpenHashMap<>();
      index = (Map) theMap;
    } else {
      index = new HashMap<>();
    }
  }

  /**
   * Add a key to the index. This must be called exactly once per row, even for null values or values that are the
   * wrong type, because the builder keeps an internal row-number counter. The builder will handle both nulls and
   * mismatched types, so callers do not need to worry about this.
   */
  public RowBasedIndexBuilder add(@Nullable final Object key)
  {
    if (key == null) {
      // Use "nullIndex" instead of "index" because "index" may be specialized as Long2ObjectMap, which cannot
      // accept null keys.
      if (nullIndex == null) {
        nullIndex = new IntAVLTreeSet();
      }

      nullIndex.add(currentRow);
    } else {
      final Object castKey = DimensionHandlerUtils.convertObjectToType(key, keyType);

      if (castKey != null) {
        index.computeIfAbsent(castKey, k -> new IntAVLTreeSet()).add(currentRow);
        nonNullKeys++;

        // Track min, max long value so we can decide later on if it's appropriate to use an array-backed implementation.
        if (keyType.is(ValueType.LONG) && (long) castKey < minLongKey) {
          minLongKey = (long) castKey;
        }

        if (keyType.is(ValueType.LONG) && (long) castKey > maxLongKey) {
          maxLongKey = (long) castKey;
        }
      }
    }

    currentRow++;

    return this;
  }

  /**
   * Create the index. After calling this, the state of the builder is undefined, and you should discard it.
   */
  public IndexedTable.Index build()
  {
    final boolean nonNullKeysUnique = index.size() == nonNullKeys;

    if (keyType.is(ValueType.LONG) && nonNullKeysUnique && !index.isEmpty() && nullIndex == null) {
      // May be a good candidate for UniqueLongArrayIndex. Check the range of values as compared to min and max.
      long range;

      try {
        // Add 1 so "range" would be equal to the size of the necessary array.
        range = Math.addExact(Math.subtractExact(maxLongKey, minLongKey), 1);
      }
      catch (ArithmeticException e) {
        // Overflow; way too big.
        range = 0;
      }

      // Use a UniqueLongArrayIndex if the range of values is small enough.
      final long rangeThreshold = Math.max(
          INT_ARRAY_SMALL_SIZE_OK,
          Math.min(Integer.MAX_VALUE, INT_ARRAY_SPACE_SAVINGS_FACTOR * index.size())
      );

      if (range > 0 && range < rangeThreshold) {
        final int[] indexAsArray = new int[Ints.checkedCast(range)];
        Arrays.fill(indexAsArray, IndexedTable.Index.NOT_FOUND);

        // Safe to cast to Long2ObjectMap because the constructor always uses one for long-typed keys.
        final ObjectIterator<Long2ObjectMap.Entry<IntSortedSet>> entries =
            ((Long2ObjectMap<IntSortedSet>) ((Map) index)).long2ObjectEntrySet().iterator();

        while (entries.hasNext()) {
          final Long2ObjectMap.Entry<IntSortedSet> entry = entries.next();
          final IntSortedSet rowNums = entry.getValue();

          if (rowNums.size() != 1) {
            throw new ISE("Expected single element");
          }

          indexAsArray[Ints.checkedCast(entry.getLongKey() - minLongKey)] = rowNums.firstInt();
          entries.remove();
        }

        assert index.isEmpty();

        // Early return of specialized implementation.
        return new UniqueLongArrayIndex(indexAsArray, minLongKey);
      }
    }

    return new MapIndex(keyType, index, nullIndex, nonNullKeysUnique);
  }
}
