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

package org.apache.druid.collections.spatial.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.spatial.ImmutableFloatPoint;
import org.apache.druid.collections.spatial.ImmutableNode;
import org.apache.druid.segment.incremental.SpatialDimensionRowTransformer;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 *
 */
public class RectangularBound implements Bound<float[], ImmutableFloatPoint>
{
  private static final byte CACHE_TYPE_ID = 0x0;

  private final float[] minCoords;
  private final float[] maxCoords;
  private final int limit;
  private final int numDims;

  @JsonCreator
  public RectangularBound(
      @JsonProperty("minCoords") float[] minCoords,
      @JsonProperty("maxCoords") float[] maxCoords,
      @JsonProperty("limit") int limit
  )
  {
    Preconditions.checkArgument(minCoords.length == maxCoords.length);

    this.numDims = minCoords.length;

    this.minCoords = minCoords;
    this.maxCoords = maxCoords;
    this.limit = limit;
  }

  public RectangularBound(
      float[] minCoords,
      float[] maxCoords
  )
  {
    this(minCoords, maxCoords, 0);
  }

  @JsonProperty
  public float[] getMinCoords()
  {
    return minCoords;
  }

  @JsonProperty
  public float[] getMaxCoords()
  {
    return maxCoords;
  }

  @Override
  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @Override
  public int getNumDims()
  {
    return numDims;
  }

  @Override
  public boolean overlaps(ImmutableNode<float[]> node)
  {
    final float[] nodeMinCoords = node.getMinCoordinates();
    final float[] nodeMaxCoords = node.getMaxCoordinates();

    for (int i = 0; i < numDims; i++) {
      if (nodeMaxCoords[i] < minCoords[i] || nodeMinCoords[i] > maxCoords[i]) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean contains(float[] coords)
  {
    for (int i = 0; i < numDims; i++) {
      if (coords[i] < minCoords[i] || coords[i] > maxCoords[i]) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean containsObj(@Nullable Object input)
  {
    if (input instanceof String) {
      final float[] coordinate = SpatialDimensionRowTransformer.decode((String) input);
      if (coordinate == null) {
        return false;
      }
      return contains(coordinate);
    }
    return false;
  }

  @Override
  public Iterable<ImmutableFloatPoint> filter(Iterable<ImmutableFloatPoint> points)
  {
    return Iterables.filter(
        points,
        new Predicate<>()
        {
          @Override
          public boolean apply(ImmutableFloatPoint immutablePoint)
          {
            return contains(immutablePoint.getCoords());
          }
        }
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    ByteBuffer minCoordsBuffer = ByteBuffer.allocate(minCoords.length * Float.BYTES);
    minCoordsBuffer.asFloatBuffer().put(minCoords);
    final byte[] minCoordsCacheKey = minCoordsBuffer.array();

    ByteBuffer maxCoordsBuffer = ByteBuffer.allocate(maxCoords.length * Float.BYTES);
    maxCoordsBuffer.asFloatBuffer().put(maxCoords);
    final byte[] maxCoordsCacheKey = maxCoordsBuffer.array();

    final ByteBuffer cacheKey = ByteBuffer
        .allocate(1 + minCoordsCacheKey.length + maxCoordsCacheKey.length + Integer.BYTES)
        .put(minCoordsCacheKey)
        .put(maxCoordsCacheKey)
        .putInt(limit)
        .put(CACHE_TYPE_ID);
    return cacheKey.array();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RectangularBound that = (RectangularBound) o;
    return limit == that.limit &&
           numDims == that.numDims &&
           Arrays.equals(minCoords, that.minCoords) &&
           Arrays.equals(maxCoords, that.maxCoords);
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(limit, numDims);
    result = 31 * result + Arrays.hashCode(minCoords);
    result = 31 * result + Arrays.hashCode(maxCoords);
    return result;
  }
}
