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

package org.apache.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdMapping;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;

/**
 *
 */
public class RegexFilteredDimensionSpec extends BaseFilteredDimensionSpec
{

  private static final byte CACHE_TYPE_ID = 0x2;

  private final String pattern;

  private final Pattern compiledRegex;

  public RegexFilteredDimensionSpec(
      @JsonProperty("delegate") DimensionSpec delegate,
      @JsonProperty("pattern") String pattern //rows not matching the pattern will be discarded
  )
  {
    super(delegate);
    this.pattern = Preconditions.checkNotNull(pattern, "pattern must not be null");
    this.compiledRegex = Pattern.compile(pattern);
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
  }

  @Override
  public DimensionSelector decorate(final DimensionSelector selector)
  {
    if (selector == null) {
      return null;
    }

    final int selectorCardinality = selector.getValueCardinality();
    if (selectorCardinality < 0 || !selector.nameLookupPossibleInAdvance()) {
      return new PredicateFilteredDimensionSelector(
          selector,
          input -> input != null && compiledRegex.matcher(input).matches()
      );
    }

    final IdMapping.Builder builder = IdMapping.Builder.ofUnknownCardinality();
    for (int i = 0; i < selectorCardinality; i++) {
      String val = selector.lookupName(i);
      if (val != null && compiledRegex.matcher(val).matches()) {
        builder.addForwardMapping(i);
      }
    }

    return new ForwardingFilteredDimensionSelector(selector, builder.build());
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] delegateCacheKey = delegate.getCacheKey();
    byte[] regexBytes = StringUtils.toUtf8(pattern);
    return ByteBuffer.allocate(2 + delegateCacheKey.length + regexBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(delegateCacheKey)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(regexBytes)
                     .array();
  }

  @Override
  public DimensionSpec withDimension(String newDimension)
  {
    return new RegexFilteredDimensionSpec(delegate.withDimension(newDimension), pattern);
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

    RegexFilteredDimensionSpec that = (RegexFilteredDimensionSpec) o;

    if (!delegate.equals(that.delegate)) {
      return false;
    }
    return pattern.equals(that.pattern);
  }

  @Override
  public int hashCode()
  {
    int result = delegate.hashCode();
    result = 31 * result + pattern.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "RegexFilteredDimensionSpec{" +
           "pattern='" + pattern + '\'' +
           '}';
  }
}
