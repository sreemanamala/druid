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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class HttpInputSourceConfigTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(HttpInputSourceConfig.class).usingGetClass().verify();
  }

  @Test
  public void testNullAllowedProtocolsUseDefault()
  {
    HttpInputSourceConfig config = new HttpInputSourceConfig(null, null);
    Assert.assertEquals(HttpInputSourceConfig.DEFAULT_ALLOWED_PROTOCOLS, config.getAllowedProtocols());
    Assert.assertEquals(Collections.emptySet(), config.getAllowedHeaders());
  }

  @Test
  public void testEmptyAllowedProtocolsUseDefault()
  {
    HttpInputSourceConfig config = new HttpInputSourceConfig(ImmutableSet.of(), null);
    Assert.assertEquals(HttpInputSourceConfig.DEFAULT_ALLOWED_PROTOCOLS, config.getAllowedProtocols());
  }

  @Test
  public void testCustomAllowedProtocols()
  {
    HttpInputSourceConfig config = new HttpInputSourceConfig(ImmutableSet.of("druid"), null);
    Assert.assertEquals(ImmutableSet.of("druid"), config.getAllowedProtocols());
  }

  @Test
  public void testAllowedHeaders()
  {
    HttpInputSourceConfig config = new HttpInputSourceConfig(
        ImmutableSet.of("druid"),
        ImmutableSet.of("Content-Type", "Referer")
    );
    Assert.assertEquals(ImmutableSet.of("druid"), config.getAllowedProtocols());
    Assert.assertEquals(ImmutableSet.of("content-type", "referer"), config.getAllowedHeaders());
  }
}
