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

package org.apache.druid.query.operator.window.value;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.operator.window.ComposingProcessor;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class WindowFirstProcessorTest
{
  @Test
  public void testFirstProcessing()
  {
    Map<String, Column> map = new LinkedHashMap<>();
    map.put("intCol", new IntArrayColumn(new int[]{88, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("doubleCol", new DoubleArrayColumn(new double[]{0.4728, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    map.put("objectCol", new ObjectArrayColumn(
        new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
        ColumnType.STRING
    ));
    map.put("nullFirstCol", new ObjectArrayColumn(
        new String[]{null, "b", "c", "d", "e", "f", "g", "h", "i", "j"},
        ColumnType.STRING
    ));

    MapOfColumnsRowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(map);

    ComposingProcessor processor = new ComposingProcessor(
        new WindowFirstProcessor("intCol", "FirstIntCol"),
        new WindowFirstProcessor("doubleCol", "FirstDoubleCol"),
        new WindowFirstProcessor("objectCol", "FirstObjectCol"),
        new WindowFirstProcessor("nullFirstCol", "NullFirstCol")
    );

    Assert.assertEquals(
        ImmutableList.of("FirstIntCol", "FirstDoubleCol", "FirstObjectCol", "NullFirstCol"),
        processor.getOutputColumnNames()
    );

    final RowsAndColumnsHelper expectations = new RowsAndColumnsHelper()
        .expectColumn("intCol", new int[]{88, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("doubleCol", new double[]{0.4728, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectColumn("FirstIntCol", new int[]{88, 88, 88, 88, 88, 88, 88, 88, 88, 88})
        .expectColumn(
            "FirstDoubleCol",
            new double[]{0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728, 0.4728}
        );

    expectations.columnHelper("FirstObjectCol", 10, ColumnType.STRING)
                .setExpectation(new String[]{"a", "a", "a", "a", "a", "a", "a", "a", "a", "a"});

    expectations.columnHelper("NullFirstCol", 10, ColumnType.STRING)
                .setNulls(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    final RowsAndColumns results = processor.process(rac);
    expectations.validate(results);
  }
}
