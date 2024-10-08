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

package org.apache.druid.java.util.common.parsers;

import com.google.common.annotations.VisibleForTesting;
import com.opencsv.RFC4180Parser;
import org.apache.druid.data.input.impl.CsvInputFormat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CSVParser extends AbstractFlatTextFormatParser
{
  private final RFC4180Parser parser = CsvInputFormat.createOpenCsvParser();

  public CSVParser(
      @Nullable final String listDelimiter,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows,
      final boolean tryParseNumbers
  )
  {
    super(listDelimiter, hasHeaderRow, maxSkipHeaderRows, tryParseNumbers);
  }

  public CSVParser(
      @Nullable final String listDelimiter,
      final Iterable<String> fieldNames,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows,
      final boolean tryParseNumbers
  )
  {
    this(listDelimiter, hasHeaderRow, maxSkipHeaderRows, tryParseNumbers);

    setFieldNames(fieldNames);
  }

  @VisibleForTesting
  CSVParser(@Nullable final String listDelimiter, final String header)
  {
    this(listDelimiter, false, 0, false);

    setFieldNames(header);
  }

  @Override
  protected List<String> parseLine(String input) throws IOException
  {
    return Arrays.asList(parser.parseLine(input));
  }
}
