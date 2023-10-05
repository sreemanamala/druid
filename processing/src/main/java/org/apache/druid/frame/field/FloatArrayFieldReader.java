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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

/**
 * Reader for fields written by {@link NumericArrayFieldWriter#getFloatArrayFieldWriter}
 */
public class FloatArrayFieldReader extends NumericArrayFieldReader
{
  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer
  )
  {
    return new NumericArrayFieldSelector<Float>(memory, fieldPointer)
    {
      final SettableFieldPointer fieldPointer = new SettableFieldPointer();
      final ColumnValueSelector<?> columnValueSelector =
          FloatFieldReader.forArray().makeColumnValueSelector(memory, fieldPointer);

      @Nullable
      @Override
      public Float getIndividualValueAtMemory(long position)
      {
        fieldPointer.setPosition(position);
        if (columnValueSelector.isNull()) {
          return null;
        }
        return columnValueSelector.getFloat();
      }

      @Override
      public int getIndividualFieldSize()
      {
        return Byte.BYTES + Float.BYTES;
      }
    };
  }
}