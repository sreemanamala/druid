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

package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class LikeOperatorConversion2 extends DirectOperatorConversion
{
  private static final SqlOperator SQL_FUNCTION = SqlInternalOperators.DRUID_IN;

  public LikeOperatorConversion2()
  {
    super(SQL_FUNCTION, "druid_in");
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      RexNode rexNode)
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final DruidExpression druidExpression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        operands.get(0)
    );
    if (druidExpression == null) {
      return null;
    }
    final String columnName = getRefColumn(
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        operands.get(0)
    );
    if (columnName == null) {
      return null;
    }
    SortedSet<String> values = extractStringValues(operands.subList(1, operands.size()));
    if (values == null) {
      return null;
    }
    return new InDimFilter(columnName, values);

    // if (druidExpression.isSimpleExtraction()) {
    // return new LikeDimFilter(
    // druidExpression.getSimpleExtraction().getColumn(),
    // RexLiteral.stringValue(operands.get(1)),
    // operands.size() > 2 ? RexLiteral.stringValue(operands.get(2)) : null,
    // druidExpression.getSimpleExtraction().getExtractionFn()
    // );
    // } else if (virtualColumnRegistry != null) {
    // String v = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
    // druidExpression,
    // operands.get(0).getType()
    // );
    //
    // return new LikeDimFilter(
    // v,
    // RexLiteral.stringValue(operands.get(1)),
    // operands.size() > 2 ? RexLiteral.stringValue(operands.get(2)) : null,
    // null
    // );
    // } else {
    // return null;
    // }
  }

  /**
   * Flattens the given set of literals into {@link String}s.
   *
   * @return null if unsuccessfull
   */
  private SortedSet<String> extractStringValues(List<RexNode> literals)
  {
    SortedSet<String> values = new TreeSet<>(Comparator.naturalOrder());
    for (RexNode literal : literals) {

      if (!literal.isA(SqlKind.LITERAL)) {
        return null;
      }
      //FIXME: this might not be good enough
      switch(literal.getType().getSqlTypeName() ) {

        case BIGINT:
        case INTEGER:
        values.add(((Number) RexLiteral.value(literal)).toString());
        break;
        default:
        values.add(RexLiteral.stringValue(literal));
        break;

      }
    }
    return values;
  }

  private static String getRefColumn(PlannerContext plannerContext, RowSignature rowSignature,
      VirtualColumnRegistry virtualColumnRegistry, RexNode expression)
  {

    final DruidExpression druidExpression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        expression
    );
    if (druidExpression == null) {
      return null;
    }

    if (druidExpression.isDirectColumnAccess()) {
      return druidExpression.getDirectColumn();
    } else {
      String v = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          druidExpression,
          expression.getType()
      );
      return v;
    }
  }
}
