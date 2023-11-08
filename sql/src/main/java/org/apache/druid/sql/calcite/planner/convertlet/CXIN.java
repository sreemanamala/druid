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

package org.apache.druid.sql.calcite.planner.convertlet;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.util.Static;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CXIN implements DruidConvertletFactory
{
  public static final CXIN INSTANCE = new CXIN();

  private static final String NAME = "TIME_IN_INTERVAL";

  private CXIN()
  {
    // Singleton.
  }

  @Override
  public SqlRexConvertlet createConvertlet(PlannerContext plannerContext)
  {
    return new TimeInIntervalConvertlet(plannerContext.getTimeZone());
  }

  @Override
  public List<SqlOperator> operators()
  {
    return Collections.singletonList(SqlInternalOperators.DRUID_IN);
  }

  private static Interval intervalFromStringArgument(
      final SqlParserPos parserPos,
      final String intervalString,
      final DateTimeZone sessionTimeZone
  )
  {
    try {
      return new Interval(intervalString, ISOChronology.getInstance(sessionTimeZone));
    }
    catch (IllegalArgumentException e) {
      final RuntimeException ex =
          new IAE("Function '%s' second argument is not a valid ISO8601 interval: %s", NAME, e.getMessage());

      throw Static.RESOURCE.validatorContext(
          parserPos.getLineNum(),
          parserPos.getColumnNum(),
          parserPos.getEndLineNum(),
          parserPos.getEndColumnNum()
      ).ex(ex);
    }
  }

  private static class TimeInIntervalConvertlet implements SqlRexConvertlet
  {
    private final DateTimeZone sessionTimeZone;

    private TimeInIntervalConvertlet(final DateTimeZone sessionTimeZone)
    {
      this.sessionTimeZone = sessionTimeZone;
    }

    @Override
    public RexNode convertCall(final SqlRexContext cx, final SqlCall call)
    {
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final List<RexNode> newArgs = new ArrayList<RexNode>();

      SqlNode left = call.getOperandList().get(0);
      newArgs.add(cx.convertExpression(left));

      SqlNodeList values = (SqlNodeList) call.getOperandList().get(1);
      for (SqlNode sqlNode : values) {
        newArgs.add(cx.convertExpression(sqlNode));
      }

      return rexBuilder.makeCall(SqlInternalOperators.DRUID_IN, newArgs);
    }
  }
}
