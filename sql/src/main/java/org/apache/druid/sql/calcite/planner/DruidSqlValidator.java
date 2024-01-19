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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.BaseDruidSqlValidator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.SqlToRelConverter.SqlIdentifierFinder;
import org.apache.calcite.util.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.checkerframework.checker.nullness.qual.PolyNull;

/**
 * Druid extended SQL validator. (At present, it doesn't actually
 * have any extensions yet, but it will soon.)
 */
class DruidSqlValidator extends BaseDruidSqlValidator
{
  private final PlannerContext plannerContext;

  protected DruidSqlValidator(
      SqlOperatorTable opTab,
      CalciteCatalogReader catalogReader,
      JavaTypeFactory typeFactory,
      Config validatorConfig,
      PlannerContext plannerContext
  )
  {
    super(opTab, catalogReader, typeFactory, validatorConfig);
    this.plannerContext = plannerContext;
  }

  @Override
  public void validateCall(SqlCall call, SqlValidatorScope scope)
  {
    if (call.getKind() == SqlKind.OVER) {
      if (!plannerContext.featureAvailable(EngineFeature.WINDOW_FUNCTIONS)) {
        throw buildCalciteContextException(
            StringUtils.format(
                "The query contains window functions; To run these window functions, specify [%s] in query context.",
                PlannerContext.CTX_ENABLE_WINDOW_FNS),
            call);
      }
    }
    if (call.getKind() == SqlKind.NULLS_FIRST) {
      SqlNode op0 = call.getOperandList().get(0);
      if (op0.getKind() == SqlKind.DESCENDING) {
        throw buildCalciteContextException("DESCENDING ordering with NULLS FIRST is not supported!", call);
      }
    }
    if (call.getKind() == SqlKind.NULLS_LAST) {
      SqlNode op0 = call.getOperandList().get(0);
      if (op0.getKind() != SqlKind.DESCENDING) {
        throw buildCalciteContextException("ASCENDING ordering with NULLS LAST is not supported!", call);
      }
    }
    super.validateCall(call, scope);
  }


  private CalciteContextException buildCalciteContextException(String message, SqlCall call)
  {
    SqlParserPos pos = call.getParserPosition();
    return new CalciteContextException(message,
        new CalciteException(message, null),
        pos.getLineNum(),
        pos.getColumnNum(),
        pos.getEndLineNum(),
        pos.getEndColumnNum());
  }

  @Override
  protected @PolyNull SqlNode performUnconditionalRewrites(@PolyNull SqlNode node, boolean underFrom)
  {
    if (
         node != null
        && node.getKind() == SqlKind.IN) {
      SqlCall call = (SqlCall) node;
      if(call.getOperandList().get(1) instanceof SqlNodeList) {
        SqlParserPos pos = call.getParserPosition();
        SqlNode left = call.getOperandList().get(0);
        SqlNodeList right = (SqlNodeList) call.getOperandList().get(1);
        Pair<SqlNodeList, SqlNodeList> exprAndLiterals = decomposeInArgs(right);
        SqlNodeList exprs = exprAndLiterals.left;
        SqlNodeList literals = exprAndLiterals.right;

        if(!literals.isEmpty()) {
          SqlOperator op;
          if(true) {
          op = SqlInternalOperators.DRUID_IN;
          }else {
          op = SqlStdOperatorTable.IN;
          }
          SqlCall inLiteralsCall = op.createCall(pos, left, literals);
//          SqlCall inLiteralsCall = SqlInternalOperators.DRUID_IN.createCall(pos, left, literals);

          if(!exprs.isEmpty()) {

            node = SqlStdOperatorTable.OR.createCall(
                pos,
                SqlStdOperatorTable.IN.createCall(pos, left, exprs),
                inLiteralsCall
            );
          } else {
            node = inLiteralsCall;
          }
        }
      }
    }
    return super.performUnconditionalRewrites(node, underFrom);
  }

  /**
   * Decomposes the IN arguments into lists of expressions and literals.
   *
   * @param sqlNodeList
   * @return a Pair ; for which the left hand side is the expressions and null literals ; the right are the literals
   */
  private Pair<SqlNodeList, SqlNodeList> decomposeInArgs(SqlNodeList sqlNodeList)
  {
    SqlIdentifierFinder identifierFinder = new SqlIdentifierFinder();
    SqlNodeList exprs = new SqlNodeList(sqlNodeList.getParserPosition());
    SqlNodeList literals = new SqlNodeList(sqlNodeList.getParserPosition());
    for (SqlNode sqlNode : sqlNodeList) {
      if (SqlUtil.isNull(sqlNode) || sqlNode.accept(identifierFinder)) {
        exprs.add(sqlNode);
      } else {
        literals.add(sqlNode);
      }
    }
    return new Pair<>(exprs, literals);
  }
}
