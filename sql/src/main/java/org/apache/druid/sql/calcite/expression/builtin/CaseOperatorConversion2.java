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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ComparableOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;

import static org.apache.calcite.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.List;

public class CaseOperatorConversion2 implements SqlOperatorConversion
{
  @Override
  public SqlOperator calciteOperator()
  {
    return new SqlInOperator(SqlKind.IN);
  }

  public static class SqlInOperator extends SqlBinaryOperator {
    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SqlInOperator.
     *
     * @param kind IN or NOT IN
     */
    SqlInOperator(SqlKind kind) {
      this(kind.sql, kind);
      assert kind == SqlKind.IN || kind == SqlKind.NOT_IN
          || kind == SqlKind.DRUID_IN || kind == SqlKind.DRUID_NOT_IN;
    }

    @Override
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call)
    {
      throw new RuntimeException("Unimplemented!");
    }
    protected SqlInOperator(String name, SqlKind kind) {
      super(name, kind,
          32,
          true,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.FIRST_KNOWN,
          null);
    }

    //~ Methods ----------------------------------------------------------------

    @Deprecated // to be removed before 2.0
    public boolean isNotIn() {
      return kind == SqlKind.NOT_IN;
    }

    @Override public SqlOperator not() {
      return of(kind.negateNullSafe());
    }

    private static SqlBinaryOperator of(SqlKind kind) {
      switch (kind) {
      case IN:
        return SqlStdOperatorTable.IN;
      case NOT_IN:
        return SqlStdOperatorTable.NOT_IN;
      case DRUID_IN:
        return SqlInternalOperators.DRUID_IN;
      case DRUID_NOT_IN:
        return SqlInternalOperators.DRUID_NOT_IN;
      default:
        throw new AssertionError("unexpected " + kind);
      }
    }

    @Override public boolean validRexOperands(int count, Litmus litmus) {
      if (count == 0) {
        return litmus.fail("wrong operand count {} for {}", count, this);
      }
      return litmus.succeed();
    }

    @Override public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
      final List<SqlNode> operands = call.getOperandList();
      assert operands.size() == 2;
      final SqlNode left = operands.get(0);
      final SqlNode right = operands.get(1);

      final RelDataTypeFactory typeFactory = validator.getTypeFactory();
      RelDataType leftType = validator.deriveType(scope, left);
      RelDataType rightType;

      // Derive type for RHS.
      if (right instanceof SqlNodeList) {
        // Handle the 'IN (expr, ...)' form.
        List<RelDataType> rightTypeList = new ArrayList<>();
        SqlNodeList nodeList = (SqlNodeList) right;
        for (int i = 0; i < nodeList.size(); i++) {
          SqlNode node = nodeList.get(i);
          RelDataType nodeType = validator.deriveType(scope, node);
          rightTypeList.add(nodeType);
        }
        rightType = typeFactory.leastRestrictive(rightTypeList);

        // First check that the expressions in the IN list are compatible
        // with each other. Same rules as the VALUES operator (per
        // SQL:2003 Part 2 Section 8.4, <in predicate>).
        if (null == rightType && validator.config().typeCoercionEnabled()) {
          // Do implicit type cast if it is allowed to.
          rightType = validator.getTypeCoercion().getWiderTypeFor(rightTypeList, true);
        }
        if (null == rightType) {
          throw validator.newValidationError(right,
              RESOURCE.incompatibleTypesInList());
        }

        // Record the RHS type for use by SqlToRelConverter.
        validator.setValidatedNodeType(nodeList, rightType);
      } else {
        // Handle the 'IN (query)' form.
        rightType = validator.deriveType(scope, right);
      }
      SqlCallBinding callBinding = new SqlCallBinding(validator, scope, call);
      // Coerce type first.
      if (callBinding.isTypeCoercionEnabled()) {
        boolean coerced = callBinding.getValidator().getTypeCoercion()
            .inOperationCoercion(callBinding);
        if (coerced) {
          // Update the node data type if we coerced any type.
          leftType = validator.deriveType(scope, call.operand(0));
          rightType = validator.deriveType(scope, call.operand(1));
        }
      }

      // Now check that the left expression is compatible with the
      // type of the list. Same strategy as the '=' operator.
      // Normalize the types on both sides to be row types
      // for the purposes of compatibility-checking.
      RelDataType leftRowType =
          SqlTypeUtil.promoteToRowType(
              typeFactory,
              leftType,
              null);
      RelDataType rightRowType =
          SqlTypeUtil.promoteToRowType(
              typeFactory,
              rightType,
              null);

      final ComparableOperandTypeChecker checker =
          (ComparableOperandTypeChecker)
              OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED;
      if (!checker.checkOperandTypes(
          new ExplicitOperatorBinding(
              callBinding,
              ImmutableList.of(leftRowType, rightRowType)), callBinding)) {
        throw validator.newValidationError(call,
            RESOURCE.incompatibleValueType(SqlStdOperatorTable.IN.getName()));
      }

      // Result is a boolean, nullable if there are any nullable types
      // on either side.
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.BOOLEAN),
          anyNullable(leftRowType.getFieldList())
          || anyNullable(rightRowType.getFieldList()));
    }

    private static boolean anyNullable(List<RelDataTypeField> fieldList) {
      for (RelDataTypeField field : fieldList) {
        if (field.getType().isNullable()) {
          return true;
        }
      }
      return false;
    }

    @Override public boolean argumentMustBeScalar(int ordinal) {
      // Argument #0 must be scalar, argument #1 can be a list (1, 2) or
      // a query (select deptno from emp). So, only coerce argument #0 into
      // a scalar sub-query. For example, in
      //  select * from emp
      //  where (select count(*) from dept) in (select deptno from dept)
      // we should coerce the LHS to a scalar.
      return ordinal == 0;
    }
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );

    // coalesce and nvl are rewritten during planning as case statements
    // rewrite simple case_searched(notnull(expr1), expr1, expr2) to nvl(expr1, expr2) since the latter is vectorized
    // at the native layer
    // this conversion won't help if the condition expression is only part of then expression, like if the input
    // expression to coalesce was an expression itself, but this is better than nothing
    if (druidExpressions != null && druidExpressions.size() == 3) {
      final DruidExpression condition = druidExpressions.get(0);
      final DruidExpression thenExpression = druidExpressions.get(1);
      final DruidExpression elseExpression = druidExpressions.get(2);
      final String thenNotNull = StringUtils.format("notnull(%s)", thenExpression.getExpression());
      if (condition.getExpression().equals(thenNotNull)) {
        return DruidExpression.ofFunctionCall(
            Calcites.getColumnTypeForRelDataType(
                rexNode.getType()),
            "nvl",
            ImmutableList.of(thenExpression, elseExpression)
        );
      }
    }

    return OperatorConversions.convertDirectCall(
        plannerContext,
        rowSignature,
        rexNode,
        "case_searched"
    );
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      RexNode rexNode
  )
  {
    final RexCall call = (RexCall) rexNode;
    final List<RexNode> operands = call.getOperands();
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );

    // rewrite case_searched(notnull(someColumn), then, else) into better native filters
    //    or(then, and(else, isNull(someColumn))
    if (druidExpressions != null && druidExpressions.size() == 3) {
      final DruidExpression condition = druidExpressions.get(0);
      final DruidExpression thenExpression = druidExpressions.get(1);
      final DruidExpression elseExpression = druidExpressions.get(2);
      if (condition.getExpression().startsWith("notnull") && condition.getArguments().get(0).isDirectColumnAccess()) {

        DimFilter thenFilter = null, elseFilter = null;
        final DimFilter isNull;
        if (plannerContext.isUseBoundsAndSelectors()) {
          isNull = new SelectorDimFilter(condition.getArguments().get(0).getDirectColumn(), null, null);
        } else {
          isNull = NullFilter.forColumn(condition.getArguments().get(0).getDirectColumn());
        }

        if (call.getOperands().get(1) instanceof RexCall) {
          final RexCall thenCall = (RexCall) call.getOperands().get(1);
          thenFilter = Expressions.toFilter(plannerContext, rowSignature, virtualColumnRegistry, thenCall);
        }

        if (thenFilter != null && call.getOperands().get(2) instanceof RexLiteral) {
          if (call.getOperands().get(2).isAlwaysTrue()) {
            return new OrDimFilter(thenFilter, isNull);
          } else {
            // else is always false, we can leave it out
            return thenFilter;
          }
        } else if (call.getOperands().get(2) instanceof RexCall) {
          RexCall elseCall = (RexCall) call.getOperands().get(2);
          elseFilter = Expressions.toFilter(plannerContext, rowSignature, virtualColumnRegistry, elseCall);
        }

        // if either then or else filters produced a native filter (that wasn't just another expression filter)
        // make sure we have filters for both sides by filling in the gaps with expression filter
        if (thenFilter != null && !(thenFilter instanceof ExpressionDimFilter) && elseFilter == null) {
          elseFilter = new ExpressionDimFilter(
              elseExpression.getExpression(),
              plannerContext.parseExpression(elseExpression.getExpression()),
              null
          );
        } else if (thenFilter == null && elseFilter != null && !(elseFilter instanceof ExpressionDimFilter)) {
          thenFilter = new ExpressionDimFilter(
              thenExpression.getExpression(),
              plannerContext.parseExpression(thenExpression.getExpression()),
              null
          );
        }

        if (thenFilter != null && elseFilter != null) {
          return new OrDimFilter(thenFilter, new AndDimFilter(elseFilter, isNull));
        }
      }
    }

    // no special cases (..ha ha!) so fall through to defaul thandling
    return SqlOperatorConversion.super.toDruidFilter(plannerContext, rowSignature, virtualColumnRegistry, rexNode);
  }
}
