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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RecognizeInRule extends RelOptRule implements SubstitutionRule // FIXME
{
  public RecognizeInRule(PlannerContext plannerContext)
  {
    // FIXME
    super(operand(RelNode.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final RelNode oldNode = call.rel(0);
    final RecognizeInShuttle shuttle = new RecognizeInShuttle(oldNode.getCluster().getRexBuilder());
    final RelNode newNode = oldNode.accept(shuttle);

    // noinspection ObjectEquality
    if (newNode != oldNode) {
      call.transformTo(newNode);
      call.getPlanner().prune(oldNode);
    }
  }

  private static class RecognizeInShuttle extends RexShuttle
  {
    private final RexBuilder rexBuilder;

    public RecognizeInShuttle(RexBuilder rexBuilder)
    {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call)
    {
      if (call.isA(SqlKind.OR)) {

        RexNode newCall = rewriteOr(call);
        if (newCall != null) {
          return newCall;
        }
      }

      if (false) {
        if (call.getKind() == SqlKind.COALESCE
            && call.getOperands().size() == 2
            && call.getOperands().get(0).isA(SqlKind.OTHER_FUNCTION)) {
          final RexCall lookupCall = (RexCall) call.getOperands().get(0);
          if (lookupCall.getOperator().equals(QueryLookupOperatorConversion.SQL_FUNCTION)
              && lookupCall.getOperands().size() == 2
              && RexUtil.isLiteral(call.getOperands().get(1), true)) {
            return rexBuilder.makeCast(
                call.getType(),
                rexBuilder.makeCall(
                    QueryLookupOperatorConversion.SQL_FUNCTION,
                    lookupCall.getOperands().get(0),
                    lookupCall.getOperands().get(1),
                    call.getOperands().get(1)
                ),
                true,
                false
            );
          }
          return call;
        }
      }

      return super.visitCall(call);
    }

    private RexNode rewriteOr(RexCall call)
    {
      List<RexNode> operands = RexUtil.flattenOr(call.getOperands());

      Map<RexNode, InGroup> groups = new HashMap<>();
      for (RexNode rexNode : operands) {
        Equality eq = Equality.of(rexNode);
        if (eq != null) {
          InGroup group = groups.get(eq.ref);
          if (group == null) {
            group = new InGroup();
          }
          group.add(eq);
        }
      }

      boolean changed = false;
      for (InGroup group : groups.values()) {
        // FIXME group.parts.size() >?
        if (group.size() > 1) {
          operands= group.contract(operands);
          changed = true;
        }
      }

      if(changed) {
        return RexUtil.composeDisjunction(rexBuilder, operands);
      }
      return call;

    }

    class InGroup
    {

      private Set<Equality> parts = new HashSet<Equality>();

      public void add(Equality eq)
      {
        parts.add(eq);
      }

      public RexNode buildIn()
      {
        List<RexNode> ops = new ArrayList<>();
        ops.add(getRef());
        ops.addAll(getLiterals());
        return rexBuilder.makeCall(SqlStdOperatorTable.IN, ops);
      }

      private Collection<RexLiteral> getLiterals()
      {
        Set<RexLiteral> literals = new HashSet<>();
        for (Equality eq : parts) {
          literals.add(eq.literal);
        }
        return literals;
      }

      private RexNode getRef()
      {
        return parts.iterator().next().ref;
      }

      public int size()
      {
        return parts.size();
      }

      public List<RexNode> contract(List<RexNode> operands)
      {
        List<RexNode> newOperands = new ArrayList<RexNode>();
        Set<RexNode> origNodes = getOrigNodes();
        boolean add = true;
        for (RexNode op : operands) {
          if (origNodes.contains(op)) {
            if (add) {
              newOperands.add(buildIn());
            }
          } else {
            newOperands.add(op);
          }
        }
        return newOperands;
      }

      private Set<RexNode> getOrigNodes()
      {
        return parts.stream().map(eq -> eq.origNode).collect(Collectors.toSet());
      }

    }
  }

  static class Equality
  {
    private final RexNode origNode;
    private final RexNode ref;
    private final RexLiteral literal;

    public Equality(RexNode origNode, RexNode ref, RexLiteral literal)
    {
      this.origNode = origNode;
      this.ref = ref;
      this.literal = literal;
    }

    public static Equality of(RexNode rexNode)
    {
      if (rexNode.isA(SqlKind.EQUALS)) {
        RexCall call = (RexCall) rexNode;
        RexNode op0 = call.getOperands().get(0);
        RexNode op1 = call.getOperands().get(1);
        if (RexUtil.isDeterministic(op0) && op1.isA(SqlKind.LITERAL)) {
          return new Equality(rexNode, op0, (RexLiteral) op1);
        }
        if (RexUtil.isDeterministic(op1) && op0.isA(SqlKind.LITERAL)) {
          return new Equality(rexNode, op1, (RexLiteral) op0);
        }
      }
      return null;

    }

  }

}
