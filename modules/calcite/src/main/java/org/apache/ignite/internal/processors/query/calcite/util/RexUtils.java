/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

/** */
public class RexUtils {
    /** */
    public static RexNode makeCast(RexBuilder builder, RexNode node, RelDataType type) {
        return TypeUtils.needCast(builder.getTypeFactory(), node.getType(), type) ? builder.makeCast(type, node) : node;
    }

    /** */
    public static RexBuilder builder(RelNode rel) {
        return builder(rel.getCluster());
    }

    /** */
    public static RexBuilder builder(RelOptCluster cluster) {
        return cluster.getRexBuilder();
    }

    /** */
    public static RexExecutor executor(RelNode rel) {
        return executor(rel.getCluster());
    }

    /** */
    public static RexExecutor executor(RelOptCluster cluster) {
        return Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    }

    /** */
    public static RexSimplify simplifier(RelOptCluster cluster) {
        return new RexSimplify(builder(cluster), RelOptPredicateList.EMPTY, executor(cluster));
    }

    /** */
    public static RexNode makeCase(RexBuilder builder, RexNode... operands) {
        if (IgniteUtils.assertionsEnabled()) {
            // each odd operand except last one has to return a boolean type
            for (int i = 0; i < operands.length; i += 2) {
                if (operands[i].getType().getSqlTypeName() != SqlTypeName.BOOLEAN && i < operands.length - 1) {
                    throw new AssertionError("Unexpected operand type. " +
                        "[operands=" + Arrays.toString(operands) + "]");
                }
            }
        }

        return builder.makeCall(SqlStdOperatorTable.CASE, operands);
    }

    /** Returns whether a list of expressions projects the incoming fields. */
    public static boolean isIdentity(List<? extends RexNode> projects, RelDataType inputRowType) {
        return isIdentity(projects, inputRowType, false);
    }

    /** Returns whether a list of expressions projects the incoming fields. */
    public static boolean isIdentity(List<? extends RexNode> projects, RelDataType inputRowType, boolean local) {
        if (inputRowType.getFieldCount() != projects.size())
            return false;

        final List<RelDataTypeField> fields = inputRowType.getFieldList();
        Class<? extends RexSlot> clazz = local ? RexLocalRef.class : RexInputRef.class;

        for (int i = 0; i < fields.size(); i++) {
            if (!clazz.isInstance(projects.get(i)))
                return false;

            RexSlot ref = (RexSlot) projects.get(i);

            if (ref.getIndex() != i)
                return false;

            if (!RelOptUtil.eq("t1", projects.get(i).getType(), "t2", fields.get(i).getType(), Litmus.IGNORE))
                return false;
        }

        return true;
    }

    /** Supported index operations. */
    private static final Set<SqlKind> TREE_INDEX_COMPARISON =
        EnumSet.of(
            EQUALS,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    /**
     * Builds index conditions.
     */
    public static IndexConditions buildSortedIndexConditions(
        RelOptCluster cluster,
        RelCollation collation,
        RexNode condition,
        RelDataType rowType,
        ImmutableBitSet requiredColumns
    ) {
        if (condition == null)
            return new IndexConditions();

        condition = RexUtil.toCnf(builder(cluster), condition);

        Map<Integer, List<RexCall>> fieldsToPredicates = mapPredicatesToFields(condition, cluster);

        if (nullOrEmpty(fieldsToPredicates))
            return new IndexConditions();

        List<RexNode> lower = new ArrayList<>();
        List<RexNode> upper = new ArrayList<>();

        // Force collation for all fields of the condition.
        if (collation == null || collation.isDefault())
            collation = RelCollations.of(ImmutableIntList.copyOf(fieldsToPredicates.keySet()));

        for (int i = 0; i < collation.getFieldCollations().size(); i++) {
            RelFieldCollation fc = collation.getFieldCollations().get(i);

            int collFldIdx = fc.getFieldIndex();

            List<RexCall> collFldPreds = fieldsToPredicates.get(collFldIdx);

            if (nullOrEmpty(collFldPreds))
                break;

            RexNode bestUpper = null;
            RexNode bestLower = null;

            for (RexCall pred : collFldPreds) {
                if (IgniteUtils.assertionsEnabled()) {
                    RexNode cond = RexUtil.removeCast(pred.operands.get(1));

                    assert idxOpSupports(cond) : cond;
                }

                boolean lowerBoundBelow = !fc.getDirection().isDescending();
                SqlOperator op = pred.getOperator();
                switch (op.kind) {
                    case EQUALS:
                        bestUpper = pred;
                        bestLower = pred;
                        break;

                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        lowerBoundBelow = !lowerBoundBelow;
                        // Fall through.

                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        if (lowerBoundBelow)
                            bestLower = pred;
                        else
                            bestUpper = pred;
                        break;

                    default:
                        throw new AssertionError("Unknown condition: " + op.kind);
                }

                if (bestUpper != null && bestLower != null)
                    break; // We've found either "=" condition or both lower and upper.
            }

            if (bestLower == null && bestUpper == null)
                break; // No bounds, so break the loop.

            if (i > 0 && bestLower != bestUpper)
                // Go behind the first index field only in the case of multiple "=" conditions on index fields.
                break; // TODO https://issues.apache.org/jira/browse/IGNITE-13568

            if (bestLower != null && bestUpper != null) { // "x>5 AND x<10"
                upper.add(bestUpper);
                lower.add(bestLower);

                if (bestLower != bestUpper)
                    break;
            }
            else if (bestLower != null) { // "x>5"
                lower.add(bestLower);

                break; // TODO https://issues.apache.org/jira/browse/IGNITE-13568
            }
            else { // "x<10"
                upper.add(bestUpper);

                break; // TODO https://issues.apache.org/jira/browse/IGNITE-13568
            }
        }

        Mappings.TargetMapping mapping = null;

        if (requiredColumns != null)
            mapping = Commons.inverseMapping(requiredColumns, rowType.getFieldCount());

        List<RexNode> lowerBound = null;
        List<RexNode> upperBound = null;

        if (!nullOrEmpty(lower))
            lowerBound = asBound(cluster, lower, rowType, mapping);
        else
            lower = null;

        if (!nullOrEmpty(upper))
            upperBound = asBound(cluster, upper, rowType, mapping);
        else
            upper = null;

        return new IndexConditions(lower, upper, lowerBound, upperBound);
    }

    /**
     * Builds index conditions.
     */
    public static List<RexNode> buildHashSearchRow(
        RelOptCluster cluster,
        RexNode condition,
        RelDataType rowType
    ) {
        condition = RexUtil.toCnf(builder(cluster), condition);

        Map<Integer, List<RexCall>> fieldsToPredicates = mapPredicatesToFields(condition, cluster);

        if (nullOrEmpty(fieldsToPredicates))
            return null;

        List<RexNode> searchPreds = null;

        for (int fldIdx : fieldsToPredicates.keySet()) {
            List<RexCall> collFldPreds = fieldsToPredicates.get(fldIdx);

            if (nullOrEmpty(collFldPreds))
                break;

            for (RexCall pred : collFldPreds) {
                if (IgniteUtils.assertionsEnabled()) {
                    RexNode cond = RexUtil.removeCast(pred.operands.get(1));

                    assert idxOpSupports(cond) : cond;
                }

                if (pred.getOperator().kind != SqlKind.EQUALS)
                    return null;

                if (searchPreds == null)
                    searchPreds = new ArrayList<>();

                searchPreds.add(pred);
            }
        }

        if (searchPreds == null)
            return null;

        return asBound(cluster, searchPreds, rowType, null);
    }

    /** */
    private static Map<Integer, List<RexCall>> mapPredicatesToFields(RexNode condition, RelOptCluster cluster) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);

        Map<Integer, List<RexCall>> res = new HashMap<>(conjunctions.size());

        for (RexNode rexNode : conjunctions) {
            if (!isBinaryComparison(rexNode))
                continue;

            RexCall predCall = (RexCall)rexNode;
            RexSlot ref = (RexSlot)extractRef(predCall);

            if (ref == null)
                continue;

            // Let RexLocalRef be on the left side.
            if (refOnTheRight(predCall))
                predCall = (RexCall)RexUtil.invert(builder(cluster), predCall);

            List<RexCall> fldPreds = res.computeIfAbsent(ref.getIndex(), k -> new ArrayList<>(conjunctions.size()));

            fldPreds.add(predCall);
        }
        return res;
    }

    /** */
    private static RexNode extractRef(RexCall call) {
        assert isBinaryComparison(call);

        RexNode leftOp = call.getOperands().get(0);
        RexNode rightOp = call.getOperands().get(1);

        leftOp = RexUtil.removeCast(leftOp);
        rightOp = RexUtil.removeCast(rightOp);

        if ((leftOp instanceof RexLocalRef || leftOp instanceof RexInputRef) && idxOpSupports(rightOp))
            return leftOp;
        else if ((rightOp instanceof RexLocalRef || rightOp instanceof RexInputRef) && idxOpSupports(leftOp))
            return rightOp;

        return null;
    }

    /** */
    private static boolean refOnTheRight(RexCall predCall) {
        RexNode rightOp = predCall.getOperands().get(1);

        rightOp = RexUtil.removeCast(rightOp);

        return rightOp.isA(SqlKind.LOCAL_REF) || rightOp.isA(SqlKind.INPUT_REF);
    }

    /** */
    private static boolean isBinaryComparison(RexNode exp) {
        return TREE_INDEX_COMPARISON.contains(exp.getKind()) &&
            (exp instanceof RexCall) &&
            ((RexCall)exp).getOperands().size() == 2;
    }

    /** */
    private static boolean idxOpSupports(RexNode op) {
        return op instanceof RexLiteral
            || op instanceof RexDynamicParam
            || op instanceof RexFieldAccess;
    }

    /** */
    private static List<RexNode> makeListOfNullLiterals(RexBuilder builder, List<RelDataType> types) {
        return Commons.transform(types, builder::makeNullLiteral);
    }

    /** */
    public static boolean isNotNull(RexNode op) {
        if (op == null)
            return false;

        return !(op instanceof RexLiteral) || !((RexLiteral)op).isNull();
    }

    /** */
    public static List<RexNode> asBound(RelOptCluster cluster, Iterable<RexNode> idxCond, RelDataType rowType,
        @Nullable Mappings.TargetMapping mapping) {
        if (nullOrEmpty(idxCond))
            return null;

        RexBuilder builder = builder(cluster);
        List<RelDataType> types = RelOptUtil.getFieldTypeList(rowType);
        List<RexNode> res = makeListOfNullLiterals(builder, types);

        for (RexNode pred : idxCond) {
            assert pred instanceof RexCall;

            RexCall call = (RexCall)pred;
            RexSlot ref = (RexSlot)RexUtil.removeCast(call.operands.get(0));
            RexNode cond = RexUtil.removeCast(call.operands.get(1));

            assert idxOpSupports(cond) : cond;

            int index = mapping == null ? ref.getIndex() : mapping.getSourceOpt(ref.getIndex());

            assert index != -1;

            res.set(index, makeCast(builder, cond, types.get(index)));
        }

        return res;
    }

    /** */
    public static Mappings.TargetMapping permutation(List<RexNode> nodes, RelDataType inputRowType, boolean local) {
        final Mappings.TargetMapping mapping =
            Mappings.create(MappingType.PARTIAL_FUNCTION, nodes.size(), inputRowType.getFieldCount());

        Class<? extends RexSlot> clazz = local ? RexLocalRef.class : RexInputRef.class;

        for (Ord<RexNode> node : Ord.zip(nodes)) {
            if (clazz.isInstance(node.e))
                mapping.set(node.i, ((RexSlot)node.e).getIndex());
        }
        return mapping;
    }

    /** */
    public static Mappings.TargetMapping inversePermutation(List<RexNode> nodes, RelDataType inputRowType, boolean local) {
        final Mappings.TargetMapping mapping =
            Mappings.create(MappingType.INVERSE_FUNCTION, nodes.size(), inputRowType.getFieldCount());

        Class<? extends RexSlot> clazz = local ? RexLocalRef.class : RexInputRef.class;

        for (Ord<RexNode> node : Ord.zip(nodes)) {
            if (clazz.isInstance(node.e))
                mapping.set(node.i, ((RexSlot)node.e).getIndex());
        }
        return mapping;
    }

    /** */
    public static Mappings.TargetMapping permutation(List<RexNode> nodes, RelDataType inputRowType) {
        return permutation(nodes, inputRowType, false);
    }

    /** */
    public static List<RexNode> replaceLocalRefs(List<RexNode> nodes) {
        return LocalRefReplacer.INSTANCE.apply(nodes);
    }

    /** */
    public static List<RexNode> replaceInputRefs(List<RexNode> nodes) {
        return InputRefReplacer.INSTANCE.apply(nodes);
    }

    /** */
    public static RexNode replaceLocalRefs(RexNode node) {
        return LocalRefReplacer.INSTANCE.apply(node);
    }

    /** */
    public static RexNode replaceInputRefs(RexNode node) {
        return InputRefReplacer.INSTANCE.apply(node);
    }

    /** */
    public static boolean hasCorrelation(RexNode node) {
        return hasCorrelation(Collections.singletonList(node));
    }

    /** */
    public static boolean hasCorrelation(List<RexNode> nodes) {
        try {
            RexVisitor<Void> v = new RexVisitorImpl<Void>(true) {
                @Override public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
                    throw new ControlFlowException();
                }
            };

            nodes.forEach(n -> n.accept(v));

            return false;
        }
        catch (ControlFlowException e) {
            return true;
        }
    }

    /** */
    public static Set<CorrelationId> extractCorrelationIds(RexNode node) {
        if (node == null)
            return Collections.emptySet();

        return extractCorrelationIds(Collections.singletonList(node));
    }

    /** */
    public static Set<Integer> notNullKeys(List<RexNode> row) {
        if (nullOrEmpty(row))
            return Collections.emptySet();

        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < row.size(); ++i ) {
            if (isNotNull(row.get(i)))
                keys.add(i);
        }

        return keys;
    }

    /** */
    public static Set<CorrelationId> extractCorrelationIds(List<RexNode> nodes) {
        final Set<CorrelationId> cors = new HashSet<>();

        RexVisitor<Void> v = new RexVisitorImpl<Void>(true) {
            @Override public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
                cors.add(correlVariable.id);

                return null;
            }
        };

        nodes.forEach(rex -> rex.accept(v));

        return cors;
    }

    /** Visitor for replacing scan local refs to input refs. */
    private static class LocalRefReplacer extends RexShuttle {
        /** */
        private static final RexShuttle INSTANCE = new LocalRefReplacer();

        /** {@inheritDoc} */
        @Override public RexNode visitLocalRef(RexLocalRef inputRef) {
            return new RexInputRef(inputRef.getIndex(), inputRef.getType());
        }
    }

    /** Visitor for replacing input refs to local refs. We need it for proper plan serialization. */
    private static class InputRefReplacer extends RexShuttle {
        /** */
        private static final RexShuttle INSTANCE = new InputRefReplacer();

        /** {@inheritDoc} */
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            return new RexLocalRef(inputRef.getIndex(), inputRef.getType());
        }
    }
}
