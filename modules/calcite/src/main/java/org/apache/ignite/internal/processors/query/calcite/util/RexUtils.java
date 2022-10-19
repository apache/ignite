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
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.rex.RexUtil.removeCast;
import static org.apache.calcite.rex.RexUtil.sargRef;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.IS_NOT_NULL;
import static org.apache.calcite.sql.SqlKind.IS_NULL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.NOT;
import static org.apache.calcite.sql.SqlKind.SEARCH;

/** */
public class RexUtils {
    /** Maximum amount of search bounds tuples per scan. */
    public static final int MAX_SEARCH_BOUNDS_COMPLEXITY = 100;

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
        if (U.assertionsEnabled()) {
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

            RexSlot ref = (RexSlot)projects.get(i);

            if (ref.getIndex() != i)
                return false;

            if (!RelOptUtil.eq("t1", projects.get(i).getType(), "t2", fields.get(i).getType(), Litmus.IGNORE))
                return false;
        }

        return true;
    }

    /** Binary comparison operations. */
    private static final Set<SqlKind> BINARY_COMPARISON =
        EnumSet.of(EQUALS, LESS_THAN, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    /** Supported index operations. */
    private static final Set<SqlKind> TREE_INDEX_COMPARISON =
        EnumSet.of(
            SEARCH,
            IS_NULL,
            IS_NOT_NULL,
            EQUALS,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    /**
     * Builds sorted search bounds.
     */
    public static List<SearchBounds> buildSortedSearchBounds(
        RelOptCluster cluster,
        RelCollation collation,
        RexNode condition,
        RelDataType rowType,
        ImmutableBitSet requiredColumns
    ) {
        if (condition == null)
            return null;

        condition = RexUtil.toCnf(builder(cluster), condition);

        Map<Integer, List<RexCall>> fieldsToPredicates = mapPredicatesToFields(condition, cluster);

        if (F.isEmpty(fieldsToPredicates))
            return null;

        // Force collation for all fields of the condition.
        if (collation == null || collation.isDefault()) {
            List<Integer> equalsFields = new ArrayList<>(fieldsToPredicates.size());
            List<Integer> otherFields = new ArrayList<>(fieldsToPredicates.size());

            // It's more effective to put equality conditions in the collation first.
            fieldsToPredicates.forEach((idx, conds) ->
                (F.exist(conds, call -> call.getOperator().getKind() == EQUALS) ? equalsFields : otherFields).add(idx));

            collation = TraitUtils.createCollation(F.concat(true, equalsFields, otherFields));
        }

        List<RelDataType> types = RelOptUtil.getFieldTypeList(rowType);

        Mappings.TargetMapping mapping = null;

        if (requiredColumns != null)
            mapping = Commons.inverseMapping(requiredColumns, types.size());

        List<SearchBounds> bounds = Arrays.asList(new SearchBounds[types.size()]);
        boolean boundsEmpty = true;
        int prevComplexity = 1;

        for (int i = 0; i < collation.getFieldCollations().size(); i++) {
            RelFieldCollation fc = collation.getFieldCollations().get(i);

            int collFldIdx = fc.getFieldIndex();

            List<RexCall> collFldPreds = fieldsToPredicates.get(collFldIdx);

            if (F.isEmpty(collFldPreds))
                break;

            if (mapping != null)
                collFldIdx = mapping.getSourceOpt(collFldIdx);

            SearchBounds fldBounds = createBounds(fc, collFldPreds, cluster, types.get(collFldIdx), prevComplexity);

            if (fldBounds == null)
                break;

            boundsEmpty = false;

            bounds.set(collFldIdx, fldBounds);

            if (fldBounds instanceof MultiBounds) {
                prevComplexity *= ((MultiBounds)fldBounds).bounds().size();

                // Any bounds after multi range bounds are not allowed, since it can cause intervals intersection.
                if (((MultiBounds)fldBounds).bounds().stream().anyMatch(b -> b.type() != SearchBounds.Type.EXACT))
                    break;
            }

            if (fldBounds.type() == SearchBounds.Type.RANGE)
                break; // TODO https://issues.apache.org/jira/browse/IGNITE-13568
        }

        return boundsEmpty ? null : bounds;
    }

    /**
     * Builds hash index search bounds.
     */
    public static List<SearchBounds> buildHashSearchBounds(
        RelOptCluster cluster,
        RexNode condition,
        RelDataType rowType,
        ImmutableBitSet requiredColumns,
        boolean ignoreNotEqualPreds
    ) {
        condition = RexUtil.toCnf(builder(cluster), condition);

        Map<Integer, List<RexCall>> fieldsToPredicates = mapPredicatesToFields(condition, cluster);

        if (F.isEmpty(fieldsToPredicates))
            return null;

        List<SearchBounds> bounds = null;

        List<RelDataType> types = RelOptUtil.getFieldTypeList(rowType);

        Mappings.TargetMapping mapping = null;

        if (requiredColumns != null)
            mapping = Commons.inverseMapping(requiredColumns, types.size());

        for (int fldIdx : fieldsToPredicates.keySet()) {
            List<RexCall> collFldPreds = fieldsToPredicates.get(fldIdx);

            if (F.isEmpty(collFldPreds))
                break;

            for (RexCall pred : collFldPreds) {
                if (pred.getOperator().kind != SqlKind.EQUALS) {
                    if (ignoreNotEqualPreds)
                        continue;
                    else // Only EQUALS predicates allowed in condition.
                        return null;
                }

                if (bounds == null)
                    bounds = Arrays.asList(new SearchBounds[types.size()]);

                if (mapping != null)
                    fldIdx = mapping.getSourceOpt(fldIdx);

                bounds.set(fldIdx, createBounds(null, Collections.singletonList(pred), cluster, types.get(fldIdx), 1));
            }
        }

        return bounds;
    }

    /** Create index search bound by conditions of the field. */
    private static SearchBounds createBounds(
        @Nullable RelFieldCollation fc, // Can be null for EQUALS condition.
        List<RexCall> collFldPreds,
        RelOptCluster cluster,
        RelDataType fldType,
        int prevComplexity
    ) {
        RexBuilder builder = builder(cluster);

        RexNode nullVal = builder.makeNullLiteral(fldType);

        RexNode upperCond = null;
        RexNode lowerCond = null;
        RexNode upperBound = null;
        RexNode lowerBound = null;
        boolean upperInclude = true;
        boolean lowerInclude = true;

        for (RexCall pred : collFldPreds) {
            RexNode val = null;
            RexNode ref = pred.getOperands().get(0);

            if (isBinaryComparison(pred)) {
                val = removeCast(pred.operands.get(1));

                assert idxOpSupports(val) : val;

                val = makeCast(builder, val, fldType);
            }

            SqlOperator op = pred.getOperator();

            if (op.kind == EQUALS)
                return new ExactBounds(pred, val);
            else if (op.kind == IS_NULL)
                return new ExactBounds(pred, nullVal);
            else if (op.kind == SEARCH) {
                Sarg<?> sarg = ((RexLiteral)pred.operands.get(1)).getValueAs(Sarg.class);

                List<SearchBounds> bounds = expandSargToBounds(fc, cluster, fldType, prevComplexity, sarg, ref);

                if (bounds == null)
                    continue;

                if (bounds.size() == 1) {
                    if (bounds.get(0) instanceof RangeBounds && collFldPreds.size() > 1) {
                        // Try to merge bounds.
                        boolean ascDir = !fc.getDirection().isDescending();
                        RangeBounds rangeBounds = (RangeBounds)bounds.get(0);
                        if (rangeBounds.lowerBound() != null) {
                            if (lowerBound != null && lowerBound != nullVal) {
                                lowerBound = leastOrGreatest(builder, !ascDir, lowerBound, rangeBounds.lowerBound(), nullVal);
                                lowerInclude |= rangeBounds.lowerInclude();
                            }
                            else {
                                lowerBound = rangeBounds.lowerBound();
                                lowerInclude = rangeBounds.lowerInclude();
                            }
                            lowerCond = lessOrGreater(builder, !ascDir, lowerInclude, ref, lowerBound);
                        }

                        if (rangeBounds.upperBound() != null) {
                            if (upperBound != null && upperBound != nullVal) {
                                upperBound = leastOrGreatest(builder, ascDir, upperBound, rangeBounds.upperBound(), nullVal);
                                upperInclude |= rangeBounds.upperInclude();
                            }
                            else {
                                upperBound = rangeBounds.upperBound();
                                upperInclude = rangeBounds.upperInclude();
                            }
                            upperCond = lessOrGreater(builder, ascDir, upperInclude, ref, upperBound);
                        }

                        continue;
                    }
                    else
                        return bounds.get(0);
                }

                return new MultiBounds(pred, bounds);
            }

            // Range bounds.
            boolean lowerBoundBelow = !fc.getDirection().isDescending();
            boolean includeBound = op.kind == GREATER_THAN_OR_EQUAL || op.kind == LESS_THAN_OR_EQUAL;
            boolean lessCondition = false;

            switch (op.kind) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    lessCondition = true;
                    lowerBoundBelow = !lowerBoundBelow;
                    // Fall through.

                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    if (lowerBoundBelow) {
                        if (lowerBound == null || lowerBound == nullVal) {
                            lowerCond = pred;
                            lowerBound = val;
                            lowerInclude = includeBound;
                        }
                        else {
                            lowerBound = leastOrGreatest(builder, lessCondition, lowerBound, val, nullVal);
                            lowerInclude |= includeBound;
                            lowerCond = lessOrGreater(builder, lessCondition, lowerInclude, ref, lowerBound);
                        }
                    }
                    else {
                        if (upperBound == null || upperBound == nullVal) {
                            upperCond = pred;
                            upperBound = val;
                            upperInclude = includeBound;
                        }
                        else {
                            upperBound = leastOrGreatest(builder, lessCondition, upperBound, val, nullVal);
                            upperInclude |= includeBound;
                            upperCond = lessOrGreater(builder, lessCondition, upperInclude, ref, upperBound);
                        }
                    }
                    // Fall through.

                case IS_NOT_NULL:
                    if (fc.nullDirection == RelFieldCollation.NullDirection.FIRST && lowerBound == null) {
                        lowerCond = pred;
                        lowerBound = nullVal;
                        lowerInclude = false;
                    }
                    else if (fc.nullDirection == RelFieldCollation.NullDirection.LAST && upperBound == null) {
                        upperCond = pred;
                        upperBound = nullVal;
                        upperInclude = false;
                    }
                    break;

                default:
                    throw new AssertionError("Unknown condition: " + op.kind);
            }
        }

        if (lowerBound == null && upperBound == null)
            return null; // No bounds.

        // Found upper bound, lower bound or both.
        RexNode cond = lowerCond == null ? upperCond :
            upperCond == null ? lowerCond :
                upperCond == lowerCond ? lowerCond : builder.makeCall(SqlStdOperatorTable.AND, lowerCond, upperCond);

        return new RangeBounds(cond, lowerBound, upperBound, lowerInclude, upperInclude);
    }

    /** */
    private static List<SearchBounds> expandSargToBounds(
        RelFieldCollation fc,
        RelOptCluster cluster,
        RelDataType fldType,
        int prevComplexity,
        Sarg<?> sarg,
        RexNode ref
    ) {
        int complexity = prevComplexity * sarg.complexity();

        // Limit amount of search bounds tuples.
        if (complexity > MAX_SEARCH_BOUNDS_COMPLEXITY)
            return null;

        RexBuilder builder = builder(cluster);

        RexNode sargCond = sargRef(builder, ref, sarg, fldType, RexUnknownAs.UNKNOWN);
        List<RexNode> disjunctions = RelOptUtil.disjunctions(RexUtil.toDnf(builder, sargCond));
        List<SearchBounds> bounds = new ArrayList<>(disjunctions.size());

        for (RexNode bound : disjunctions) {
            List<RexNode> conjunctions = RelOptUtil.conjunctions(bound);
            List<RexCall> calls = new ArrayList<>(conjunctions.size());

            for (RexNode rexNode : conjunctions) {
                if (isSupportedTreeComparison(rexNode))
                    calls.add((RexCall)rexNode);
                else // Cannot filter using this predicate (NOT_EQUALS for example), give a chance to other predicates.
                    return null;
            }

            bounds.add(createBounds(fc, calls, cluster, fldType, complexity));
        }

        return bounds;
    }

    /** */
    private static RexNode leastOrGreatest(RexBuilder builder, boolean least, RexNode arg0, RexNode arg1, RexNode nullVal) {
        // There is no implementor for LEAST/GREATEST, so convert this calls directly to CASE operator.
        List<RexNode> argList = new ArrayList<>();

        // CASE
        //  WHEN arg0 IS NULL OR arg1 IS NULL THEN NULL
        //  WHEN arg0 < arg1 THEN arg0
        //  ELSE arg1
        // END
        argList.add(builder.makeCall(SqlStdOperatorTable.OR,
            builder.makeCall(SqlStdOperatorTable.IS_NULL, arg0),
            builder.makeCall(SqlStdOperatorTable.IS_NULL, arg1)));
        argList.add(nullVal);
        argList.add(builder.makeCall(least ? SqlStdOperatorTable.LESS_THAN : SqlStdOperatorTable.GREATER_THAN, arg0, arg1));
        argList.add(arg0);
        argList.add(arg1);

        return builder.makeCall(SqlStdOperatorTable.CASE, argList);
    }

    /** */
    private static RexNode lessOrGreater(
        RexBuilder builder,
        boolean less,
        boolean includeBound,
        RexNode arg0,
        RexNode arg1
    ) {
        return builder.makeCall(less ?
            (includeBound ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN) :
            (includeBound ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.GREATER_THAN),
            arg0, arg1);
    }

    /**
     * Builds index conditions.
     */
    public static List<RexNode> buildHashSearchRow(
        RelOptCluster cluster,
        RexNode condition,
        RelDataType rowType
    ) {
        List<SearchBounds> searchBounds = buildHashSearchBounds(cluster, condition, rowType, null, false);

        if (searchBounds == null)
            return null;

        return Commons.transform(searchBounds, b -> {
            assert b == null || b instanceof ExactBounds : b;

            return b == null ? null : ((ExactBounds)b).bound();
        });
    }

    /** */
    private static Map<Integer, List<RexCall>> mapPredicatesToFields(RexNode condition, RelOptCluster cluster) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);

        Map<Integer, List<RexCall>> res = new HashMap<>(conjunctions.size());

        for (RexNode rexNode : conjunctions) {
            rexNode = expandBooleanFieldComparison(rexNode, builder(cluster));

            if (!isSupportedTreeComparison(rexNode))
                continue;

            RexCall predCall = (RexCall)rexNode;
            RexSlot ref;

            if (isBinaryComparison(rexNode)) {
                ref = (RexSlot)extractRefFromBinary(predCall, cluster);

                if (ref == null)
                    continue;

                // Let RexLocalRef be on the left side.
                if (refOnTheRight(predCall))
                    predCall = (RexCall)RexUtil.invert(builder(cluster), predCall);
            }
            else {
                ref = (RexSlot)extractRefFromOperand(predCall, cluster, 0);

                if (ref == null)
                    continue;
            }

            List<RexCall> fldPreds = res.computeIfAbsent(ref.getIndex(), k -> new ArrayList<>(conjunctions.size()));

            fldPreds.add(predCall);
        }
        return res;
    }

    /** */
    private static RexNode expandBooleanFieldComparison(RexNode rexNode, RexBuilder builder) {
        if (rexNode instanceof RexSlot)
            return builder.makeCall(SqlStdOperatorTable.EQUALS, rexNode, builder.makeLiteral(true));
        else if (rexNode instanceof RexCall && rexNode.getKind() == NOT &&
            ((RexCall)rexNode).getOperands().get(0) instanceof RexSlot) {
            return builder.makeCall(SqlStdOperatorTable.EQUALS, ((RexCall)rexNode).getOperands().get(0),
                builder.makeLiteral(false));
        }

        return rexNode;
    }

    /** */
    private static RexNode extractRefFromBinary(RexCall call, RelOptCluster cluster) {
        assert isBinaryComparison(call);

        RexNode leftRef = extractRefFromOperand(call, cluster, 0);
        RexNode rightOp = call.getOperands().get(1);

        if (leftRef != null)
            return idxOpSupports(removeCast(rightOp)) ? leftRef : null;

        RexNode rightRef = extractRefFromOperand(call, cluster, 1);
        RexNode leftOp = call.getOperands().get(0);

        if (rightRef != null)
            return idxOpSupports(removeCast(leftOp)) ? rightRef : null;

        return null;
    }

    /** */
    private static RexNode extractRefFromOperand(RexCall call, RelOptCluster cluster, int operandNum) {
        assert isSupportedTreeComparison(call);

        RexNode op = call.getOperands().get(operandNum);

        op = removeCast(op);

        // Can proceed without ref cast only if cast was redundant in terms of values comparison.
        if ((op instanceof RexSlot) &&
            !TypeUtils.needCast(cluster.getTypeFactory(), op.getType(), call.getOperands().get(operandNum).getType()))
            return op;

        return null;
    }

    /** */
    private static boolean refOnTheRight(RexCall predCall) {
        RexNode rightOp = predCall.getOperands().get(1);

        rightOp = removeCast(rightOp);

        return rightOp.isA(SqlKind.LOCAL_REF) || rightOp.isA(SqlKind.INPUT_REF);
    }

    /** */
    private static boolean isBinaryComparison(RexNode exp) {
        return BINARY_COMPARISON.contains(exp.getKind()) &&
            (exp instanceof RexCall) &&
            ((RexCall)exp).getOperands().size() == 2;
    }

    /** */
    private static boolean isSupportedTreeComparison(RexNode exp) {
        return TREE_INDEX_COMPARISON.contains(exp.getKind()) &&
            (exp instanceof RexCall);
    }

    /** */
    private static boolean idxOpSupports(RexNode op) {
        return op instanceof RexLiteral
            || op instanceof RexDynamicParam
            || op instanceof RexFieldAccess;
    }

    /** */
    public static boolean isNotNull(RexNode op) {
        if (op == null)
            return false;

        return !(op instanceof RexLiteral) || !((RexLiteral)op).isNull();
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
        if (F.isEmpty(row))
            return Collections.emptySet();

        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < row.size(); ++i ) {
            if (isNotNull(row.get(i)))
                keys.add(i);
        }

        return keys;
    }

    /** @return Double value of the literal expression. */
    public static double doubleFromRex(RexNode n, double def) {
        try {
            if (n.isA(SqlKind.LITERAL))
                return ((RexLiteral)n).getValueAs(Double.class);
            else
                return def;
        }
        catch (Exception e) {
            assert false : "Unable to extract value: " + e.getMessage();

            return def;
        }
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
