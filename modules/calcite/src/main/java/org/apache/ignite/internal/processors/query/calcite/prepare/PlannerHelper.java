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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.hint.HintUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.IgniteMultiJoinOptimizeRule;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class PlannerHelper {
    /**
     * Rule {@link JoinCommuteRule} takes too long when joins number grows. We disable this rule if query has joins
     * count bigger than this value.
     */
    public static final int MAX_JOINS_TO_COMMUTE = 3;

    /**
     * Rules {@link JoinPushThroughJoinRule} (left and right) take too long when joins number grows. We disable this rule
     * if query has joins count bigger than this value.
     */
    public static final int MAX_JOINS_TO_COMMUTE_INPUTS = 5;

    /**
     * Mininal joins number to launch {@link IgniteMultiJoinOptimizeRule}. Calcite's default join order optimization rules
     * like {@link JoinCommuteRule} or {@link JoinPushThroughJoinRule} take time but can give us better plans. They produce
     * more join variants. And we estimate their physical costs. While the joins count is small, let's use the default rules.
     *
     * @see #optimizeJoinsOrder(IgnitePlanner, RelNode, List)
     */
    public static final int JOINS_COUNT_FOR_HEURISTIC_ORDER = 3;

    /**
     * Default constructor.
     */
    private PlannerHelper() {

    }

    /**
     * @param sqlNode Sql node.
     * @param planner Planner.
     * @param log Logger.
     */
    public static IgniteRel optimize(SqlNode sqlNode, IgnitePlanner planner, IgniteLogger log) {
        try {
            // Convert to Relational operators graph.
            RelRoot root = planner.rel(sqlNode);

            root = addExternalOptions(root);

            planner.addDisabledRules(HintUtils.options(root.rel, extractRootHints(root.rel), HintDefinition.DISABLE_RULE));

            RelNode rel = root.rel;

            // Transformation chain
            rel = planner.transform(PlannerPhase.HEP_DECORRELATE, rel.getTraitSet(), rel);

            // RelOptUtil#propagateRelHints(RelNode, equiv) may skip hints because current RelNode has no hints.
            // Or if hints reside in a child nodes which are not inputs of the current node. Like LogicalFlter#condition.
            // Such hints may appear or be required below in the tree, after rules applying.
            // In Calcite, RelDecorrelator#decorrelateQuery(...) can re-propagate hints.
            rel = RelOptUtil.propagateRelHints(rel, false);

            rel = planner.replaceCorrelatesCollisions(rel);

            rel = planner.extractConjunctionOverDisjunctionCommonPart(rel);

            rel = planner.trimUnusedFields(root.withRel(rel)).rel;

            rel = planner.transform(PlannerPhase.HEP_FILTER_PUSH_DOWN, rel.getTraitSet(), rel);

            // The following pushed down project can erase top-level hints. We store them to reassign hints for join nodes.
            // Clear the inherit pathes to consider the hints as not propogated ones.
            List<RelHint> topHints = HintUtils.allRelHints(rel).stream().map(h -> h.inheritPath.isEmpty()
                ? h
                : h.copy(Collections.emptyList())).collect(Collectors.toList());

            rel = planner.transform(PlannerPhase.HEP_PROJECT_PUSH_DOWN, rel.getTraitSet(), rel);

            rel = optimizeJoinsOrder(planner, rel, topHints);

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(root.collation == null ? RelCollations.EMPTY : root.collation)
                .simplify();

            IgniteRel igniteRel = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

            if (!root.isRefTrivial()) {
                final List<RexNode> projects = new ArrayList<>();
                final RexBuilder rexBuilder = igniteRel.getCluster().getRexBuilder();

                for (int field : Pair.left(root.fields))
                    projects.add(rexBuilder.makeInputRef(igniteRel, field));

                igniteRel = new IgniteProject(igniteRel.getCluster(), desired, igniteRel, projects, root.validatedRowType);
            }

            if (sqlNode.isA(ImmutableSet.of(SqlKind.INSERT, SqlKind.UPDATE, SqlKind.MERGE)))
                igniteRel = new FixDependentModifyNodeShuttle().visit(igniteRel);

            log.error("TEST | Plan:\n" + RelOptUtil.toString(igniteRel));

            return igniteRel;
        }
        catch (Throwable ex) {
            log.error("Unexpected error at query optimizer.", ex);
            log.error(planner.dump());

            throw ex;
        }
    }

    /**
     * To prevent long join order planning, disables {@link JoinCommuteRule} and/or {@link JoinPushThroughJoinRule} rules
     * if the joins count reaches the thresholds.
     *
     * @return Original {@code rel}.
     */
    private static RelNode checkJoinsCommutes(IgnitePlanner planner, RelNode rel) {
        int joinsCnt = RelOptUtil.countJoins(rel);

        if (joinsCnt > MAX_JOINS_TO_COMMUTE)
            planner.addDisabledRules(Collections.singletonList(CoreRules.JOIN_COMMUTE.toString()));

        if (joinsCnt > MAX_JOINS_TO_COMMUTE_INPUTS) {
            planner.addDisabledRules(Arrays.asList(JoinPushThroughJoinRule.LEFT.toString(),
                JoinPushThroughJoinRule.RIGHT.toString()));
        }

        return rel;
    }

    /**
     * Tries to optimize joins order.
     *
     * @see JoinToMultiJoinRule
     * @see IgniteMultiJoinOptimizeRule
     *
     * @return An node with optimized joins or original {@code root} if didn't optimize.
     */
    private static RelNode optimizeJoinsOrder(IgnitePlanner planner, RelNode root, List<RelHint> topLevelHints) {
        List<Join> joins = findNodes(root, Join.class, false);

        if (joins.isEmpty())
            return checkJoinsCommutes(planner, root);

        int disabledCnt = 0;

        // If all the joins have the forced order, no need to optimize the joins order at all.
        for (RelNode join : joins) {
            for (RelHint hint : ((Hintable)join).getHints()) {
                if (HintDefinition.ENFORCE_JOIN_ORDER.name().equals(hint.hintName)) {
                    ++disabledCnt;

                    break;
                }
            }
        }

        if (joins.size() - disabledCnt < JOINS_COUNT_FOR_HEURISTIC_ORDER)
            return checkJoinsCommutes(planner, root);

        RelNode res = planner.transform(PlannerPhase.HEP_OPTIMIZE_JOIN_ORDER, root.getTraitSet(), root);

        // Still has a MultiJoin, didn't manage to collect one flat join to optimize.
        if (!findNodes(res, MultiJoin.class, true).isEmpty())
            return checkJoinsCommutes(planner, root);

        // If a new joins order was proposed, no need to launch another join order optimizations.
        planner.addDisabledRules(HintDefinition.ENFORCE_JOIN_ORDER.disabledRules().stream().map(RelOptRule::toString)
            .collect(Collectors.toSet()));

        if (!topLevelHints.isEmpty()) {
            res = actualTopLevelJoinTypeHints(res, topLevelHints, joins.get(0));

            restoreJoinTypeHints(res);
        }

        return res;
    }

    /** */
    private static RelNode actualTopLevelJoinTypeHints(RelNode rel, List<RelHint> topLevelHints, Join filterNode) {
        assert rel instanceof Hintable;

        // Ignore inheritance to compare hints type and options.
        List<RelHint> filteredRelHints = ((Hintable)rel).getHints().stream()
            .map(h -> h.inheritPath.isEmpty() ? h : h.copy(Collections.emptyList())).collect(Collectors.toList());

        List<RelHint> res = new ArrayList<>(topLevelHints.size());

        for (RelHint topHint : topLevelHints) {
            assert topHint.inheritPath.isEmpty();

            boolean storeHint = true;

            for (RelHint curHint : filteredRelHints) {
                if (topHint.equals(curHint)) {
                    storeHint = false;

                    break;
                }
            }

            if (storeHint)
                res.add(topHint);
        }

        // Keep hints only for joins.
        res = Commons.context(filterNode).config().getSqlToRelConverterConfig().getHintStrategyTable().apply(res, filterNode);

        if (!res.isEmpty())
            rel = ((Hintable)rel).attachHints(res);

        return rel;
    }

    /**
     * A join type hint might be assigned to a query root (top-level hint) or to a table. Originally, SELECT-level hints
     * are propagated and assigned to following Joins and TableScans. We lose assigned to Join nodes ones
     * in {@link JoinToMultiJoinRule} and have to reassign them from top-level hints.
     */
    private static void restoreJoinTypeHints(RelNode root) {
        RelShuttle visitor = new RelHomogeneousShuttle() {
            /** Hints to assign on current tree level. */
            private final Deque<List<RelHint>> hintsStack = new ArrayDeque<>();

            /** Current hint inheritance path. It is important for hint priority. */
            private final List<Integer> inputsStack = new ArrayList<>();

            /** {@inheritDoc} */
            @Override public RelNode visit(RelNode rel) {
                // Leaf scans has no inputs. And we are interrested only in Joins.
                if (rel.getInputs().isEmpty())
                    return rel;

                List<RelHint> curHints = Collections.emptyList();

                if ((rel instanceof Hintable) && !(rel instanceof Join) && !((Hintable)rel).getHints().isEmpty()) {
                    for (RelHint hint : ((Hintable)rel).getHints()) {
                        // Reassign only top-level hints (without the inherit path).
                        if (!hint.inheritPath.isEmpty())
                            continue;

                        if (curHints == Collections.EMPTY_LIST)
                            curHints = new ArrayList<>();

                        curHints.add(hint);
                    }
                }

                // We may find additional top-level hints in a subquery. From this point, we need to combine them.
                if (!stack.isEmpty()) {
                    List<RelHint> prevHints = hintsStack.peekLast();

                    if (!curHints.isEmpty() && !prevHints.isEmpty())
                        curHints.addAll(prevHints);
                    else if (curHints.isEmpty())
                        curHints = prevHints;

                    assert curHints.size() >= hintsStack.peekLast().size();
                }

                hintsStack.add(curHints);

                RelNode res = super.visit(rel);

                hintsStack.removeLast();

                return res;
            }

            /** {@inheritDoc} */
            @Override protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                inputsStack.add(i);

                if (child instanceof Join && !hintsStack.isEmpty()) {
                    List<RelHint> curHints = hintsStack.peekLast();

                    if (!curHints.isEmpty()) {
                        assert Commons.context(child).config().getSqlToRelConverterConfig().getHintStrategyTable()
                            .apply(curHints, child).size() == curHints.size() : "Not all hints are applicable.";

                        curHints = curHints.stream().map(h -> h.copy(inputsStack)).collect(Collectors.toList());

                        // Join is a Hintable.
                        child = ((Hintable)child).attachHints(curHints);

                        parent.replaceInput(i, child);
                    }
                }

                RelNode res = super.visitChild(parent, i, child);

                inputsStack.remove(inputsStack.size() - 1);

                return res;
            }
        };

        root.accept(visitor);
    }

    /**
     * Add external options as hints to {@code root.rel}.
     *
     * @return New or old root node.
     */
    private static RelRoot addExternalOptions(RelRoot root) {
        if (!Commons.context(root.rel).isForcedJoinOrder())
            return root;

        if (!(root.rel instanceof Hintable)) {
            Commons.context(root.rel).logger().warning("Unable to set hint " + HintDefinition.ENFORCE_JOIN_ORDER
                + " passed as an external parameter to the root relation operator ["
                + RelOptUtil.toString(HintUtils.noInputsRelWrap(root.rel)).trim()
                + "] because it is not a Hintable.");

            return root;
        }

        List<RelHint> newHints = Stream.concat(HintUtils.allRelHints(root.rel).stream(),
            Stream.of(RelHint.builder(HintDefinition.ENFORCE_JOIN_ORDER.name()).build())).collect(Collectors.toList());

        root = root.withRel(((Hintable)root.rel).withHints(newHints));

        RelOptUtil.propagateRelHints(root.rel, false);

        return root;
    }

    /**
     * Extracts planner-level hints like 'DISABLE_RULE' if the root node is a combining node like 'UNION'.
     */
    private static Collection<RelHint> extractRootHints(RelNode rel) {
        if (!HintUtils.allRelHints(rel).isEmpty())
            return HintUtils.allRelHints(rel);

        if (rel instanceof SetOp) {
            return F.flatCollections(rel.getInputs().stream()
                .map(PlannerHelper::extractRootHints).collect(Collectors.toList()));
        }

        return Collections.emptyList();
    }

    /**
     * This shuttle analyzes a relational tree and inserts an eager spool node
     * just under the TableModify node in case latter depends upon a table used
     * to query the data for modify node to avoid the double processing
     * of the retrieved rows.
     * <p/>
     * It considers two cases: <ol>
     *     <li>
     *         Modify node produces rows to insert, then a spool is required.
     *     </li>
     *     <li>
     *         Modify node updates rows only, then a spool is required if 1) we
     *         are scaning an index and 2) any of the indexed column is updated
     *         by modify node.
     *     </li>
     * <ol/>
     *
     */
    private static class FixDependentModifyNodeShuttle extends IgniteRelShuttle {
        /**
         * Flag indicates whether we should insert a spool or not.
         */
        private boolean spoolNeeded;

        /** Current modify node. */
        private IgniteTableModify modifyNode;

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteTableModify rel) {
            assert modifyNode == null;

            modifyNode = rel;

            if (rel.isDelete())
                return rel;

            if (rel.isMerge()) // MERGE operator always contains modified table as a source.
                spoolNeeded = true;
            else
                processNode(rel);

            if (spoolNeeded) {
                IgniteTableSpool spool = new IgniteTableSpool(
                    rel.getCluster(),
                    rel.getInput().getTraitSet(),
                    Spool.Type.EAGER,
                    rel.getInput()
                );

                rel.replaceInput(0, spool);
            }

            return rel;
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteTableScan rel) {
            return processScan(rel);
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteIndexScan rel) {
            return processScan(rel);
        }

        /** {@inheritDoc} */
        @Override protected IgniteRel processNode(IgniteRel rel) {
            List<IgniteRel> inputs = Commons.cast(rel.getInputs());

            for (int i = 0; i < inputs.size(); i++) {
                if (spoolNeeded)
                    break;

                visitChild(rel, i, inputs.get(i));
            }

            return rel;
        }

        /**
         * Process a scan node and raise a {@link #spoolNeeded flag} if needed.
         *
         * @param scan TableScan to analyze.
         * @return The input rel.
         */
        private IgniteRel processScan(TableScan scan) {
            IgniteTable tbl = modifyNode != null ? modifyNode.getTable().unwrap(IgniteTable.class) : null;

            if (tbl == null || scan.getTable().unwrap(IgniteTable.class) != tbl)
                return (IgniteRel)scan;

            if (modifyNodeInsertsData()) {
                spoolNeeded = true;

                return (IgniteRel)scan;
            }

            // for update-only node the spool needed if any of the updated
            // column is part of the index we are going to scan
            if (scan instanceof IgniteTableScan)
                return (IgniteRel)scan;

            ImmutableSet<Integer> indexedCols = ImmutableSet.copyOf(
                tbl.getIndex(((AbstractIndexScan)scan).indexName()).collation().getKeys());

            spoolNeeded = modifyNode.getUpdateColumnList().stream()
                .map(tbl.descriptor()::columnDescriptor)
                .map(ColumnDescriptor::fieldIndex)
                .anyMatch(indexedCols::contains);

            return (IgniteRel)scan;
        }

        /**
         * @return {@code true} in case {@link #modifyNode} produces any insert.
         */
        private boolean modifyNodeInsertsData() {
            return modifyNode.isInsert();
        }
    }

    /**
     * Searches tree {@code root} for nodes of {@code nodeType}.
     *
     * @return Nodes matching {@code nodeType}. An empty list if none matches. A single value list if a node
     * found and {@code stopOnFirst} is {@code true}.
     */
    public static <T extends RelNode> List<T> findNodes(RelNode root, Class<T> nodeType, boolean stopOnFirst) {
        List<T> rels = new ArrayList<>();

        try {
            RelShuttle visitor = new RelHomogeneousShuttle() {
                @Override public RelNode visit(RelNode node) {
                    if (nodeType.isAssignableFrom(node.getClass())) {
                        rels.add((T)node);

                        if (stopOnFirst)
                            throw Util.FoundOne.NULL;
                    }

                    return super.visit(node);
                }
            };

            root.accept(visitor);
        }
        catch (Util.FoundOne ignored) {
            // No-op.
        }

        return rels;
    }
}
