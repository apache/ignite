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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
import org.apache.ignite.internal.processors.query.calcite.rule.logical.IgniteJoinsOrderOptimizationRule;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class PlannerHelper {
    /** */
    private static final Collection<String> HIINT_OF_JOIN_TYPE = Stream.of(
        HintDefinition.NL_JOIN, HintDefinition.NO_NL_JOIN,
        HintDefinition.CNL_JOIN, HintDefinition.NO_CNL_JOIN,
        HintDefinition.MERGE_JOIN, HintDefinition.NO_MERGE_JOIN
    ).map(Enum::name).collect(Collectors.toSet());

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

            planner.setDisabledRules(HintUtils.options(root.rel, extractRootHints(root.rel), HintDefinition.DISABLE_RULE));

            RelNode rel = root.rel;

            // Transformation chain
            rel = planner.transform(PlannerPhase.HEP_DECORRELATE, rel.getTraitSet(), rel);

            rel = planner.replaceCorrelatesCollisions(rel);

            rel = planner.trimUnusedFields(root.withRel(rel)).rel;

            rel = planner.transform(PlannerPhase.HEP_FILTER_PUSH_DOWN, rel.getTraitSet(), rel);

            List<RelHint> topHints = HintUtils.allRelHints(rel);

            rel = planner.transform(PlannerPhase.HEP_PROJECT_PUSH_DOWN, rel.getTraitSet(), rel);

            assert HintUtils.allRelHints(rel).isEmpty() || Objects.equals(topHints, HintUtils.allRelHints(rel));

            if (HintUtils.allRelHints(rel).isEmpty() && !topHints.isEmpty()) {
                assert rel instanceof Hintable;

                rel = ((Hintable)rel).withHints(topHints);
            }

            // RelOptUtil#propagateRelHints(RelNode, equiv) may skip hints because current RelNode has no hints.
            // Or if hints reside in a child nodes which are not inputs of the current node. Like LogicalFlter#condition.
            // Such hints may appear or be required below in the tree, after rules applying.
            // Also, pushed down LogicalProject may apper wiout the original hints required after the following join optimization.
            rel = RelOptUtil.propagateRelHints(rel, false);

            rel = optimizeJoinsOrder(planner, rel);

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

            return igniteRel;
        }
        catch (Throwable ex) {
            log.error("Unexpected error at query optimizer.", ex);
            log.error(planner.dump());

            throw ex;
        }
    }

    /**
     * Tries to optimize Joins order.
     *
     * @see JoinToMultiJoinRule
     * @see IgniteJoinsOrderOptimizationRule
     *
     * @return An optimized node or original {@code root} if didn't optimize.
     */
    private static RelNode optimizeJoinsOrder(IgnitePlanner planner, RelNode root) {
        List<Join> joins = findNodes(root, Join.class, false);

        // No original Joins found, nothing to optimize.
        if (joins.isEmpty())
            return root;

        int disabledCnt = 0;

        // If all the joins are with the forced order, no need to do join order heuristics at all.
        for (Join join : joins) {
            for (RelHint hint : join.getHints()) {
                if (HintDefinition.ENFORCE_JOIN_ORDER.name().equals(hint.hintName)) {
                    ++disabledCnt;

                    break;
                }
            }
        }

        if (disabledCnt == joins.size())
            return root;

        RelNode optimizedRel = planner.transform(PlannerPhase.HEP_OPTIMIZE_JOIN_ORDER, root.getTraitSet(), root);

        // Unable to optimize.
        if (!findNodes(optimizedRel, MultiJoin.class, true).isEmpty())
            return root;

        // If Joins order commuted, no need to launch additional join order optimizations.
        planner.setDisabledRules(HintDefinition.ENFORCE_JOIN_ORDER.disabledRules().stream().map(RelOptRule::toString)
            .collect(Collectors.toList()));

        restoreJoinTypeHints(optimizedRel);

        return optimizedRel;
    }

    /**
     * A join type hint might be assigned to SELECT or to a table. Originally, SELECT-level hints are propagated and assigned
     * to following Joins and TableScans. We lose assigned to Join nodes ones in {@link JoinToMultiJoinRule}. Thereby we
     * have to reassign original top-level hints.
     */
    private static void restoreJoinTypeHints(RelNode root) {
        RelShuttle visitor = new RelHomogeneousShuttle() {
            /** Hints to assign on current tree level. */
            private final List<List<RelHint>> hintsStack = new ArrayList<>();

            /** Current hint inheritance path. Hint inheritance is important for hint priority. */
            private final List<Integer> inputsStack = new ArrayList<>();

            /** {@inheritDoc} */
            @Override public RelNode visit(RelNode rel) {
                if (rel.getInputs().isEmpty())
                    return rel;

                List<RelHint> curHints = Collections.emptyList();

                if (rel instanceof Hintable && !(rel instanceof Join) && !((Hintable)rel).getHints().isEmpty()) {
                    for (RelHint hint : ((Hintable)rel).getHints()) {
                        if (!hint.inheritPath.isEmpty() || !HIINT_OF_JOIN_TYPE.contains(hint.hintName))
                            continue;

                        if (curHints == Collections.EMPTY_LIST)
                            curHints = new ArrayList<>();

                        curHints.add(hint);
                    }
                }

                if (!stack.isEmpty()) {
                    List<RelHint> prevHints = hintsStack.get(hintsStack.size() - 1);

                    if (!curHints.isEmpty() && !prevHints.isEmpty()) {
                        prevHints.addAll(curHints);

                        curHints = prevHints;
                    }
                    else if (curHints.isEmpty())
                        curHints = prevHints;

                    assert curHints.size() >= hintsStack.get(hintsStack.size() - 1).size();
                }

                hintsStack.add(curHints);

                RelNode res = super.visit(rel);

                hintsStack.remove(hintsStack.size() - 1);

                return res;
            }

            /** {@inheritDoc} */
            @Override protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                inputsStack.add(i);

                if (child instanceof Join && !hintsStack.isEmpty()) {
                    List<RelHint> curHints = hintsStack.get(hintsStack.size() - 1);

                    if (!curHints.isEmpty()) {
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
     * @return Nodes of type {@code nodeType}. Empty list if not found. Single value list if a node found and
     * {@code stopOnFirst} is {@code true}.
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
