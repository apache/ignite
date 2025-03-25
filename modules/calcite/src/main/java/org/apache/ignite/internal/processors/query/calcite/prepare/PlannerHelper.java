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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
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

            rel = planner.trimUnusedFields(root.withRel(rel)).rel;

            rel = planner.transform(PlannerPhase.HEP_FILTER_PUSH_DOWN, rel.getTraitSet(), rel);

            rel = planner.transform(PlannerPhase.HEP_PROJECT_PUSH_DOWN, rel.getTraitSet(), rel);

            optimizeJoins(planner, rel);

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

    /** */
    private static void optimizeJoins(IgnitePlanner planner, RelNode rel) {
        int joinsCnt = joinsCount(rel);

        if (joinsCnt > MAX_JOINS_TO_COMMUTE)
            planner.addDisabledRules(Collections.singletonList(CoreRules.JOIN_COMMUTE.toString()));

        if (joinsCnt > MAX_JOINS_TO_COMMUTE_INPUTS) {
            planner.addDisabledRules(Arrays.asList(JoinPushThroughJoinRule.LEFT.toString(),
                JoinPushThroughJoinRule.RIGHT.toString()));
        }
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

    /** */
    private static int joinsCount(RelNode root) {
        AtomicInteger cnt = new AtomicInteger();

        JoinSizeFinder jCnt = new JoinSizeFinder();

        jCnt.visit(root);

        int g = jCnt.sizeOfBiggestJoin();

        RelShuttle visitor = new RelHomogeneousShuttle() {
            @Override public RelNode visit(RelNode node) {
                if (node instanceof Join || node instanceof Correlate)
                    cnt.incrementAndGet();

                return super.visit(node);
            }
        };

        root.accept(visitor);

        return cnt.get();
    }

    /**
     * A shuttle to estimate a biggest join to optimize.
     *
     * <p>There are only two rules: <ol>
     *     <li>Each achievable leaf node contribute to join complexity, thus must be counted</li>
     *     <li>If this shuttle reach the {@link LogicalCorrelate} node, only left shoulder will be
     *         analysed by this shuttle. For right shoulder a new shuttle will be created, and maximum
     *         of previously found subquery and current one will be saved</li>
     * </ol>
     */
    private static class JoinSizeFinder extends RelHomogeneousShuttle {
        private int countOfSources = 0;
        private int maxCountOfSourcesInSubQuery = 0;

        /** {@inheritDoc} */
        @Override
        public RelNode visit(RelNode other) {
            if (other.getInputs().isEmpty()) {
                countOfSources++;

                return other;
            }

            return super.visit(other);
        }

        /** {@inheritDoc} */
        @Override public RelNode visit(LogicalCorrelate correlate) {
            JoinSizeFinder inSubquerySizeFinder = new JoinSizeFinder();
            inSubquerySizeFinder.visit(correlate.getInput(1));

            maxCountOfSourcesInSubQuery = Math.max(
                maxCountOfSourcesInSubQuery,
                inSubquerySizeFinder.sizeOfBiggestJoin()
            );

            return visitChild(correlate, 0, correlate.getInput(0));
        }

        int sizeOfBiggestJoin() {
            return Math.max(countOfSources, maxCountOfSourcesInSubQuery);
        }
    }
}
