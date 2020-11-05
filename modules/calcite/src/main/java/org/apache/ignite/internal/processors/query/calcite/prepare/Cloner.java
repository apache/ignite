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

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
class Cloner implements IgniteRelVisitor<IgniteRel> {
    /** */
    private final RelOptCluster cluster;

    /** */
    private ImmutableList.Builder<IgniteReceiver> remotes;

    Cloner(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Clones and associates a plan with a new cluster.
     *
     * @param src Fragments to clone.
     * @return New plan.
     */
    public List<Fragment> go(List<Fragment> src) {
        return Commons.transform(src, this::go);
    }

    /**
     * Clones and associates a plan with a new cluster.
     *
     * @param src Fragment to clone.
     * @return New plan.
     */
    public Fragment go(Fragment src) {
        try {
            remotes = ImmutableList.builder();

            IgniteRel newRoot = visit(src.root());
            ImmutableList<IgniteReceiver> remotes = this.remotes.build();

            return new Fragment(src.fragmentId(), newRoot, remotes, src.serialized());
        }
        finally {
            remotes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSender rel) {
        IgniteRel input = visit((IgniteRel) rel.getInput());

        return new IgniteSender(cluster, rel.getTraitSet(), input, rel.exchangeId(), rel.targetFragmentId(), rel.distribution());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteFilter(cluster, rel.getTraitSet(), input, rel.getCondition());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTrimExchange rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteTrimExchange(cluster, rel.getTraitSet(), input, rel.distribution());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteProject(cluster, rel.getTraitSet(), input, rel.getProjects(), rel.getRowType());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteTableModify(cluster, rel.getTraitSet(), rel.getTable(), input,
            rel.getOperation(), rel.getUpdateColumnList(), rel.getSourceExpressionList(), rel.isFlattened());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteNestedLoopJoin rel) {
        RelNode left = visit((IgniteRel) rel.getLeft());
        RelNode right = visit((IgniteRel) rel.getRight());

        return new IgniteNestedLoopJoin(cluster, rel.getTraitSet(), left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteCorrelatedNestedLoopJoin rel) {
        RelNode left = visit((IgniteRel) rel.getLeft());
        RelNode right = visit((IgniteRel) rel.getRight());

        return new IgniteCorrelatedNestedLoopJoin(cluster, rel.getTraitSet(), left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteIndexScan rel) {
        return new IgniteIndexScan(cluster, rel.getTraitSet(), rel.getTable(), rel.indexName(), rel.projects(),
            rel.condition(), rel.lowerCondition(), rel.upperCondition(), rel.requiredColunms());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return new IgniteTableScan(cluster, rel.getTraitSet(), rel.getTable(), rel.projects(), rel.condition(),
            rel.requiredColunms());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteValues rel) {
        return new IgniteValues(cluster, rel.getRowType(), rel.getTuples(), rel.getTraitSet());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteUnionAll rel) {
        List<RelNode> inputs = Commons.transform(rel.getInputs(), rel0 -> visit((IgniteRel) rel0));

        return new IgniteUnionAll(cluster, rel.getTraitSet(), inputs);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSort rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteSort(cluster, rel.getTraitSet(), input, rel.collation, rel.offset, rel.fetch);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableSpool rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteTableSpool(cluster, rel.getTraitSet(), input);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        IgniteReceiver receiver = new IgniteReceiver(cluster, rel.getTraitSet(), rel.getRowType(),
            rel.exchangeId(), rel.sourceFragmentId());

        if (remotes != null)
            remotes.add(receiver);

        return receiver;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteExchange(cluster, rel.getTraitSet(), input, rel.getDistribution());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteAggregate rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteAggregate(cluster, rel.getTraitSet(), input,
            rel.getGroupSet(), rel.getGroupSets(), rel.getAggCallList());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMapAggregate rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteMapAggregate(cluster, rel.getTraitSet(), input,
            rel.getGroupSet(), rel.getGroupSets(), rel.getAggCallList());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReduceAggregate rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteReduceAggregate(cluster, rel.getTraitSet(), input,
            rel.groupSet(), rel.groupSets(), rel.aggregateCalls(), rel.getRowType());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteRel rel) {
        return rel.accept(this);
    }
}
