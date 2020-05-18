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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregate;
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
import org.apache.ignite.internal.util.typedef.F;

/** */
class Cloner implements IgniteRelVisitor<IgniteRel> {
    /** */
    private final RelOptCluster cluster;

    /** */
    private FragmentProto curr;

    Cloner(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Clones and associates a plan with a new cluster.
     *
     * @param src Plan to clone.
     * @return New plan.
     */
    List<Fragment> go(List<Fragment> src) {
        assert !F.isEmpty(src);

        List<Fragment> fragments = new ArrayList<>(src.size());

        for (Fragment fragment : src) {
            curr = new FragmentProto(fragment.fragmentId(), fragment.root());
            curr.root = visit(curr.root);
            fragments.add(curr.build());
            curr = null;
        }

        return fragments;
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
    @Override public IgniteRel visit(IgniteJoin rel) {
        RelNode left = visit((IgniteRel) rel.getLeft());
        RelNode right = visit((IgniteRel) rel.getRight());

        return new IgniteJoin(cluster, rel.getTraitSet(), left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return new IgniteTableScan(cluster, rel.getTraitSet(), rel.getTable(), rel.indexName(), rel.condition());
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
    @Override public IgniteRel visit(IgniteReceiver rel) {
        IgniteReceiver receiver = new IgniteReceiver(cluster, rel.getTraitSet(), rel.getRowType(),
            rel.exchangeId(), rel.sourceFragmentId());

        curr.remotes.add(receiver);

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

    /** */
    private static class FragmentProto {
        /** */
        private final long id;

        /** */
        private IgniteRel root;

        /** */
        private final ImmutableList.Builder<IgniteReceiver> remotes = ImmutableList.builder();

        /** */
        private FragmentProto(long id, IgniteRel root) {
            this.id = id;
            this.root = root;
        }

        Fragment build() {
            return new Fragment(id, root, remotes.build());
        }
    }
}
