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
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.util.typedef.F;

/** */
class Cloner implements IgniteRelVisitor<IgniteRel> {
    /** */
    private final RelOptCluster cluster;

    /** */
    private final Prepare.CatalogReader catalogReader;

    /** */
    private List<Fragment> fragments;

    /**
     * @param ctx Planner context.
     */
    Cloner(PlanningContext ctx) {
        cluster = ctx.createCluster();
        catalogReader = ctx.catalogReader();
    }

    /**
     * Clones and associates a plan with a new cluster.
     *
     * @param src Plan to clone.
     * @return New plan.
     */
    List<Fragment> go(List<Fragment> src) {
        assert !F.isEmpty(src);

        fragments = new ArrayList<>(src.size());

        Fragment first = F.first(src);

        fragments.add(new Fragment(first.fragmentId(), visit(first.root())));

        assert fragments.size() == src.size();

        Collections.reverse(fragments);

        return fragments;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSender rel) {
        IgniteRel input = visit((IgniteRel) rel.getInput());

        return new IgniteSender(cluster, rel.getTraitSet(), input);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteFilter(cluster, rel.getTraitSet(), input, rel.getCondition());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteHashFilter rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteHashFilter(cluster, rel.getTraitSet(), input);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteProject(cluster, rel.getTraitSet(), input, rel.getProjects(), rel.getRowType());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        RelNode input = visit((IgniteRel) rel.getInput());

        return new IgniteTableModify(cluster, rel.getTraitSet(), rel.getTable(), catalogReader, input,
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
        return new IgniteTableScan(cluster, rel.getTraitSet(), rel.getTable());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteValues rel) {
        return new IgniteValues(cluster, rel.getRowType(), rel.getTuples(), rel.getTraitSet());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        Fragment fragment = rel.source();
        fragments.add(fragment = new Fragment(fragment.fragmentId(), visit(fragment.root())));

        return new IgniteReceiver(cluster, rel.getTraitSet(), rel.getRowType(), fragment);
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
