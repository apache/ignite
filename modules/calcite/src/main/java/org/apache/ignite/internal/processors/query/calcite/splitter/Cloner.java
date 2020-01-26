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

package org.apache.ignite.internal.processors.query.calcite.splitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/** */
class Cloner implements IgniteRelVisitor<IgniteRel> {
    /** */
    private final RelOptCluster cluster;

    /** */
    private List<Fragment> fragments;

    /**
     * @param cluster New cluster.
     */
    Cloner(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Clones and associates a plan with a new cluster.
     *
     * @param plan Plan to clone.
     * @return New plan.
     */
    QueryPlan go(QueryPlan plan) {
        List<Fragment> planFragments = plan.fragments();

        assert !F.isEmpty(planFragments);

        fragments = new ArrayList<>(planFragments.size());

        Fragment first = F.first(planFragments);

        fragments.add(new Fragment(first.fragmentId(), visit(first.root())));

        assert fragments.size() == planFragments.size();

        Collections.reverse(fragments);

        return new QueryPlan(fragments);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSender rel) {
        IgniteRel input = visit(rel.getInput());

        return new IgniteSender(cluster, rel.getTraitSet(), input);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        RelNode input = visit(rel.getInput());

        return new IgniteFilter(cluster, rel.getTraitSet(), input, rel.getCondition());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        RelNode input = visit(rel.getInput());

        return new IgniteProject(cluster, rel.getTraitSet(), input, rel.getProjects(), rel.getRowType());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteJoin rel) {
        RelNode left = visit(rel.getLeft());
        RelNode right = visit(rel.getRight());

        return new IgniteJoin(cluster, rel.getTraitSet(), left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return new IgniteTableScan(cluster, rel.getTraitSet(), rel.getTable());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        Fragment fragment = (Fragment) rel.source();
        fragments.add(fragment = new Fragment(fragment.fragmentId(), visit(fragment.root())));

        return new IgniteReceiver(cluster, rel.getTraitSet(), rel.getRowType(), fragment);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        RelNode input = visit(rel.getInput());

        return new IgniteExchange(cluster, rel.getTraitSet(), input, rel.getDistribution());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** */
    private IgniteRel visit(RelNode rel) {
        return visit(Commons.igniteRel(rel));
    }
}
