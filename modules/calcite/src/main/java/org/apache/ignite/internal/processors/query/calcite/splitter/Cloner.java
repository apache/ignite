/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.splitter;

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
import org.apache.ignite.internal.processors.query.calcite.rel.RelOp;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class Cloner implements RelOp<IgniteRel, IgniteRel> {
    /** */
    private final RelOptCluster cluster;

    /** */
    private class VisitorImpl implements IgniteRelVisitor<IgniteRel> {
        @Override public IgniteRel visit(IgniteSender rel) {
            RelNode input = visit(Commons.igniteRel(rel.getInput()));

            return new IgniteSender(cluster, rel.getTraitSet(), input, rel.target());
        }

        @Override public IgniteRel visit(IgniteFilter rel) {
            RelNode input = visit(Commons.igniteRel(rel.getInput()));

            return new IgniteFilter(cluster, rel.getTraitSet(), input, rel.getCondition());
        }

        @Override public IgniteRel visit(IgniteProject rel) {
            RelNode input = visit(Commons.igniteRel(rel.getInput()));

            return new IgniteProject(cluster, rel.getTraitSet(), input, rel.getProjects(), rel.getRowType());
        }

        @Override public IgniteRel visit(IgniteJoin rel) {
            RelNode left = visit(Commons.igniteRel(rel.getLeft()));
            RelNode right = visit(Commons.igniteRel(rel.getRight()));

            return new IgniteJoin(cluster, rel.getTraitSet(), left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
        }

        @Override public IgniteRel visit(IgniteTableScan rel) {
            return new IgniteTableScan(cluster, rel.getTraitSet(), rel.getTable());
        }

        @Override public IgniteRel visit(IgniteReceiver rel) {
            return new IgniteReceiver(cluster, rel.getTraitSet(), rel.getRowType(), rel.source());
        }

        @Override public IgniteRel visit(IgniteExchange rel) {
            RelNode input = visit(Commons.igniteRel(rel.getInput()));

            return new IgniteExchange(cluster, rel.getTraitSet(), input, rel.getDistribution());
        }

        @Override public IgniteRel visit(IgniteRel rel) {
            return rel.accept(this);
        }
    }

    /**
     * @param cluster New cluster.
     */
    public Cloner(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel go(IgniteRel rel) {
        return new VisitorImpl().visit(rel);
    }
}
