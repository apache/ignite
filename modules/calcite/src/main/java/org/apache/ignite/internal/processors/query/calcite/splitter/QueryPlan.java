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
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
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

/**
 * Distributed query plan.
 */
public class QueryPlan {
    /** */
    private final List<Fragment> fragments;

    /**
     * @param fragments Query fragments.
     */
    public QueryPlan(List<Fragment> fragments) {
        this.fragments = fragments;
    }

    /**
     * Inits query fragments.
     *
     * @param mappingService Mapping service.
     * @param ctx Planner context.
     */
    public void init(MappingService mappingService, IgniteCalciteContext ctx) {
        int i = 0;

        RelMetadataQueryEx mq = RelMetadataQueryEx.instance();

        while (true) {
            try {
                F.first(fragments).init(mappingService, ctx, mq);

                break;
            }
            catch (OptimisticPlanningException e) {
                if (++i > 3)
                    throw new IgniteSQLException("Failed to map query.", e);

                Edge edge = e.edge();

                RelNode parent = edge.parent();
                RelNode child = edge.child();

                RelOptCluster cluster = child.getCluster();
                RelTraitSet traitSet = child.getTraitSet();

                Fragment fragment = new Fragment(new IgniteSender(cluster, traitSet, child));

                fragments.add(fragment);

                if (parent != null)
                    parent.replaceInput(edge.childIndex(), new IgniteReceiver(cluster, traitSet, child.getRowType(), fragment));
                else {
                    // need to fix a distribution of a root of a fragment
                    int idx = 0;

                    for (; idx < fragments.size(); idx++) {
                        if (fragments.get(idx).root() == child)
                            break;
                    }

                    assert idx < fragments.size();

                    fragments.set(idx, new Fragment(new IgniteReceiver(cluster, traitSet, child.getRowType(), fragment)));
                }
            }
        }
    }

    /**
     * @return Query fragments.
     */
    public List<Fragment> fragments() {
        return fragments;
    }

    /**
     * Creates a template from the plan.
     *
     * @return Template.
     */
    public QueryPlan template() {
        return clone(Commons.EMPTY_CLUSTER);
    }

    /**
     * @param template Template.
     * @param ctx Context.
     * @return New plan, linked to the given context.
     */
    public static QueryPlan fromTemplate(QueryPlan template, IgniteCalciteContext ctx) {
        return template.clone(ctx.createCluster());
    }

    /**
     * Clones this plan with a new cluster.
     */
    private QueryPlan clone(RelOptCluster cluster) {
        RelOptPlanner cur = cluster.getPlanner();
        RelOptPlanner other = F.first(fragments).root().getCluster().getPlanner();

        if (cur == other)
            return this;

        VisitorImpl visitor = new VisitorImpl(cluster);
        visitor.visit(F.first(fragments));

        return new QueryPlan(visitor.fragments);
    }

    /** */
    private class VisitorImpl implements IgniteRelVisitor<IgniteRel> {
        /** */
        private final RelOptCluster cluster;

        /** */
        private final List<Fragment> fragments;

        private VisitorImpl(RelOptCluster cluster) {
            this.cluster = cluster;

            fragments = new ArrayList<>(QueryPlan.this.fragments.size());
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteSender rel) {
            return new IgniteSender(cluster, rel.getTraitSet(), visit(Commons.igniteRel(rel.getInput())));
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteFilter rel) {
            RelNode input = visit(Commons.igniteRel(rel.getInput()));

            return new IgniteFilter(cluster, rel.getTraitSet(), input, rel.getCondition());
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteProject rel) {
            RelNode input = visit(Commons.igniteRel(rel.getInput()));

            return new IgniteProject(cluster, rel.getTraitSet(), input, rel.getProjects(), rel.getRowType());
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteJoin rel) {
            RelNode left = visit(Commons.igniteRel(rel.getLeft()));
            RelNode right = visit(Commons.igniteRel(rel.getRight()));

            return new IgniteJoin(cluster, rel.getTraitSet(), left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteTableScan rel) {
            return new IgniteTableScan(cluster, rel.getTraitSet(), rel.getTable());
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteReceiver rel) {
            return new IgniteReceiver(cluster, rel.getTraitSet(), rel.getRowType(), visit((Fragment) rel.source()));
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteExchange rel) {
            RelNode input = visit(Commons.igniteRel(rel.getInput()));

            return new IgniteExchange(cluster, rel.getTraitSet(), input, rel.getDistribution());
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteRel rel) {
            return rel.accept(this);
        }

        /** */
        private Fragment visit(Fragment src) {
            Fragment res = new Fragment(src.fragmentId(), visit(Commons.igniteRel(src.root())));

            fragments.add(0, res);

            return res;
        }
    }
}
