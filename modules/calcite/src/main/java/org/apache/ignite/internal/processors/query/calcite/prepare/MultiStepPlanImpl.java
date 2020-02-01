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
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Distributed query plan.
 */
public class MultiStepPlanImpl implements MultiStepPlan {
    /** */
    private final List<Fragment> fragments;

    /** */
    private final List<GridQueryFieldMetadata> fieldsMeta;

    /**
     * @param fragments Query fragments.
     */
    public MultiStepPlanImpl(List<Fragment> fragments) {
        this(fragments, ImmutableList.of());
    }

    /**
     * @param fragments Query fragments.
     * @param fieldsMeta Fields metadata.
     */
    public MultiStepPlanImpl(List<Fragment> fragments, List<GridQueryFieldMetadata> fieldsMeta) {
        this.fieldsMeta = fieldsMeta;
        this.fragments = fragments;
    }

    /** {@inheritDoc} */
    @Override public List<Fragment> fragments() {
        return fragments;
    }

    /** {@inheritDoc} */
    @Override public List<GridQueryFieldMetadata> fieldsMetadata() {
        return fieldsMeta;
    }

    /** {@inheritDoc} */
    @Override public void init(MappingService mappingService, PlanningContext ctx) {
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

    /** {@inheritDoc} */
    @Override public MultiStepPlan clone(RelOptCluster cluster) {
        return new Cloner(cluster).go(this);
    }
}
