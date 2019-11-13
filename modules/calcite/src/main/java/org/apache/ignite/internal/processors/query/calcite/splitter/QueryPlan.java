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

import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.util.Edge;

/**
 *
 */
public class QueryPlan {
    private final List<Fragment> fragments;

    public QueryPlan(List<Fragment> fragments) {
        this.fragments = fragments;
    }

    public void init(Context ctx) {
        int i = 0;

        RelMetadataQueryEx mq = RelMetadataQueryEx.instance();

        while (true) {
            try {
                for (Fragment fragment : fragments)
                    fragment.init(ctx, mq);

                break;
            }
            catch (OptimisticPlanningException e) {
                if (++i > 3)
                    throw new IgniteSQLException("Failed to map query.", e);

                for (Fragment fragment0 : fragments)
                    fragment0.reset();

                Edge edge = e.edge();

                RelNode parent = edge.parent();
                RelNode child = edge.child();

                RelOptCluster cluster = child.getCluster();
                RelTraitSet traitSet = child.getTraitSet();

                Sender sender = new Sender(cluster, traitSet, child);
                parent.replaceInput(edge.childIdx(), new Receiver(cluster, traitSet, sender));

                fragments.add(new Fragment(sender));
            }
        }
    }

    public List<Fragment> fragments() {
        return fragments;
    }
}
