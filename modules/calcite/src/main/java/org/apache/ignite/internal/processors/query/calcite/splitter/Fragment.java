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

import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.exchange.Receiver;
import org.apache.ignite.internal.processors.query.calcite.exchange.Sender;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class Fragment {
    public final RelNode root;

    public SourceDistribution distribution;

    public Fragment(RelNode root) {
        this.root = root;
    }

    public void init(Context ctx) {
        RelMetadataQueryEx mq = RelMetadataQueryEx.instance();

        distribution = mq.getSourceDistribution(root);

        PartitionsDistribution mapping = distribution.partitionMapping;

        if (mapping == null)
            distribution.partitionMapping = isRootFragment() ? registry(ctx).single() : registry(ctx).random(topologyVersion(ctx));
        else if (mapping.excessive)
            distribution.partitionMapping = mapping.deduplicate();
        
        if (!F.isEmpty(distribution.remoteInputs)) {
            for (Receiver input : distribution.remoteInputs)
                input.init(distribution, mq);
        }
    }

    private boolean isRootFragment() {
        return !(root instanceof Sender);
    }

    private PartitionsDistributionRegistry registry(Context ctx) {
        return ctx.unwrap(PartitionsDistributionRegistry.class);
    }

    private AffinityTopologyVersion topologyVersion(Context ctx) {
        return ctx.unwrap(AffinityTopologyVersion.class);
    }
}
