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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationRegistry;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class Fragment {
    public final RelNode rel;

    private NodesMapping mapping;
    private ImmutableIntList localInputs;
    private ImmutableList<Fragment> remoteInputs;

    public Fragment(RelNode rel) {
        this.rel = rel;
    }

    public void init(Context ctx, RelMetadataQuery mq) {
        init(null, ctx, mq);
    }

    public void init(Fragment parent, Context ctx, RelMetadataQuery mq) {
        FragmentInfo info = IgniteMdFragmentInfo.fragmentInfo(rel, mq);

        remoteInputs = info.remoteInputs();
        localInputs = info.localInputs();

        if (info.mapping() == null)
            mapping = remote() ? registry(ctx).random(topologyVersion(ctx)) : registry(ctx).local();
        else {
            try {
                mapping = info.mapping().deduplicate();
            }
            catch (LocationMappingException e) {
                throw new IgniteSQLException("Failed to map fragment to location, partition lost.", e);
            }
        }

        if (parent != null) {
            assert remote();

            ((Sender)rel).init(parent.mapping);
        }

        if (!F.isEmpty(remoteInputs)) {
            for (Fragment input : remoteInputs)
                input.init(this, ctx, mq);
        }
    }

    private boolean remote() {
        return rel instanceof Sender;
    }

    private LocationRegistry registry(Context ctx) {
        return ctx.unwrap(LocationRegistry.class);
    }

    private AffinityTopologyVersion topologyVersion(Context ctx) {
        return ctx.unwrap(AffinityTopologyVersion.class);
    }
}
