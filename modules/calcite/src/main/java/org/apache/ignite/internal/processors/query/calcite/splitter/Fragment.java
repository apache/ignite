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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationRegistry;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class Fragment implements Source {
    private static final AtomicLong ID_GEN = new AtomicLong();

    private final long exchangeId = ID_GEN.getAndIncrement();

    private final RelNode root;

    private NodesMapping mapping;

    public Fragment(RelNode root) {
        this.root = root;
    }

    public void init(Context ctx, RelMetadataQuery mq) {
        FragmentInfo info = IgniteMdFragmentInfo.fragmentInfo(root, mq);

        if (info.mapping() == null)
            mapping = remote() ? registry(ctx).random(topologyVersion(ctx)) : registry(ctx).local();
        else
            mapping = info.mapping().deduplicate();

        ImmutableList<Pair<Receiver, Source>> sources = info.sources();

        if (!F.isEmpty(sources)) {
            for (Pair<Receiver, Source> input : sources) {
                Receiver receiver = input.left;
                Source source = input.right;

                source.init(mapping, receiver.distribution(), ctx, mq);
            }
        }
    }

    @Override public long exchangeId() {
        return exchangeId;
    }

    @Override public void init(NodesMapping mapping, DistributionTrait distribution, Context ctx, RelMetadataQuery mq) {
        assert remote();

        ((Sender) root).init(new TargetImpl(exchangeId, mapping, distribution));

        init(ctx, mq);
    }

    public RelNode root() {
        return root;
    }

    @Override public NodesMapping mapping() {
        return mapping;
    }

    private boolean remote() {
        return root instanceof Sender;
    }

    private LocationRegistry registry(Context ctx) {
        return Objects.requireNonNull(ctx.unwrap(LocationRegistry.class));
    }

    private AffinityTopologyVersion topologyVersion(Context ctx) {
        return Objects.requireNonNull(ctx.unwrap(AffinityTopologyVersion.class));
    }
}
