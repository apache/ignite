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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class Fragment implements RelSource {
    private static final AtomicLong ID_GEN = new AtomicLong();

    private final long exchangeId = ID_GEN.getAndIncrement();

    private final RelNode root;

    private NodesMapping mapping;

    public Fragment(RelNode root) {
        this.root = root;
    }

    public void init(PlannerContext ctx, RelMetadataQuery mq) {
        FragmentInfo info = IgniteMdFragmentInfo.fragmentInfo(root, mq);

        if (info.mapping() == null)
            mapping = remote() ? ctx.mapForRandom(ctx.topologyVersion()) : ctx.mapForLocal();
        else
            mapping = info.mapping().deduplicate();

        ImmutableList<Pair<IgniteReceiver, RelSource>> sources = info.sources();

        if (!F.isEmpty(sources)) {
            for (Pair<IgniteReceiver, RelSource> input : sources) {
                IgniteReceiver receiver = input.left;
                RelSource source = input.right;

                source.init(mapping, receiver.distribution(), ctx, mq);
            }
        }
    }

    @Override public long exchangeId() {
        return exchangeId;
    }

    @Override public void init(NodesMapping mapping, IgniteDistribution distribution, PlannerContext ctx, RelMetadataQuery mq) {
        assert remote();

        ((IgniteSender) root).target(new RelTargetImpl(exchangeId, mapping, distribution));

        init(ctx, mq);
    }

    public RelNode root() {
        return root;
    }

    @Override public NodesMapping mapping() {
        return mapping;
    }

    private boolean remote() {
        return root instanceof IgniteSender;
    }
}
