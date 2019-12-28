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

import com.google.common.collect.ImmutableList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Fragment of distributed query
 */
public class Fragment implements RelSource {
    /** */
    public static final long UNDEFINED_ID = -1;

    /** */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** */
    private final long id;

    /** */
    private final RelNode root;

    /** */
    private NodesMapping mapping;

    /**
     * @param root Root node of the fragment.
     */
    public Fragment(RelNode root) {
        id = ID_GEN.getAndIncrement();

        this.root = root;
    }

    /**
     * Inits fragment and its dependencies. Mainly init process consists of data location calculation.
     *
     * @param ctx Planner context.
     * @param mq Metadata query used for data location calculation.
     */
    public void init(PlannerContext ctx, RelMetadataQuery mq) {
        FragmentInfo info = IgniteMdFragmentInfo.fragmentInfo(root, mq);

        if (!info.mapped())
            mapping = remote() ? ctx.mapForRandom() : ctx.mapForLocal();
        else {
            mapping = info.mapping().deduplicate();

            if (!remote() && !mapping.nodes().contains(ctx.localNodeId()))
                throw new OptimisticPlanningException("Failed to calculate physical distribution", new Edge(null, root, -1));
        }

        ImmutableList<Pair<IgniteReceiver, RelSource>> sources = info.sources();

        if (!F.isEmpty(sources)) {
            for (Pair<IgniteReceiver, RelSource> input : sources) {
                IgniteReceiver receiver = input.left;
                RelSource source = input.right;

                source.init(mapping, receiver.distribution(), ctx, mq);
            }
        }
    }

    /**
     * @return Root node.
     */
    public RelNode root() {
        return root;
    }

    /** {@inheritDoc} */
    @Override public long fragmentId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public NodesMapping mapping() {
        return mapping;
    }

    /** {@inheritDoc} */
    @Override public void init(NodesMapping mapping, IgniteDistribution distribution, PlannerContext ctx, RelMetadataQuery mq) {
        assert remote();

        ((IgniteSender) root).target(new RelTargetImpl(mapping, distribution));

        init(ctx, mq);
    }

    /** */
    public boolean remote() {
        return root instanceof IgniteSender;
    }
}
