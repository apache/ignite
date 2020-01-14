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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;

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
    public void init(IgniteCalciteContext ctx, RelMetadataQuery mq) {
        FragmentInfo info = IgniteMdFragmentInfo.fragmentInfo(root, mq);

        mapping = fragmentMapping(ctx, info, mq);

        ImmutableList<Pair<IgniteReceiver, RelSource>> sources = info.sources();

        if (!F.isEmpty(sources)) {
            for (Pair<IgniteReceiver, RelSource> input : sources) {
                IgniteReceiver receiver = input.left;
                RelSource source = input.right;

                source.bindToTarget(new RelTargetImpl(id, fragmentMapping(ctx, info, mq), receiver.distribution()), ctx, mq);
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
    @Override public void bindToTarget(RelTarget target, IgniteCalciteContext ctx, RelMetadataQuery mq) {
        assert !local();

        ((IgniteSender) root).target(target);

        init(ctx, mq);
    }

    /** */
    public boolean local() {
        return !(root instanceof IgniteSender);
    }

    /** */
    private NodesMapping fragmentMapping(IgniteCalciteContext ctx, FragmentInfo info, RelMetadataQuery mq) {
        NodesMapping mapping;

        try {
            if (info.mapped())
                mapping = local() ? ctx.mapForLocal().mergeWith(info.mapping()) : info.mapping();
            else if (local())
                mapping = ctx.mapForLocal();
            else {
                RelDistribution.Type type = IgniteMdDistribution._distribution(root, mq).getType();

                boolean single = type == SINGLETON || type == BROADCAST_DISTRIBUTED;

                // TODO selection strategy.
                mapping = ctx.mapForIntermediate(single ? 1 : 0, null);
            }
        }
        catch (LocationMappingException e) {
            throw new OptimisticPlanningException("Failed to calculate physical distribution", new Edge(null, root, -1));
        }

        return mapping.deduplicate();
    }
}
