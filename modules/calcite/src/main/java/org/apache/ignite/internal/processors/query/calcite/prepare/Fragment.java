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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentInfo;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;

/**
 * Fragment of distributed query
 */
public class Fragment implements RelTargetAware {
    /** */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** */
    private final long id;

    /** */
    private final IgniteRel root;

    /** */
    private NodesMapping mapping;

    /**
     * @param root Root node of the fragment.
     */
    public Fragment(IgniteRel root) {
        this(ID_GEN.getAndIncrement(), root);
    }

    /**
     * @param id Fragment id.
     * @param root Root node of the fragment.
     */
    public Fragment(long id, IgniteRel root){
        this.id = id;
        this.root = root;
    }

    /**
     * Inits fragment and its dependencies. Mainly init process consists of data location calculation.
     *
     * @param mappingService Mapping service.
     * @param ctx Planner context.
     * @param mq Metadata query used for data location calculation.
     */
    public void init(MappingService mappingService, PlanningContext ctx, RelMetadataQuery mq) throws OptimisticPlanningException {
        FragmentInfo info = IgniteMdFragmentInfo._fragmentInfo(root, mq);

        mapping = fragmentMapping(mappingService, ctx, info);

        if (F.isEmpty(info.targetAwareList()))
            return;

        RelTargetImpl target = new RelTargetImpl(id, mapping);

        for (RelTargetAware aware : info.targetAwareList())
            aware.target(target);
    }

    /**
     * @return Root node.
     */
    public IgniteRel root() {
        return root;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return id;
    }

    /**
     * @return Fragment mapping.
     */
    public NodesMapping mapping() {
        return mapping;
    }

    /** {@inheritDoc} */
    @Override public void target(RelTarget target) {
        assert root instanceof RelTargetAware;

        ((RelTargetAware) root).target(target);
    }

    /** */
    public boolean local() {
        return !(root instanceof IgniteSender);
    }

    /** */
    private NodesMapping fragmentMapping(MappingService mappingService, PlanningContext ctx, FragmentInfo info) throws OptimisticPlanningException {
        NodesMapping mapping;

        try {
            if (info.mapped())
                mapping = local() ? localMapping(ctx).mergeWith(info.mapping()) : info.mapping();
            else if (local())
                mapping = localMapping(ctx);
            else {
                RelDistribution.Type type = ((IgniteSender)root).sourceDistribution().getType();

                boolean single = type == SINGLETON || type == BROADCAST_DISTRIBUTED;

                // TODO selection strategy.
                mapping = mappingService.mapBalanced(ctx.topologyVersion(), single ? 1 : 0, null);
            }
        }
        catch (LocationMappingException e) {
            throw new OptimisticPlanningException("Failed to calculate physical distribution", root, e);
        }

        return mapping.deduplicate();
    }

    /** */
    private NodesMapping localMapping(PlanningContext ctx) {
        return new NodesMapping(Collections.singletonList(ctx.localNodeId()), null, NodesMapping.CLIENT);
    }
}
