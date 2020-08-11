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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdNodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;

/**
 * Fragment of distributed query
 */
public class Fragment {
    /** */
    static final AtomicLong ID_GEN = new AtomicLong();

    /** */
    private final long id;

    /** */
    private final IgniteRel root;

    /** */
    private final ImmutableList<IgniteReceiver> remotes;

    /**
     * @param id Fragment id.
     * @param root Root node of the fragment.
     * @param remotes Remote sources of the fragment.
     */
    public Fragment(long id, IgniteRel root, List<IgniteReceiver> remotes) {
        this.id = id;
        this.root = root;
        this.remotes = ImmutableList.copyOf(remotes);
    }

    /**
     * Mapps the fragment to its data location.
     *
     * @param mappingService Mapping service.
     * @param ctx Planner context.
     * @param mq Metadata query.
     */
    NodesMapping map(MappingService mappingService, PlanningContext ctx, RelMetadataQuery mq) throws OptimisticPlanningException {
        NodesMapping mapping = IgniteMdNodesMapping._nodesMapping(root, mq);

        try {
            if (mapping != null)
                mapping = local() ? localMapping(ctx).mergeWith(mapping) : mapping;
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

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return id;
    }

    /**
     * @return Root node.
     */
    public IgniteRel root() {
        return root;
    }

    /**
     * @return Fragment remote sources.
     */
    public List<IgniteReceiver> remotes() {
        return remotes;
    }

    /** */
    public boolean local() {
        return !(root instanceof IgniteSender);
    }

    /** */
    private NodesMapping localMapping(PlanningContext ctx) {
        return new NodesMapping(Collections.singletonList(ctx.localNodeId()), null, NodesMapping.CLIENT);
    }
}
