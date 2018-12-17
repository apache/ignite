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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: partition states.
 */
public class SqlSystemViewPartitionStates extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewPartitionStates(GridKernalContext ctx) {
        super("PARTITION_STATES", "Partition states (allocation map)", ctx, "CACHE_GROUP_ID,NODE_ID,PARTITION",
            newColumn("CACHE_GROUP_ID", Value.INT),
            newColumn("NODE_ID", Value.UUID),
            newColumn("PARTITION", Value.INT),
            newColumn("STATE")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(final Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition grpIdCond = conditionForColumn("CACHE_GROUP_ID", first, last);
        SqlSystemViewColumnCondition partCond = conditionForColumn("PARTITION", first, last);
        SqlSystemViewColumnCondition nodeCond = conditionForColumn("NODE_ID", first, last);

        Integer grpIdFilter = grpIdCond.isEquality() ? grpIdCond.valueForEquality().getInt() : null;
        Integer partFilter = partCond.isEquality() ? partCond.valueForEquality().getInt() : null;
        UUID nodeFilter = nodeCond.isEquality() ? uuidFromValue(nodeCond.valueForEquality()) : null;

        if (nodeCond.isEquality() && nodeFilter == null)
            return Collections.emptyIterator();

        AtomicLong rowKey = new AtomicLong();

        return F.concat(F.concat(F.iterator(grpPartTop(grpIdFilter).entrySet(),
            grpTop -> F.iterator(nodeParts(grpTop.getValue(), nodeFilter).entrySet(),
                nodeToParts -> F.iterator(partStates(nodeToParts.getValue(), partFilter),
                    partToStates -> createRow(ses,
                        rowKey.incrementAndGet(),
                        grpTop.getKey(),
                        nodeToParts.getKey(),
                        partToStates.getKey(),
                        partToStates.getValue()),
                    true),
                true),
            true)));
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        // Approximate row count.
        return ctx.cache().cacheGroups().size() * ctx.discovery().aliveServerNodes().size() *
            RendezvousAffinityFunction.DFLT_PARTITION_COUNT;
    }

    /**
     * Filtered set of partition states.
     *
     * @param partMap Partition map.
     * @param partFilter Partition number or {@code null} if filter don't needed.
     */
    private Set<Map.Entry<Integer, GridDhtPartitionState>> partStates(GridDhtPartitionMap partMap, Integer partFilter) {
        if (partFilter == null || partFilter < 0)
            return partMap.entrySet();

        GridDhtPartitionState state = partMap.get(partFilter);

        return state == null ? Collections.emptySet() : F.asMap(partFilter, state).entrySet();
    }

    /**
     * Filtered map of nodes partition states.
     *
     * @param top Cache group partition topology.
     * @param nodeFilter Node id or {@code null} if filter don't needed.
     */
    private Map<UUID, GridDhtPartitionMap> nodeParts(GridDhtPartitionTopology top, UUID nodeFilter) {
        if (nodeFilter == null)
            return top.partitionMap(false);

        GridDhtPartitionMap partMap = top.partitions(nodeFilter);

        return partMap == null ? Collections.emptyMap() : F.asMap(nodeFilter, partMap);
    }

    /**
     * Filtered map of cache group partition topologies.
     *
     * @param grpIdFilter Group id or {@code null} if filter don't needed.
     */
    private Map<Integer, GridDhtPartitionTopology> grpPartTop(Integer grpIdFilter) {
       if (grpIdFilter == null) {
           return ctx.cache().cacheGroups().stream().collect(Collectors.toMap(CacheGroupContext::groupId,
               CacheGroupContext::topology));
       }

       CacheGroupContext gctx = ctx.cache().cacheGroup(grpIdFilter);

       return gctx == null ? Collections.emptyMap() : F.asMap(grpIdFilter, gctx.topology());
    }
}
