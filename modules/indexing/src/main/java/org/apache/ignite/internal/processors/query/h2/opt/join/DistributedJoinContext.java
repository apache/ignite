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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Context for distributed joins.
 */
public class DistributedJoinContext {
    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final Map<UUID, int[]> partsMap;

    /** */
    private final UUID originNodeId;

    /** */
    private final long qryId;

    /** */
    private final int segment;

    /** */
    private final int pageSize;

    /** Range streams for indexes. */
    private Map<Integer, Object> streams;

    /** Range sources for indexes. */
    private Map<SourceKey, Object> sources;

    /** */
    private int batchLookupIdGen;

    /** */
    private UUID[] partsNodes;

    /** */
    private volatile boolean cancelled;

    /**
     * Constructor.
     *
     * @param topVer Topology version.
     * @param partsMap Partitions map.
     * @param originNodeId ID of the node started the query.
     * @param qryId Query ID.
     * @param segment Segment.
     * @param pageSize Pahe size.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public DistributedJoinContext(
        AffinityTopologyVersion topVer,
        Map<UUID, int[]> partsMap,
        UUID originNodeId,
        long qryId,
        int segment,
        int pageSize
    ) {
        this.topVer = topVer;
        this.partsMap = partsMap;
        this.originNodeId = originNodeId;
        this.qryId = qryId;
        this.segment = segment;
        this.pageSize = pageSize;
    }

    /**
     * @return Affinity topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Partitions map.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Map<UUID,int[]> partitionsMap() {
        return partsMap;
    }

    /**
     * @return Origin node ID.
     */
    public UUID originNodeId() {
        return originNodeId;
    }

    /**
     * @return Query request ID.
     */
    public long queryId() {
        return qryId;
    }

    /**
     * @return index segment ID.
     */
    public int segment() {
        return segment;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param p Partition.
     * @param cctx Cache context.
     * @return Owning node ID.
     */
    public UUID nodeForPartition(int p, GridCacheContext<?, ?> cctx) {
        UUID[] nodeIds = partsNodes;

        if (nodeIds == null) {
            assert partsMap != null;

            nodeIds = new UUID[cctx.affinity().partitions()];

            for (Map.Entry<UUID, int[]> e : partsMap.entrySet()) {
                UUID nodeId = e.getKey();
                int[] nodeParts = e.getValue();

                assert nodeId != null;
                assert !F.isEmpty(nodeParts);

                for (int part : nodeParts) {
                    assert nodeIds[part] == null;

                    nodeIds[part] = nodeId;
                }
            }

            partsNodes = nodeIds;
        }

        return nodeIds[p];
    }

    /**
     * @param batchLookupId Batch lookup ID.
     * @param streams Range streams.
     */
    public synchronized void putStreams(int batchLookupId, Object streams) {
        if (this.streams == null) {
            if (streams == null)
                return;

            this.streams = new HashMap<>();
        }

        if (streams == null)
            this.streams.remove(batchLookupId);
        else
            this.streams.put(batchLookupId, streams);
    }

    /**
     * @param batchLookupId Batch lookup ID.
     * @return Range streams.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T getStreams(int batchLookupId) {
        if (streams == null)
            return null;

        return (T)streams.get(batchLookupId);
    }

    /**
     * @param ownerId Owner node ID.
     * @param segmentId Index segment ID.
     * @param batchLookupId Batch lookup ID.
     * @param src Range source.
     */
    public synchronized void putSource(UUID ownerId, int segmentId, int batchLookupId, Object src) {
        SourceKey srcKey = new SourceKey(ownerId, segmentId, batchLookupId);

        if (src != null) {
            if (sources == null)
                sources = new HashMap<>();

            sources.put(srcKey, src);
        }
        else if (sources != null)
            sources.remove(srcKey);
    }

    /**
     * @param ownerId Owner node ID.
     * @param segmentId Index segment ID.
     * @param batchLookupId Batch lookup ID.
     * @return Range source.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T getSource(UUID ownerId, int segmentId, int batchLookupId) {
        if (sources == null)
            return null;

        return (T)sources.get(new SourceKey(ownerId, segmentId, batchLookupId));
    }

    /**
     * @return Next batch ID.
     */
    public int nextBatchLookupId() {
        return ++batchLookupIdGen;
    }

    /**
     * @return Cleared flag.
     */
    public boolean isCancelled() {
        return cancelled;
    }

    /**
     * Mark as cleared.
     */
    public void cancel() {
        cancelled = true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedJoinContext.class, this);
    }
}
