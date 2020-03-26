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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import javax.cache.CacheException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.h2.index.Cursor;
import org.h2.index.IndexLookupBatch;
import org.h2.result.SearchRow;
import org.h2.util.DoneFuture;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor.COL_NOT_EXISTS;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds.rangeBounds;

/**
 * Index lookup batch.
 */
public class DistributedLookupBatch implements IndexLookupBatch {
    /** */
    private static final Object EXPLICIT_NULL = new Object();

    /** Index. */
    private final H2TreeIndex idx;

    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final boolean ucast;

    /** */
    private final int affColId;

    /** Join context. */
    private DistributedJoinContext joinCtx;

    /** */
    private int batchLookupId;

    /** */
    private Map<SegmentKey, RangeStream> rangeStreams = Collections.emptyMap();

    /** */
    private List<SegmentKey> broadcastSegments;

    /** */
    private List<Future<Cursor>> res = Collections.emptyList();

    /** */
    private boolean batchFull;

    /** */
    private boolean findCalled;

    /**
     * @param cctx Cache Cache context.
     * @param ucast Unicast or broadcast query.
     * @param affColId Affinity column ID.
     */
    public DistributedLookupBatch(H2TreeIndex idx, GridCacheContext<?, ?> cctx,
        boolean ucast, int affColId) {
        this.idx = idx;
        this.cctx = cctx;
        this.ucast = ucast;
        this.affColId = affColId;
    }

    /**
     * @param firstRow First row.
     * @param lastRow Last row.
     * @return Affinity key or {@code null}.
     */
    private Object getAffinityKey(SearchRow firstRow, SearchRow lastRow) {
        if (affColId == COL_NOT_EXISTS)
            return null;

        if (firstRow == null || lastRow == null)
            return null;

        Value affKeyFirst = firstRow.getValue(affColId);
        Value affKeyLast = lastRow.getValue(affColId);

        if (affKeyFirst != null && equal(affKeyFirst, affKeyLast))
            return affKeyFirst == ValueNull.INSTANCE ? EXPLICIT_NULL : affKeyFirst.getObject();

        if (idx.getTable().rowDescriptor().isKeyColumn(affColId))
            return null;

        // Try to extract affinity key from primary key.
        Value pkFirst = firstRow.getValue(QueryUtils.KEY_COL);
        Value pkLast = lastRow.getValue(QueryUtils.KEY_COL);

        if (pkFirst == ValueNull.INSTANCE || pkLast == ValueNull.INSTANCE)
            return EXPLICIT_NULL;

        if (pkFirst == null || pkLast == null || !equal(pkFirst, pkLast))
            return null;

        Object pkAffKeyFirst = cctx.affinity().affinityKey(pkFirst.getObject());
        Object pkAffKeyLast = cctx.affinity().affinityKey(pkLast.getObject());

        if (pkAffKeyFirst == null || pkAffKeyLast == null)
            throw new CacheException("Cache key without affinity key.");

        if (pkAffKeyFirst.equals(pkAffKeyLast))
            return pkAffKeyFirst;

        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ForLoopReplaceableByForEach", "IfMayBeConditional"})
    @Override public boolean addSearchRows(SearchRow firstRow, SearchRow lastRow) {
        if (joinCtx == null || findCalled) {
            if (joinCtx == null) {
                // It is the first call after query begin (may be after reuse),
                // reinitialize query context and result.
                QueryContext qctx = QueryContext.threadLocal();

                res = new ArrayList<>();

                assert qctx != null;
                assert !findCalled;

                joinCtx = qctx.distributedJoinContext();
            }
            else {
                // Cleanup after the previous lookup phase.
                assert batchLookupId != 0;

                findCalled = false;
                joinCtx.putStreams(batchLookupId, null);
                res.clear();
            }

            // Reinitialize for the next lookup phase.
            batchLookupId = joinCtx.nextBatchLookupId();
            rangeStreams = new HashMap<>();
        }

        Object affKey = getAffinityKey(firstRow, lastRow);

        List<SegmentKey> segmentKeys;

        if (affKey != null) {
            // Affinity key is provided.
            if (affKey == EXPLICIT_NULL) // Affinity key is explicit null, we will not find anything.
                return false;

            segmentKeys = F.asList(rangeSegment(affKey));
        }
        else {
            // Affinity key is not provided or is not the same in upper and lower bounds, we have to broadcast.
            if (broadcastSegments == null)
                broadcastSegments = broadcastSegments();

            segmentKeys = broadcastSegments;
        }

        assert !F.isEmpty(segmentKeys) : segmentKeys;

        final int rangeId = res.size();

        // Create messages.
        GridH2RowMessage first = idx.toSearchRowMessage(firstRow);
        GridH2RowMessage last = idx.toSearchRowMessage(lastRow);

        // Range containing upper and lower bounds.
        GridH2RowRangeBounds rangeBounds = rangeBounds(rangeId, first, last);

        // Add range to every message of every participating node.
        for (int i = 0; i < segmentKeys.size(); i++) {
            SegmentKey segmentKey = segmentKeys.get(i);
            assert segmentKey != null;

            RangeStream stream = rangeStreams.get(segmentKey);

            List<GridH2RowRangeBounds> bounds;

            if (stream == null) {
                stream = new RangeStream(cctx.kernalContext(), idx, joinCtx, segmentKey.node());

                stream.request(createRequest(joinCtx, batchLookupId, segmentKey.segmentId()));
                stream.request().bounds(bounds = new ArrayList<>());

                rangeStreams.put(segmentKey, stream);
            }
            else
                bounds = stream.request().bounds();

            bounds.add(rangeBounds);

            // If at least one node will have a full batch then we are ok.
            if (bounds.size() >= joinCtx.pageSize())
                batchFull = true;
        }

        Cursor cur;

        if (segmentKeys.size() == 1)
            cur = new UnicastCursor(rangeId, rangeStreams.get(F.first(segmentKeys)));
        else
            cur = new BroadcastCursor(idx, rangeId, segmentKeys, rangeStreams);

        res.add(new DoneFuture<>(cur));

        return true;
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return {@code true} If they equal.
     */
    private boolean equal(Value v1, Value v2) {
        return v1 == v2 || (v1 != null && v2 != null &&
            v1.compareTypeSafe(v2, idx.getDatabase().getCompareMode()) == 0);
    }

    /** {@inheritDoc} */
    @Override public boolean isBatchFull() {
        return batchFull;
    }

    /**
     *
     */
    private void startStreams() {
        if (rangeStreams.isEmpty()) {
            assert res.isEmpty();

            return;
        }

        joinCtx.putStreams(batchLookupId, rangeStreams);

        // Start streaming.
        for (RangeStream stream : rangeStreams.values())
            stream.start();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Override public List<Future<Cursor>> find() {
        batchFull = false;
        findCalled = true;

        startStreams();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void reset(boolean beforeQry) {
        if (beforeQry || joinCtx == null) // Query context can be null if addSearchRows was never called.
            return;

        assert batchLookupId != 0;

        // Do cleanup after the query run.
        joinCtx.putStreams(batchLookupId, null);

        joinCtx = null; // The same query can be reused multiple times for different query contexts.

        batchLookupId = 0;

        rangeStreams = Collections.emptyMap();
        broadcastSegments = null;
        batchFull = false;
        findCalled = false;
        res = Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public String getPlanSQL() {
        return ucast ? "unicast" : "broadcast";
    }

    /**
     * @param affKeyObj Affinity key.
     * @return Segment key for Affinity key.
     */
    public SegmentKey rangeSegment(Object affKeyObj) {
        assert affKeyObj != null && affKeyObj != EXPLICIT_NULL : affKeyObj;

        ClusterNode node;

        int partition = cctx.affinity().partition(affKeyObj);

        if (joinCtx.partitionsMap() != null) {
            // If we have explicit partitions map, we have to use it to calculate affinity node.
            UUID nodeId = joinCtx.nodeForPartition(partition, cctx);

            node = cctx.discovery().node(nodeId);
        }
        else // Get primary node for current topology version.
            node = cctx.affinity().primaryByKey(affKeyObj, joinCtx.topologyVersion());

        if (node == null) // Node was not found, probably topology changed and we need to retry the whole query.
            throw H2Utils.retryException("Failed to get primary node by key for range segment.");

        return new SegmentKey(node, idx.segmentForPartition(partition));
    }

    /**
     * @return Collection of nodes for broadcasting.
     */
    public List<SegmentKey> broadcastSegments() {
        Map<UUID, int[]> partMap = joinCtx.partitionsMap();

        List<ClusterNode> nodes;

        if (partMap == null)
            nodes = new ArrayList<>(CU.affinityNodes(cctx, joinCtx.topologyVersion()));
        else {
            nodes = new ArrayList<>(partMap.size());

            for (UUID nodeId : partMap.keySet()) {
                ClusterNode node = cctx.kernalContext().discovery().node(nodeId);

                if (node == null)
                    throw H2Utils.retryException("Failed to get node by ID during broadcast [" +
                        "nodeId=" + nodeId + ']');

                nodes.add(node);
            }
        }

        if (F.isEmpty(nodes))
            throw H2Utils.retryException("Failed to collect affinity nodes during broadcast [" +
                "cacheName=" + cctx.name() + ']');

        int segmentsCount = idx.segmentsCount();

        List<SegmentKey> res = new ArrayList<>(nodes.size() * segmentsCount);

        for (ClusterNode node : nodes) {
            for (int seg = 0; seg < segmentsCount; seg++)
                res.add(new SegmentKey(node, seg));
        }

        return res;
    }

    /**
     * @param joinCtx Join context.
     * @param batchLookupId Batch lookup ID.
     * @param segmentId Segment ID.
     * @return Index range request.
     */
    public static GridH2IndexRangeRequest createRequest(DistributedJoinContext joinCtx, int batchLookupId,
        int segmentId) {
        GridH2IndexRangeRequest req = new GridH2IndexRangeRequest();

        req.originNodeId(joinCtx.originNodeId());
        req.queryId(joinCtx.queryId());
        req.originSegmentId(joinCtx.segment());
        req.segment(segmentId);
        req.batchLookupId(batchLookupId);

        return req;
    }
}
