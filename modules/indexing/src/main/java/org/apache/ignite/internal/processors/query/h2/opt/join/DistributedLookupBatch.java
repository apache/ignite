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

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
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

import javax.cache.CacheException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import static org.apache.ignite.internal.processors.query.h2.opt.join.DistributedJoinMode.LOCAL_ONLY;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.KEY_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor.COL_NOT_EXISTS;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds.rangeBounds;

/**
 * Index lookup batch.
 */
public class DistributedLookupBatch implements IndexLookupBatch {
    /** Index. */
    private final GridH2IndexBase idx;

    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final boolean ucast;

    /** */
    private final int affColId;

    /** */
    private GridH2QueryContext qctx;

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
    public DistributedLookupBatch(GridH2IndexBase idx, GridCacheContext<?, ?> cctx, boolean ucast, int affColId) {
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
            return affKeyFirst == ValueNull.INSTANCE ? GridH2IndexBase.EXPLICIT_NULL : affKeyFirst.getObject();

        if (idx.getTable().rowDescriptor().isKeyColumn(affColId))
            return null;

        // Try to extract affinity key from primary key.
        Value pkFirst = firstRow.getValue(KEY_COL);
        Value pkLast = lastRow.getValue(KEY_COL);

        if (pkFirst == ValueNull.INSTANCE || pkLast == ValueNull.INSTANCE)
            return GridH2IndexBase.EXPLICIT_NULL;

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
        if (qctx == null || findCalled) {
            if (qctx == null) {
                // It is the first call after query begin (may be after reuse),
                // reinitialize query context and result.
                qctx = GridH2QueryContext.get();
                res = new ArrayList<>();

                assert qctx != null;
                assert !findCalled;
            }
            else {
                // Cleanup after the previous lookup phase.
                assert batchLookupId != 0;

                findCalled = false;
                qctx.putStreams(batchLookupId, null);
                res.clear();
            }

            // Reinitialize for the next lookup phase.
            batchLookupId = qctx.nextBatchLookupId();
            rangeStreams = new HashMap<>();
        }

        Object affKey = getAffinityKey(firstRow, lastRow);

        boolean locQry = localQuery();

        List<SegmentKey> segmentKeys;

        if (affKey != null) {
            // Affinity key is provided.
            if (affKey == GridH2IndexBase.EXPLICIT_NULL) // Affinity key is explicit null, we will not find anything.
                return false;

            segmentKeys = F.asList(rangeSegment(affKey, locQry));
        }
        else {
            // Affinity key is not provided or is not the same in upper and lower bounds, we have to broadcast.
            if (broadcastSegments == null)
                broadcastSegments = broadcastSegments(locQry);

            segmentKeys = broadcastSegments;
        }

        if (locQry && segmentKeys.isEmpty())
            return false; // Nothing to do

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
                stream = new RangeStream(cctx.kernalContext(), idx, qctx, segmentKey.node());

                stream.request(createRequest(qctx, batchLookupId, segmentKey.segmentId()));
                stream.request().bounds(bounds = new ArrayList<>());

                rangeStreams.put(segmentKey, stream);
            }
            else
                bounds = stream.request().bounds();

            bounds.add(rangeBounds);

            // If at least one node will have a full batch then we are ok.
            if (bounds.size() >= qctx.pageSize())
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
     * @return {@code True} if local query execution is enforced.
     */
    private boolean localQuery() {
        assert qctx != null : "Missing query context: " + this;

        return qctx.distributedJoinMode() == LOCAL_ONLY;
    }

    /**
     *
     */
    private void startStreams() {
        if (rangeStreams.isEmpty()) {
            assert res.isEmpty();

            return;
        }

        qctx.putStreams(batchLookupId, rangeStreams);

        // Start streaming.
        for (RangeStream stream : rangeStreams.values()) {
            stream.start();
        }
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
        if (beforeQry || qctx == null) // Query context can be null if addSearchRows was never called.
            return;

        assert batchLookupId != 0;

        // Do cleanup after the query run.
        qctx.putStreams(batchLookupId, null);
        qctx = null; // The same query can be reused multiple times for different query contexts.
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
     * @param isLocalQry Local query flag.
     * @return Segment key for Affinity key.
     */
    public SegmentKey rangeSegment(Object affKeyObj, boolean isLocalQry) {
        assert affKeyObj != null && affKeyObj != GridH2IndexBase.EXPLICIT_NULL : affKeyObj;

        ClusterNode node;

        int partition = cctx.affinity().partition(affKeyObj);

        if (isLocalQry) {
            if (qctx.partitionsMap() != null) {
                // If we have explicit partitions map, we have to use it to calculate affinity node.
                UUID nodeId = qctx.nodeForPartition(partition, cctx);

                if(!cctx.localNodeId().equals(nodeId))
                    return null; // Prevent remote index call for local queries.
            }

            if (!cctx.affinity().primaryByKey(cctx.localNode(), partition, qctx.topologyVersion()))
                return null;

            node = cctx.localNode();
        }
        else{
            if (qctx.partitionsMap() != null) {
                // If we have explicit partitions map, we have to use it to calculate affinity node.
                UUID nodeId = qctx.nodeForPartition(partition, cctx);

                node = cctx.discovery().node(nodeId);
            }
            else // Get primary node for current topology version.
                node = cctx.affinity().primaryByKey(affKeyObj, qctx.topologyVersion());

            if (node == null) // Node was not found, probably topology changed and we need to retry the whole query.
                throw H2Utils.retryException("Failed to get primary node by key for range segment.");
        }

        return new SegmentKey(node, idx.segmentForPartition(partition));
    }

    /**
     * @param isLocalQry Local query flag.
     * @return Collection of nodes for broadcasting.
     */
    public List<SegmentKey> broadcastSegments(boolean isLocalQry) {
        Map<UUID, int[]> partMap = qctx.partitionsMap();

        List<ClusterNode> nodes;

        if (isLocalQry) {
            if (partMap != null && !partMap.containsKey(cctx.localNodeId()))
                return Collections.emptyList(); // Prevent remote index call for local queries.

            nodes = Collections.singletonList(cctx.localNode());
        }
        else {
            if (partMap == null)
                nodes = new ArrayList<>(CU.affinityNodes(cctx, qctx.topologyVersion()));
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
        }

        int segmentsCount = idx.segmentsCount();

        List<SegmentKey> res = new ArrayList<>(nodes.size() * segmentsCount);

        for (ClusterNode node : nodes) {
            for (int seg = 0; seg < segmentsCount; seg++)
                res.add(new SegmentKey(node, seg));
        }

        return res;
    }

    /**
     * @param qctx Query context.
     * @param batchLookupId Batch lookup ID.
     * @param segmentId Segment ID.
     * @return Index range request.
     */
    public static GridH2IndexRangeRequest createRequest(GridH2QueryContext qctx, int batchLookupId, int segmentId) {
        GridH2IndexRangeRequest req = new GridH2IndexRangeRequest();

        req.originNodeId(qctx.originNodeId());
        req.queryId(qctx.queryId());
        req.originSegmentId(qctx.segment());
        req.segment(segmentId);
        req.batchLookupId(batchLookupId);

        return req;
    }
}
