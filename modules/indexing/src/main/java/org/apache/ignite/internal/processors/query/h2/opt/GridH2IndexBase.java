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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRange;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexCondition;
import org.h2.index.IndexLookupBatch;
import org.h2.index.ViewIndex;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.util.DoneFuture;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.LOCAL_ONLY;
import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.OFF;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.KEY_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.VAL_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2CollocationModel.buildCollocationModel;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.PREPARE;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_ERROR;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_NOT_FOUND;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_OK;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds.rangeBounds;
import static org.h2.result.Row.MEMORY_CALCULATE;

/**
 * Index base.
 */
public abstract class GridH2IndexBase extends BaseIndex {
    /** */
    private static final Object EXPLICIT_NULL = new Object();

    /** */
    private Object msgTopic;

    /** */
    private GridMessageListener msgLsnr;

    /** */
    private IgniteLogger log;

    /** */
    private final CIX2<ClusterNode,Message> locNodeHnd = new CIX2<ClusterNode,Message>() {
        @Override public void applyx(ClusterNode clusterNode, Message msg) throws IgniteCheckedException {
            onMessage0(clusterNode.id(), msg);
        }
    };

    protected GridCacheContext<?, ?> ctx;

    /**
     * @param tbl Table.
     */
    protected final void initDistributedJoinMessaging(GridH2Table tbl) {
        final GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (desc != null && desc.context() != null) {
            ctx = desc.context();

            GridKernalContext ctx = desc.context().kernalContext();

            log = ctx.log(getClass());

            msgTopic = new IgniteBiTuple<>(GridTopic.TOPIC_QUERY, tbl.identifierString() + '.' + getName());

            msgLsnr = new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                    GridSpinBusyLock l = desc.indexing().busyLock();

                    if (!l.enterBusy())
                        return;

                    try {
                        onMessage0(nodeId, msg);
                    }
                    finally {
                        l.leaveBusy();
                    }
                }
            };

            ctx.io().addMessageListener(msgTopic, msgLsnr);
        }
        else {
            msgTopic = null;
            msgLsnr = null;
            log = new NullLogger();
        }
    }

    /** {@inheritDoc} */
    @Override public final void close(Session ses) {
        // No-op. Actual index destruction must happen in method destroy.
    }

    /**
     * Attempts to destroys index and release all the resources.
     * We use this method instead of {@link #close(Session)} because that method
     * is used by H2 internally.
     *
     * @param rmv Flag remove.
     */
    public void destroy(boolean rmv) {
        if (msgLsnr != null)
            kernalContext().io().removeMessageListener(msgTopic, msgLsnr);
    }

    /**
     * @return Index segment ID for current query context.
     */
    protected int threadLocalSegment() {
        if(segmentsCount() == 1)
            return 0;

        GridH2QueryContext qctx = GridH2QueryContext.get();

        if(qctx == null)
            throw new IllegalStateException("GridH2QueryContext is not initialized.");

        return qctx.segment();
    }

    /**
     * Puts row.
     *
     * @param row Row.
     * @return Existing row or {@code null}.
     */
    public abstract GridH2Row put(GridH2Row row);

    /**
     * Remove row from index.
     *
     * @param row Row.
     * @return Removed row.
     */
    public abstract GridH2Row remove(SearchRow row);

    /**
     * Remove row from index, does not return removed row.
     *
     * @param row Row.
     */
    public void removex(SearchRow row) {
        remove(row);
    }

    /**
     * @param ses Session.
     */
    private static void clearViewIndexCache(Session ses) {
        Map<Object,ViewIndex> viewIdxCache = ses.getViewIndexCache(true);

        if (!viewIdxCache.isEmpty())
            viewIdxCache.clear();
    }

    /**
     * @param ses Session.
     * @param filters All joined table filters.
     * @param filter Current filter.
     * @return Multiplier.
     */
    public final int getDistributedMultiplier(Session ses, TableFilter[] filters, int filter) {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        // We do optimizations with respect to distributed joins only on PREPARE stage only.
        // Notice that we check for isJoinBatchEnabled, because we can do multiple different
        // optimization passes on PREPARE stage.
        // Query expressions can not be distributed as well.
        if (qctx == null || qctx.type() != PREPARE || qctx.distributedJoinMode() == OFF ||
            !ses.isJoinBatchEnabled() || ses.isPreparingQueryExpression())
            return GridH2CollocationModel.MULTIPLIER_COLLOCATED;

        // We have to clear this cache because normally sub-query plan cost does not depend on anything
        // other than index condition masks and sort order, but in our case it can depend on order
        // of previous table filters.
        clearViewIndexCache(ses);

        assert filters != null;

        GridH2CollocationModel c = buildCollocationModel(qctx, ses.getSubQueryInfo(), filters, filter, false);

        return c.calculateMultiplier();
    }

    /** {@inheritDoc} */
    @Override public GridH2Table getTable() {
        return (GridH2Table)super.getTable();
    }

    /**
     * Filters rows from expired ones and using predicate.
     *
     * @param cursor GridCursor over rows.
     * @param filter Optional filter.
     * @return Filtered iterator.
     */
    protected GridCursor<GridH2Row> filter(GridCursor<GridH2Row> cursor, IndexingQueryFilter filter) {
        return new FilteringCursor(cursor, U.currentTimeMillis(), filter, getTable().cacheName());
    }

    /**
     * @return Filter for currently running query or {@code null} if none.
     */
    protected static IndexingQueryFilter threadLocalFilter() {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        return qctx != null ? qctx.filter() : null;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        // No-op: destroyed from owning table.
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        if (qctx == null || qctx.distributedJoinMode() == OFF || !getTable().isPartitioned())
            return null;

        IndexColumn affCol = getTable().getAffinityKeyColumn();
        GridH2RowDescriptor desc = getTable().rowDescriptor();

        int affColId = -1;
        boolean ucast = false;

        if (affCol != null) {
            affColId = affCol.column.getColumnId();
            int[] masks = filters[filter].getMasks();

            if (masks != null) {
                ucast = (masks[affColId] & IndexCondition.EQUALITY) != 0 ||
                        desc.checkKeyIndexCondition(masks, IndexCondition.EQUALITY);
            }
        }

        GridCacheContext<?, ?> cctx = getTable().rowDescriptor().context();

        return new DistributedLookupBatch(cctx, ucast, affColId);
    }

    /** {@inheritDoc} */
    @Override public void removeChildrenAndResources(Session session) {
        // The sole purpose of this override is to pass session to table.removeIndex
        assert table instanceof GridH2Table;

        ((GridH2Table)table).removeIndex(session, this);
        remove(session);
        database.removeMeta(session, getId());
    }

    /**
     * @param nodes Nodes.
     * @param msg Message.
     */
    private void send(Collection<ClusterNode> nodes, Message msg) {
        if (!getTable().rowDescriptor().indexing().send(msgTopic,
            -1,
            nodes,
            msg,
            null,
            locNodeHnd,
            GridIoPolicy.IDX_POOL,
            false))
            throw new GridH2RetryException("Failed to send message to nodes: " + nodes + ".");
    }

    /**
     * @param nodeId Source node ID.
     * @param msg Message.
     */
    private void onMessage0(UUID nodeId, Object msg) {
        ClusterNode node = kernalContext().discovery().node(nodeId);

        if (node == null)
            return;

        try {
            if (msg instanceof GridH2IndexRangeRequest)
                onIndexRangeRequest(node, (GridH2IndexRangeRequest)msg);
            else if (msg instanceof GridH2IndexRangeResponse)
                onIndexRangeResponse(node, (GridH2IndexRangeResponse)msg);
        }
        catch (Throwable th) {
            U.error(log, "Failed to handle message[nodeId=" + nodeId + ", msg=" + msg + "]", th);

            if (th instanceof Error)
                throw th;
        }
    }

    /**
     * @return Kernal context.
     */
    private GridKernalContext kernalContext() {
        return getTable().rowDescriptor().context().kernalContext();
    }

    /**
     * @param node Requesting node.
     * @param msg Request message.
     */
    private void onIndexRangeRequest(final ClusterNode node, final GridH2IndexRangeRequest msg) {
        GridH2IndexRangeResponse res = new GridH2IndexRangeResponse();

        res.originNodeId(msg.originNodeId());
        res.queryId(msg.queryId());
        res.originSegmentId(msg.originSegmentId());
        res.segment(msg.segment());
        res.batchLookupId(msg.batchLookupId());

        GridH2QueryContext qctx = GridH2QueryContext.get(kernalContext().localNodeId(), msg.originNodeId(),
            msg.queryId(), msg.originSegmentId(), MAP);

        if (qctx == null)
            res.status(STATUS_NOT_FOUND);
        else {
            try {
                RangeSource src;

                if (msg.bounds() != null) {
                    // This is the first request containing all the search rows.
                    assert !msg.bounds().isEmpty() : "empty bounds";

                    src = new RangeSource(msg.bounds(), msg.segment(), qctx.filter());
                }
                else {
                    // This is request to fetch next portion of data.
                    src = qctx.getSource(node.id(), msg.segment(), msg.batchLookupId());

                    assert src != null;
                }

                List<GridH2RowRange> ranges = new ArrayList<>();

                int maxRows = qctx.pageSize();

                assert maxRows > 0 : maxRows;

                while (maxRows > 0) {
                    GridH2RowRange range = src.next(maxRows);

                    if (range == null)
                        break;

                    ranges.add(range);

                    if (range.rows() != null)
                        maxRows -= range.rows().size();
                }

                assert !ranges.isEmpty();

                if (src.hasMoreRows()) {
                    // Save source for future fetches.
                    if (msg.bounds() != null)
                        qctx.putSource(node.id(), msg.segment(), msg.batchLookupId(), src);
                }
                else if (msg.bounds() == null) {
                    // Drop saved source.
                    qctx.putSource(node.id(), msg.segment(), msg.batchLookupId(), null);
                }

                res.ranges(ranges);
                res.status(STATUS_OK);
            }
            catch (Throwable th) {
                U.error(log, "Failed to process request: " + msg, th);

                res.error(th.getClass() + ": " + th.getMessage());
                res.status(STATUS_ERROR);
            }
        }

        send(singletonList(node), res);
    }

    /**
     * @param node Responded node.
     * @param msg Response message.
     */
    private void onIndexRangeResponse(ClusterNode node, GridH2IndexRangeResponse msg) {
        GridH2QueryContext qctx = GridH2QueryContext.get(kernalContext().localNodeId(),
            msg.originNodeId(), msg.queryId(), msg.originSegmentId(), MAP);

        if (qctx == null)
            return;

        Map<SegmentKey, RangeStream> streams = qctx.getStreams(msg.batchLookupId());

        if (streams == null)
            return;

        RangeStream stream = streams.get(new SegmentKey(node, msg.segment()));

        assert stream != null;

        stream.onResponse(msg);
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return {@code true} If they equal.
     */
    private boolean equal(Value v1, Value v2) {
        return v1 == v2 || (v1 != null && v2 != null && v1.compareTypeSafe(v2, getDatabase().getCompareMode()) == 0);
    }

    /**
     * @param qctx Query context.
     * @param batchLookupId Batch lookup ID.
     * @param segmentId Segment ID.
     * @return Index range request.
     */
    private static GridH2IndexRangeRequest createRequest(GridH2QueryContext qctx, int batchLookupId, int segmentId) {
        GridH2IndexRangeRequest req = new GridH2IndexRangeRequest();

        req.originNodeId(qctx.originNodeId());
        req.queryId(qctx.queryId());
        req.originSegmentId(qctx.segment());
        req.segment(segmentId);
        req.batchLookupId(batchLookupId);

        return req;
    }


    /**
     * @param qctx Query context.
     * @param cctx Cache context.
     * @param isLocalQry Local query flag.
     * @return Collection of nodes for broadcasting.
     */
    private List<SegmentKey> broadcastSegments(GridH2QueryContext qctx, GridCacheContext<?, ?> cctx, boolean isLocalQry) {
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

                GridKernalContext ctx = kernalContext();

                for (UUID nodeId : partMap.keySet()) {
                    ClusterNode node = ctx.discovery().node(nodeId);

                    if (node == null)
                        throw new GridH2RetryException("Failed to find node.");

                    nodes.add(node);
                }
            }

            if (F.isEmpty(nodes))
                throw new GridH2RetryException("Failed to collect affinity nodes.");
        }

        int segmentsCount = segmentsCount();

        List<SegmentKey> res = new ArrayList<>(nodes.size() * segmentsCount);

        for (ClusterNode node : nodes) {
            for (int seg = 0; seg < segmentsCount; seg++)
                res.add(new SegmentKey(node, seg));
        }

        return res;
    }

    /**
     * @param cctx Cache context.
     * @param qctx Query context.
     * @param affKeyObj Affinity key.
     * @param isLocalQry Local query flag.
     * @return Segment key for Affinity key.
     */
    private SegmentKey rangeSegment(GridCacheContext<?, ?> cctx, GridH2QueryContext qctx, Object affKeyObj, boolean isLocalQry) {
        assert affKeyObj != null && affKeyObj != EXPLICIT_NULL : affKeyObj;

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
                throw new GridH2RetryException("Failed to find node.");
        }

        return new SegmentKey(node, segmentForPartition(partition));
    }

    /** */
    protected class SegmentKey {
        /** */
        final ClusterNode node;

        /** */
        final int segmentId;

        SegmentKey(ClusterNode node, int segmentId) {
            assert node != null;

            this.node = node;
            this.segmentId = segmentId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            SegmentKey key = (SegmentKey)o;

            return segmentId == key.segmentId && node.id().equals(key.node.id());

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = node.hashCode();
            result = 31 * result + segmentId;
            return result;
        }
    }

    /**
     * @param row Row.
     * @return Row message.
     */
    private GridH2RowMessage toRowMessage(Row row) {
        if (row == null)
            return null;

        int cols = row.getColumnCount();

        assert cols > 0 : cols;

        List<GridH2ValueMessage> vals = new ArrayList<>(cols);

        for (int i = 0; i < cols; i++) {
            try {
                vals.add(GridH2ValueMessageFactory.toMessage(row.getValue(i)));
            }
            catch (IgniteCheckedException e) {
                throw new CacheException(e);
            }
        }

        GridH2RowMessage res = new GridH2RowMessage();

        res.values(vals);

        return res;
    }

    /**
     * @param msg Row message.
     * @return Search row.
     */
    private SearchRow toSearchRow(GridH2RowMessage msg) {
        if (msg == null)
            return null;

        GridKernalContext ctx = kernalContext();

        Value[] vals = new Value[getTable().getColumns().length];

        assert vals.length > 0;

        List<GridH2ValueMessage> msgVals = msg.values();

        for (int i = 0; i < indexColumns.length; i++) {
            if (i >= msgVals.size())
                continue;

            try {
                vals[indexColumns[i].column.getColumnId()] = msgVals.get(i).value(ctx);
            }
            catch (IgniteCheckedException e) {
                throw new CacheException(e);
            }
        }

        return database.createRow(vals, MEMORY_CALCULATE);
    }

    /**
     * @param row Search row.
     * @return Row message.
     */
    private GridH2RowMessage toSearchRowMessage(SearchRow row) {
        if (row == null)
            return null;

        List<GridH2ValueMessage> vals = new ArrayList<>(indexColumns.length);

        for (IndexColumn idxCol : indexColumns) {
            Value val = row.getValue(idxCol.column.getColumnId());

            if (val == null)
                break;

            try {
                vals.add(GridH2ValueMessageFactory.toMessage(val));
            }
            catch (IgniteCheckedException e) {
                throw new CacheException(e);
            }
        }

        GridH2RowMessage res = new GridH2RowMessage();

        res.values(vals);

        return res;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param cmp Comparator.
     */
    public static <Z> void bubbleUp(Z[] arr, int off, Comparator<Z> cmp) {
        // TODO Optimize: use binary search if the range in array is big.
        for (int i = off, last = arr.length - 1; i < last; i++) {
            if (cmp.compare(arr[i], arr[i + 1]) <= 0)
                break;

            U.swap(arr, i, i + 1);
        }
    }

    /**
     * @param msg Message.
     * @return Row.
     */
    private Row toRow(GridH2RowMessage msg) {
        if (msg == null)
            return null;

        GridKernalContext ctx = kernalContext();

        List<GridH2ValueMessage> vals = msg.values();

        assert !F.isEmpty(vals) : vals;

        Value[] vals0 = new Value[vals.size()];

        for (int i = 0; i < vals0.length; i++) {
            try {
                vals0[i] = vals.get(i).value(ctx);
            }
            catch (IgniteCheckedException e) {
                throw new CacheException(e);
            }
        }

        return database.createRow(vals0, MEMORY_CALCULATE);
    }

    /** @return Index segments count. */
    protected abstract int segmentsCount();

    /**
     * @param partition Partition idx.
     * @return Segment ID for given key
     */
    protected int segmentForPartition(int partition){
        return segmentsCount() == 1 ? 0 : (partition % segmentsCount());
    }

    /**
     * @param row Table row.
     * @return Segment ID for given row.
     */
    protected int segmentForRow(SearchRow row) {
        assert row != null;

        CacheObject key;

        if (ctx != null) {
            final Value keyColValue = row.getValue(KEY_COL);

            assert keyColValue != null;

            final Object o = keyColValue.getObject();

            if (o instanceof CacheObject)
                key = (CacheObject)o;
            else
                key = ctx.toCacheKeyObject(o);

            return segmentForPartition(ctx.affinity().partition(key));
        }

        assert segmentsCount() == 1;

        return 0;
    }

    /**
     * Simple cursor from a single node.
     */
    private static class UnicastCursor implements Cursor {
        /** */
        final int rangeId;

        /** */
        RangeStream stream;

        /**
         * @param rangeId Range ID.
         * @param keys Remote index segment keys.
         * @param rangeStreams Range streams.
         */
        UnicastCursor(int rangeId, List<SegmentKey> keys, Map<SegmentKey, RangeStream> rangeStreams) {
            assert keys.size() == 1;

            this.rangeId = rangeId;
            this.stream = rangeStreams.get(F.first(keys));

            assert stream != null;
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            return stream.next(rangeId);
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return stream.get(rangeId);
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Merge cursor from multiple nodes.
     */
    private class BroadcastCursor implements Cursor, Comparator<RangeStream> {
        /** */
        final int rangeId;

        /** */
        final RangeStream[] streams;

        /** */
        boolean first = true;

        /** */
        int off;

        /**
         * @param rangeId Range ID.
         * @param segmentKeys Remote nodes.
         * @param rangeStreams Range streams.
         */
        BroadcastCursor(int rangeId, Collection<SegmentKey> segmentKeys, Map<SegmentKey, RangeStream> rangeStreams) {

            this.rangeId = rangeId;

            streams = new RangeStream[segmentKeys.size()];

            int i = 0;

            for (SegmentKey segmentKey : segmentKeys) {
                RangeStream stream = rangeStreams.get(segmentKey);

                assert stream != null;

                streams[i++] = stream;
            }
        }

        /** {@inheritDoc} */
        @Override public int compare(RangeStream o1, RangeStream o2) {
            if (o1 == o2)
                return 0;

            // Nulls are at the beginning of array.
            if (o1 == null)
                return -1;

            if (o2 == null)
                return 1;

            return compareRows(o1.get(rangeId), o2.get(rangeId));
        }

        /**
         * Try to fetch the first row.
         *
         * @return {@code true} If we were able to find at least one row.
         */
        private boolean goFirst() {
            // Fetch first row from all the streams and sort them.
            for (int i = 0; i < streams.length; i++) {
                if (!streams[i].next(rangeId)) {
                    streams[i] = null;
                    off++; // After sorting this offset will cut off all null elements at the beginning of array.
                }
            }

            if (off == streams.length)
                return false;

            Arrays.sort(streams, this);

            return true;
        }

        /**
         * Fetch next row.
         *
         * @return {@code true} If we were able to find at least one row.
         */
        private boolean goNext() {
            assert off != streams.length;

            if (!streams[off].next(rangeId)) {
                // Next row from current min stream was not found -> nullify that stream and bump offset forward.
                streams[off] = null;

                return ++off != streams.length;
            }

            // Bubble up current min stream with respect to fetched row to achieve correct sort order of streams.
            bubbleUp(streams, off, this);

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (first) {
                first = false;

                return goFirst();
            }

            return goNext();
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return streams[off].get(rangeId);
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Index lookup batch.
     */
    private class DistributedLookupBatch implements IndexLookupBatch {
        /** */
        final GridCacheContext<?,?> cctx;

        /** */
        final boolean ucast;

        /** */
        final int affColId;

        /** */
        GridH2QueryContext qctx;

        /** */
        int batchLookupId;

        /** */
        Map<SegmentKey, RangeStream> rangeStreams = Collections.emptyMap();

        /** */
        List<SegmentKey> broadcastSegments;

        /** */
        List<Future<Cursor>> res = Collections.emptyList();

        /** */
        boolean batchFull;

        /** */
        boolean findCalled;

        /**
         * @param cctx Cache Cache context.
         * @param ucast Unicast or broadcast query.
         * @param affColId Affinity column ID.
         */
        DistributedLookupBatch(GridCacheContext<?, ?> cctx, boolean ucast, int affColId) {
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
            if (firstRow == null || lastRow == null)
                return null;

            Value affKeyFirst = firstRow.getValue(affColId);
            Value affKeyLast = lastRow.getValue(affColId);

            if (affKeyFirst != null && equal(affKeyFirst, affKeyLast))
                return affKeyFirst == ValueNull.INSTANCE ? EXPLICIT_NULL : affKeyFirst.getObject();

            if (getTable().rowDescriptor().isKeyColumn(affColId))
                return null;

            // Try to extract affinity key from primary key.
            Value pkFirst = firstRow.getValue(KEY_COL);
            Value pkLast = lastRow.getValue(KEY_COL);

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
        @SuppressWarnings("ForLoopReplaceableByForEach")
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

            Object affKey = affColId == -1 ? null : getAffinityKey(firstRow, lastRow);

            boolean locQry = localQuery();

            List<SegmentKey> segmentKeys;

            if (affKey != null) {
                // Affinity key is provided.
                if (affKey == EXPLICIT_NULL) // Affinity key is explicit null, we will not find anything.
                    return false;

                segmentKeys = F.asList(rangeSegment(cctx, qctx, affKey, locQry));
            }
            else {
                // Affinity key is not provided or is not the same in upper and lower bounds, we have to broadcast.
                if (broadcastSegments == null)
                    broadcastSegments = broadcastSegments(qctx, cctx, locQry);

                segmentKeys = broadcastSegments;
            }

            if (locQry && segmentKeys.isEmpty())
                return false; // Nothing to do

            assert !F.isEmpty(segmentKeys) : segmentKeys;

            final int rangeId = res.size();

            // Create messages.
            GridH2RowMessage first = toSearchRowMessage(firstRow);
            GridH2RowMessage last = toSearchRowMessage(lastRow);

            // Range containing upper and lower bounds.
            GridH2RowRangeBounds rangeBounds = rangeBounds(rangeId, first, last);

            // Add range to every message of every participating node.
            for (int i = 0; i < segmentKeys.size(); i++) {
                SegmentKey segmentKey = segmentKeys.get(i);
                assert segmentKey != null;

                RangeStream stream = rangeStreams.get(segmentKey);

                List<GridH2RowRangeBounds> bounds;

                if (stream == null) {
                    stream = new RangeStream(qctx, segmentKey.node);

                    stream.req = createRequest(qctx, batchLookupId, segmentKey.segmentId);
                    stream.req.bounds(bounds = new ArrayList<>());

                    rangeStreams.put(segmentKey, stream);
                }
                else
                    bounds = stream.req.bounds();

                bounds.add(rangeBounds);

                // If at least one node will have a full batch then we are ok.
                if (bounds.size() >= qctx.pageSize())
                    batchFull = true;
            }

            Future<Cursor> fut = new DoneFuture<>(segmentKeys.size() == 1 ?
                new UnicastCursor(rangeId, segmentKeys, rangeStreams) :
                new BroadcastCursor(rangeId, segmentKeys, rangeStreams));

            res.add(fut);

            return true;
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
            for (RangeStream stream : rangeStreams.values())
                stream.start();
        }

        /** {@inheritDoc} */
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
    }

    /**
     * Per node range stream.
     */
    private class RangeStream {
        /** */
        final GridH2QueryContext qctx;

        /** */
        final ClusterNode node;

        /** */
        GridH2IndexRangeRequest req;

        /** */
        int remainingRanges;

        /** */
        final BlockingQueue<GridH2IndexRangeResponse> respQueue = new LinkedBlockingQueue<>();

        /** */
        Iterator<GridH2RowRange> ranges = emptyIterator();

        /** */
        Cursor cursor = GridH2Cursor.EMPTY;

        /** */
        int cursorRangeId = -1;

        /**
         * @param qctx Query context.
         * @param node Node.
         */
        RangeStream(GridH2QueryContext qctx, ClusterNode node) {
            this.node = node;
            this.qctx = qctx;
        }

        /**
         * Start streaming.
         */
        private void start() {
            assert ctx != null;
            assert log != null: getName();

            remainingRanges = req.bounds().size();

            assert remainingRanges > 0;

            if (log.isDebugEnabled())
                log.debug("Starting stream: [node=" + node + ", req=" + req + "]");

            send(singletonList(node), req);
        }

        /**
         * @param msg Response.
         */
        public void onResponse(GridH2IndexRangeResponse msg) {
            respQueue.add(msg);
        }

        /**
         * @return Response.
         */
        private GridH2IndexRangeResponse awaitForResponse() {
            assert remainingRanges > 0;

            final long start = U.currentTimeMillis();

            for (int attempt = 0;; attempt++) {
                if (qctx.isCleared())
                    throw new GridH2RetryException("Query is cancelled.");

                if (kernalContext().isStopping())
                    throw new GridH2RetryException("Stopping node.");

                GridH2IndexRangeResponse res;

                try {
                    res = respQueue.poll(500, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException ignored) {
                    throw new GridH2RetryException("Interrupted.");
                }

                if (res != null) {
                    switch (res.status()) {
                        case STATUS_OK:
                            List<GridH2RowRange> ranges0 = res.ranges();

                            remainingRanges -= ranges0.size();

                            if (ranges0.get(ranges0.size() - 1).isPartial())
                                remainingRanges++;

                            if (remainingRanges > 0) {
                                if (req.bounds() != null)
                                    req = createRequest(qctx, req.batchLookupId(), req.segment());

                                // Prefetch next page.
                                send(singletonList(node), req);
                            }
                            else
                                req = null;

                            return res;

                        case STATUS_NOT_FOUND:
                            if (req == null || req.bounds() == null) // We have already received the first response.
                                throw new GridH2RetryException("Failure on remote node.");

                            if (U.currentTimeMillis() - start > 30_000)
                                throw new GridH2RetryException("Timeout.");

                            try {
                                U.sleep(20 * attempt);
                            }
                            catch (IgniteInterruptedCheckedException e) {
                                throw new IgniteInterruptedException(e.getMessage());
                            }

                            // Retry to send the request once more after some time.
                            send(singletonList(node), req);

                            break;

                        case STATUS_ERROR:
                            throw new CacheException(res.error());

                        default:
                            throw new IllegalStateException();
                    }
                }

                if (!kernalContext().discovery().alive(node))
                    throw new GridH2RetryException("Node left: " + node);
            }
        }

        /**
         * @param rangeId Requested range ID.
         * @return {@code true} If next row for the requested range was found.
         */
        private boolean next(final int rangeId) {
            for (;;) {
                if (rangeId == cursorRangeId) {
                    if (cursor.next())
                        return true;
                }
                else if (rangeId < cursorRangeId)
                    return false;

                cursor = GridH2Cursor.EMPTY;

                while (!ranges.hasNext()) {
                    if (remainingRanges == 0) {
                        ranges = emptyIterator();

                        return false;
                    }

                    ranges = awaitForResponse().ranges().iterator();
                }

                GridH2RowRange range = ranges.next();

                cursorRangeId = range.rangeId();

                if (!F.isEmpty(range.rows())) {
                    final Iterator<GridH2RowMessage> it = range.rows().iterator();

                    if (it.hasNext()) {
                        cursor = new GridH2Cursor(new Iterator<Row>() {
                            @Override public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override public Row next() {
                                // Lazily convert messages into real rows.
                                return toRow(it.next());
                            }

                            @Override public void remove() {
                                throw new UnsupportedOperationException();
                            }
                        });
                    }
                }
            }
        }

        /**
         * @param rangeId Requested range ID.
         * @return Current row.
         */
        private Row get(int rangeId) {
            assert rangeId == cursorRangeId;

            return cursor.get();
        }
    }

    /**
     * Bounds iterator.
     */
    private class RangeSource {
        /** */
        Iterator<GridH2RowRangeBounds> boundsIter;

        /** */
        int curRangeId = -1;

        /** */
        private final int segment;

        /** */
        final IndexingQueryFilter filter;

        /** Iterator. */
        Iterator<GridH2Row> iter = emptyIterator();

        /**
         * @param bounds Bounds.
         * @param filter Filter.
         */
        RangeSource(
            Iterable<GridH2RowRangeBounds> bounds,
            int segment,
            IndexingQueryFilter filter
        ) {
            this.segment = segment;
            this.filter = filter;
            boundsIter = bounds.iterator();
        }

        /**
         * @return {@code true} If there are more rows in this source.
         */
        public boolean hasMoreRows() throws IgniteCheckedException {
            return boundsIter.hasNext() || iter.hasNext();
        }

        /**
         * @param maxRows Max allowed rows.
         * @return Range.
         */
        public GridH2RowRange next(int maxRows) {
            assert maxRows > 0 : maxRows;

            for (; ; ) {
                if (iter.hasNext()) {
                    // Here we are getting last rows from previously partially fetched range.
                    List<GridH2RowMessage> rows = new ArrayList<>();

                    GridH2RowRange nextRange = new GridH2RowRange();

                    nextRange.rangeId(curRangeId);
                    nextRange.rows(rows);

                    do {
                        rows.add(toRowMessage(iter.next()));
                    }
                    while (rows.size() < maxRows && iter.hasNext());

                    if (iter.hasNext())
                        nextRange.setPartial();
                    else
                        iter = emptyIterator();

                    return nextRange;
                }

                iter = emptyIterator();

                if (!boundsIter.hasNext()) {
                    boundsIter = emptyIterator();

                    return null;
                }

                GridH2RowRangeBounds bounds = boundsIter.next();

                curRangeId = bounds.rangeId();

                SearchRow first = toSearchRow(bounds.first());
                SearchRow last = toSearchRow(bounds.last());

                IgniteTree t = treeForRead(segment);

                iter = new CursorIteratorWrapper(doFind0(t, first, true, last, filter));

                if (!iter.hasNext()) {
                    // We have to return empty range here.
                    GridH2RowRange emptyRange = new GridH2RowRange();

                    emptyRange.rangeId(curRangeId);

                    return emptyRange;
                }
            }
        }
    }

    /**
     * @param segment Segment Id.
     * @return Snapshot for requested segment if there is one.
     */
    protected <K, V> IgniteTree<K, V> treeForRead(int segment) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param t Tree.
     * @param first Lower bound.
     * @param includeFirst Whether lower bound should be inclusive.
     * @param last Upper bound always inclusive.
     * @param filter Filter.
     * @return Iterator over rows in given range.
     */
    protected GridCursor<GridH2Row> doFind0(
        IgniteTree t,
        @Nullable SearchRow first,
        boolean includeFirst,
        @Nullable SearchRow last,
        IndexingQueryFilter filter) {
        throw new UnsupportedOperationException();
    }

    /**
     * Cursor which filters by expiration time and predicate.
     */
    protected static class FilteringCursor implements GridCursor<GridH2Row> {
        /** */
        private final GridCursor<GridH2Row> cursor;
        /** */
        private final IgniteBiPredicate<Object, Object> fltr;

        /** */
        private final long time;

        /** Is value required for filtering predicate? */
        private final boolean isValRequired;

        /** */
        private GridH2Row next;

        /**
         * @param cursor GridCursor.
         * @param time Time for expired rows filtering.
         * @param qryFilter Filter.
         * @param cacheName Cache name.
         */
        protected FilteringCursor(GridCursor<GridH2Row> cursor, long time, IndexingQueryFilter qryFilter,
            String cacheName) {
            this.cursor = cursor;

            this.time = time;

            if (qryFilter != null) {
                this.fltr = qryFilter.forCache(cacheName);

                this.isValRequired = qryFilter.isValueRequired();
            }
            else {
                this.fltr = null;

                this.isValRequired = false;
            }
        }

        /**
         * @param row Row.
         * @return If this row was accepted.
         */
        @SuppressWarnings("unchecked")
        protected boolean accept(GridH2Row row) {
            if (row.expireTime() != 0 && row.expireTime() <= time)
                return false;

            if (fltr == null)
                return true;

            Object key = row.getValue(KEY_COL).getObject();
            Object val = isValRequired ? row.getValue(VAL_COL).getObject() : null;

            assert key != null;
            assert !isValRequired || val != null;

            return fltr.apply(key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean next() throws IgniteCheckedException {
            next = null;

            while (cursor.next()) {
                GridH2Row t = cursor.get();

                if (accept(t)) {
                    next = t;
                    return true;
                }
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public GridH2Row get() throws IgniteCheckedException {
            if (next == null)
                throw new NoSuchElementException();

            return next;
        }
    }

    /**
     *
     */
    private static final class CursorIteratorWrapper implements Iterator<GridH2Row> {
        /** */
        private final GridCursor<GridH2Row> cursor;

        /** Next element. */
        private GridH2Row next;

        /**
         * @param cursor Cursor.
         */
        private CursorIteratorWrapper(GridCursor<GridH2Row> cursor) {
            assert cursor != null;

            this.cursor = cursor;

            try {
                if (cursor.next())
                    next = cursor.get();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null;
        }

        /** {@inheritDoc} */
        @Override public GridH2Row next() {
            try {
                GridH2Row res = next;

                if (cursor.next())
                    next = cursor.get();
                else
                    next = null;

                return res;
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException("operation is not supported");
        }
    }

    /** Empty cursor. */
    protected static final GridCursor<GridH2Row> EMPTY_CURSOR = new GridCursor<GridH2Row>() {
        /** {@inheritDoc} */
        @Override public boolean next() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public GridH2Row get() {
            return null;
        }
    };
}
