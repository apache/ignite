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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRange;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridFilteredIterator;
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
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.KEY_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.VAL_COL;
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
    private static final AtomicLong idxIdGen = new AtomicLong();

    /** */
    protected final long idxId = idxIdGen.incrementAndGet();

    /** */
    private final ThreadLocal<Object> snapshot = new ThreadLocal<>();

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

    /**
     * @param tbl Table.
     */
    protected final void initDistributedJoinMessaging(GridH2Table tbl) {
        final GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (desc != null && desc.context() != null) {
            GridKernalContext ctx = desc.context().kernalContext();

            log = ctx.log(getClass());

            msgTopic = new IgniteBiTuple<>(GridTopic.TOPIC_QUERY, tbl.identifier() + '.' + getName());

            msgLsnr = new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg) {
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
     */
    public void destroy() {
        if (msgLsnr != null)
            kernalContext().io().removeMessageListener(msgTopic, msgLsnr);
    }

    /**
     * If the index supports rebuilding it has to creates its own copy.
     *
     * @return Rebuilt copy.
     * @throws InterruptedException If interrupted.
     */
    public GridH2IndexBase rebuild() throws InterruptedException {
        return this;
    }

    /**
     * Put row if absent.
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
     * Takes or sets existing snapshot to be used in current thread.
     *
     * @param s Optional existing snapshot to use.
     * @param qctx Query context.
     * @return Snapshot.
     */
    public final Object takeSnapshot(@Nullable Object s, GridH2QueryContext qctx) {
        assert snapshot.get() == null;

        if (s == null)
            s = doTakeSnapshot();

        if (s != null) {
            if (s instanceof GridReservable && !((GridReservable)s).reserve())
                return null;

            snapshot.set(s);

            if (qctx != null)
                qctx.putSnapshot(idxId, s);
        }

        return s;
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
    public int getDistributedMultiplier(Session ses, TableFilter[] filters, int filter) {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        // We do complex optimizations with respect to distributed joins only on prepare stage
        // because on run stage reordering of joined tables by Optimizer is explicitly disabled
        // and thus multiplier will be always the same, so it will not affect choice of index.
        // Query expressions can not be distributed as well.
        if (qctx == null || qctx.type() != PREPARE || !qctx.distributedJoins() || ses.isPreparingQueryExpression())
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
     * Takes and returns actual snapshot or {@code null} if snapshots are not supported.
     *
     * @return Snapshot or {@code null}.
     */
    @Nullable protected abstract Object doTakeSnapshot();

    /**
     * @return Thread local snapshot.
     */
    @SuppressWarnings("unchecked")
    protected <T> T threadLocalSnapshot() {
        return (T)snapshot.get();
    }

    /**
     * Releases snapshot for current thread.
     */
    public void releaseSnapshot() {
        Object s = snapshot.get();

        assert s != null;

        snapshot.remove();

        if (s instanceof GridReservable)
            ((GridReservable)s).release();

        if (s instanceof AutoCloseable)
            U.closeQuiet((AutoCloseable)s);
    }

    /**
     * Filters rows from expired ones and using predicate.
     *
     * @param iter Iterator over rows.
     * @param filter Optional filter.
     * @return Filtered iterator.
     */
    protected Iterator<GridH2Row> filter(Iterator<GridH2Row> iter, IndexingQueryFilter filter) {
        return new FilteringIterator(iter, U.currentTimeMillis(), filter, getTable().spaceName());
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
        throw DbException.getUnsupportedException("remove index");
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
    @Override public IndexLookupBatch createLookupBatch(TableFilter filter) {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        if (qctx == null || !qctx.distributedJoins() || !getTable().isPartitioned())
            return null;

        IndexColumn affCol = getTable().getAffinityKeyColumn();

        int affColId;
        boolean ucast;

        if (affCol != null) {
            affColId = affCol.column.getColumnId();
            int[] masks = filter.getMasks();
            ucast = masks != null && masks[affColId] == IndexCondition.EQUALITY;
        }
        else {
            affColId = -1;
            ucast = false;
        }

        GridCacheContext<?,?> cctx = getTable().rowDescriptor().context();

        return new DistributedLookupBatch(cctx, ucast, affColId);
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
    private void onIndexRangeRequest(ClusterNode node, GridH2IndexRangeRequest msg) {
        GridH2QueryContext qctx = GridH2QueryContext.get(kernalContext().localNodeId(),
            msg.originNodeId(),
            msg.queryId(),
            MAP);

        GridH2IndexRangeResponse res = new GridH2IndexRangeResponse();

        res.originNodeId(msg.originNodeId());
        res.queryId(msg.queryId());
        res.batchLookupId(msg.batchLookupId());

        if (qctx == null)
            res.status(STATUS_NOT_FOUND);
        else {
            try {
                RangeSource src;

                if (msg.bounds() != null) {
                    // This is the first request containing all the search rows.
                    ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> snapshot0 = qctx.getSnapshot(idxId);

                    assert !msg.bounds().isEmpty() : "empty bounds";

                    src = new RangeSource(msg.bounds(), snapshot0, qctx.filter());
                }
                else {
                    // This is request to fetch next portion of data.
                    src = qctx.getSource(node.id(), msg.batchLookupId());

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

                if (src.hasMoreRows()) {
                    // Save source for future fetches.
                    if (msg.bounds() != null)
                        qctx.putSource(node.id(), msg.batchLookupId(), src);
                }
                else if (msg.bounds() == null) {
                    // Drop saved source.
                    qctx.putSource(node.id(), msg.batchLookupId(), null);
                }

                assert !ranges.isEmpty();

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
            msg.originNodeId(), msg.queryId(), MAP);

        if (qctx == null)
            return;

        Map<ClusterNode, RangeStream> streams = qctx.getStreams(msg.batchLookupId());

        if (streams == null)
            return;

        RangeStream stream = streams.get(node);

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
     * @return Index range request.
     */
    private static GridH2IndexRangeRequest createRequest(GridH2QueryContext qctx, int batchLookupId) {
        GridH2IndexRangeRequest req = new GridH2IndexRangeRequest();

        req.originNodeId(qctx.originNodeId());
        req.queryId(qctx.queryId());
        req.batchLookupId(batchLookupId);

        return req;
    }

    /**
     * @param qctx Query context.
     * @param cctx Cache context.
     * @return Collection of nodes for broadcasting.
     */
    private List<ClusterNode> broadcastNodes(GridH2QueryContext qctx, GridCacheContext<?,?> cctx) {
        Map<UUID, int[]> partMap = qctx.partitionsMap();

        List<ClusterNode> res;

        if (partMap == null)
            res = new ArrayList<>(CU.affinityNodes(cctx, qctx.topologyVersion()));
        else {
            res = new ArrayList<>(partMap.size());

            GridKernalContext ctx = kernalContext();

            for (UUID nodeId : partMap.keySet()) {
                ClusterNode node = ctx.discovery().node(nodeId);

                if (node == null)
                    throw new GridH2RetryException("Failed to find node.");

                res.add(node);
            }
        }

        if (F.isEmpty(res))
            throw new GridH2RetryException("Failed to collect affinity nodes.");

        return res;
    }

    /**
     * @param cctx Cache context.
     * @param qctx Query context.
     * @param affKeyObj Affinity key.
     * @return Cluster nodes or {@code null} if affinity key is a null value.
     */
    private ClusterNode rangeNode(GridCacheContext<?,?> cctx, GridH2QueryContext qctx, Object affKeyObj) {
        assert affKeyObj != null && affKeyObj != EXPLICIT_NULL : affKeyObj;

        ClusterNode node;

        if (qctx.partitionsMap() != null) {
            // If we have explicit partitions map, we have to use it to calculate affinity node.
            UUID nodeId = qctx.nodeForPartition(cctx.affinity().partition(affKeyObj), cctx);

            node = cctx.discovery().node(nodeId);
        }
        else // Get primary node for current topology version.
            node = cctx.affinity().primaryByKey(affKeyObj, qctx.topologyVersion());

        if (node == null) // Node was not found, probably topology changed and we need to retry the whole query.
            throw new GridH2RetryException("Failed to find node.");

        return node;
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
         * @param nodes Remote nodes.
         * @param rangeStreams Range streams.
         */
        private UnicastCursor(int rangeId, Collection<ClusterNode> nodes, Map<ClusterNode,RangeStream> rangeStreams) {
            assert nodes.size() == 1;

            this.rangeId = rangeId;
            this.stream = rangeStreams.get(F.first(nodes));

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
         * @param nodes Remote nodes.
         * @param rangeStreams Range streams.
         */
        private BroadcastCursor(int rangeId, Collection<ClusterNode> nodes, Map<ClusterNode,RangeStream> rangeStreams) {
            assert nodes.size() > 1;

            this.rangeId = rangeId;

            streams = new RangeStream[nodes.size()];

            int i = 0;

            for (ClusterNode node : nodes) {
                RangeStream stream = rangeStreams.get(node);

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
            for (int i = off, last = streams.length - 1; i < last; i++) {
                if (compareRows(streams[i].get(rangeId), streams[i + 1].get(rangeId)) <= 0)
                    break;

                U.swap(streams, i, i + 1);
            }

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
        Map<ClusterNode, RangeStream> rangeStreams = Collections.emptyMap();

        /** */
        List<ClusterNode> broadcastNodes;

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
        private DistributedLookupBatch(GridCacheContext<?,?> cctx, boolean ucast, int affColId) {
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

            if (affColId == KEY_COL)
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

            List<ClusterNode> nodes;
            Future<Cursor> fut;

            if (affKey != null) {
                // Affinity key is provided.
                if (affKey == EXPLICIT_NULL) // Affinity key is explicit null, we will not find anything.
                    return false;

                nodes = F.asList(rangeNode(cctx, qctx, affKey));
            }
            else {
                // Affinity key is not provided or is not the same in upper and lower bounds, we have to broadcast.
                if (broadcastNodes == null)
                    broadcastNodes = broadcastNodes(qctx, cctx);

                nodes = broadcastNodes;
            }

            assert !F.isEmpty(nodes) : nodes;

            final int rangeId = res.size();

            // Create messages.
            GridH2RowMessage first = toSearchRowMessage(firstRow);
            GridH2RowMessage last = toSearchRowMessage(lastRow);

            // Range containing upper and lower bounds.
            GridH2RowRangeBounds rangeBounds = rangeBounds(rangeId, first, last);

            // Add range to every message of every participating node.
            for (int i = 0; i < nodes.size(); i++) {
                ClusterNode node = nodes.get(i);
                assert node != null;

                RangeStream stream = rangeStreams.get(node);

                List<GridH2RowRangeBounds> bounds;

                if (stream == null) {
                    stream = new RangeStream(qctx, node);

                    stream.req = createRequest(qctx, batchLookupId);
                    stream.req.bounds(bounds = new ArrayList<>());

                    rangeStreams.put(node, stream);
                }
                else
                    bounds = stream.req.bounds();

                bounds.add(rangeBounds);

                // If at least one node will have a full batch then we are ok.
                if (bounds.size() >= qctx.pageSize())
                    batchFull = true;
            }

            fut = new DoneFuture<>(nodes.size() == 1 ?
                new UnicastCursor(rangeId, nodes, rangeStreams) :
                new BroadcastCursor(rangeId, nodes, rangeStreams));

            res.add(fut);

            return true;
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
            broadcastNodes = null;
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
                                    req = createRequest(qctx, req.batchLookupId());

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
        Iterator<GridH2Row> curRange = emptyIterator();

        /** */
        final ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree;

        /** */
        final IndexingQueryFilter filter;

        /**
         * @param bounds Bounds.
         * @param tree Snapshot.
         * @param filter Filter.
         */
        RangeSource(
            Iterable<GridH2RowRangeBounds> bounds,
            ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree,
            IndexingQueryFilter filter
        ) {
            this.filter = filter;
            this.tree = tree;
            boundsIter = bounds.iterator();
        }

        /**
         * @return {@code true} If there are more rows in this source.
         */
        public boolean hasMoreRows() {
            return boundsIter.hasNext() || curRange.hasNext();
        }

        /**
         * @param maxRows Max allowed rows.
         * @return Range.
         */
        public GridH2RowRange next(int maxRows) {
            assert maxRows > 0 : maxRows;

            for (;;) {
                if (curRange.hasNext()) {
                    // Here we are getting last rows from previously partially fetched range.
                    List<GridH2RowMessage> rows = new ArrayList<>();

                    GridH2RowRange nextRange = new GridH2RowRange();

                    nextRange.rangeId(curRangeId);
                    nextRange.rows(rows);

                    do {
                        rows.add(toRowMessage(curRange.next()));
                    }
                    while (rows.size() < maxRows && curRange.hasNext());

                    if (curRange.hasNext())
                        nextRange.setPartial();
                    else
                        curRange = emptyIterator();

                    return nextRange;
                }

                curRange = emptyIterator();

                if (!boundsIter.hasNext()) {
                    boundsIter = emptyIterator();

                    return null;
                }

                GridH2RowRangeBounds bounds = boundsIter.next();

                curRangeId = bounds.rangeId();

                SearchRow first = toSearchRow(bounds.first());
                SearchRow last = toSearchRow(bounds.last());

                ConcurrentNavigableMap<GridSearchRowPointer,GridH2Row> t = tree != null ? tree : treeForRead();

                curRange = doFind0(t, first, true, last, filter);

                if (!curRange.hasNext()) {
                    // We have to return empty range here.
                    GridH2RowRange emptyRange = new GridH2RowRange();

                    emptyRange.rangeId(curRangeId);

                    return emptyRange;
                }
            }
        }
    }

    /**
     * @return Snapshot for current thread if there is one.
     */
    protected ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> treeForRead() {
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
    protected Iterator<GridH2Row> doFind0(ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> t,
        @Nullable SearchRow first,
        boolean includeFirst,
        @Nullable SearchRow last,
        IndexingQueryFilter filter) {
        throw new UnsupportedOperationException();
    }

    /**
     * Iterator which filters by expiration time and predicate.
     */
    protected static class FilteringIterator extends GridFilteredIterator<GridH2Row> {
        /** */
        private final IgniteBiPredicate<Object, Object> fltr;

        /** */
        private final long time;

        /** Is value required for filtering predicate? */
        private final boolean isValRequired;

        /**
         * @param iter Iterator.
         * @param time Time for expired rows filtering.
         * @param qryFilter Filter.
         * @param spaceName Space name.
         */
        protected FilteringIterator(Iterator<GridH2Row> iter,
            long time,
            IndexingQueryFilter qryFilter,
            String spaceName) {
            super(iter);

            this.time = time;

            if (qryFilter != null) {
                this.fltr = qryFilter.forSpace(spaceName);

                this.isValRequired = qryFilter.isValueRequired();
            } else {
                this.fltr = null;

                this.isValRequired = false;
            }
        }

        /**
         * @param row Row.
         * @return If this row was accepted.
         */
        @SuppressWarnings("unchecked")
        @Override protected boolean accept(GridH2Row row) {
            if (row instanceof GridH2AbstractKeyValueRow) {
                if (((GridH2AbstractKeyValueRow) row).expirationTime() <= time)
                    return false;
            }

            if (fltr == null)
                return true;

            Object key = row.getValue(KEY_COL).getObject();
            Object val = isValRequired ? row.getValue(VAL_COL).getObject() : null;

            assert key != null;
            assert !isValRequired || val != null;

            return fltr.apply(key, val);
        }
    }
}
