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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.join.CursorIteratorWrapper;
import org.apache.ignite.internal.processors.query.h2.opt.join.DistributedLookupBatch;
import org.apache.ignite.internal.processors.query.h2.opt.join.CollocationModel;
import org.apache.ignite.internal.processors.query.h2.opt.join.RangeSource;
import org.apache.ignite.internal.processors.query.h2.opt.join.RangeStream;
import org.apache.ignite.internal.processors.query.h2.opt.join.SegmentKey;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRange;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.IndexCondition;
import org.h2.index.IndexLookupBatch;
import org.h2.index.ViewIndex;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;

import javax.cache.CacheException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.query.h2.opt.join.DistributedJoinMode.OFF;
import static org.apache.ignite.internal.processors.query.h2.opt.join.CollocationModel.buildCollocationModel;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.KEY_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.PREPARE;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_ERROR;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_NOT_FOUND;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_OK;
import static org.h2.result.Row.MEMORY_CALCULATE;

/**
 * Index base.
 */
public abstract class GridH2IndexBase extends BaseIndex {
    /** */
    public static final Object EXPLICIT_NULL = new Object();

    /** */
    private Object msgTopic;

    /** */
    private GridMessageListener msgLsnr;

    /** */
    private IgniteLogger log;

    /** */
    private final CIX2<ClusterNode,Message> locNodeHnd = new CIX2<ClusterNode,Message>() {
        @Override public void applyx(ClusterNode clusterNode, Message msg) {
            onMessage0(clusterNode.id(), msg);
        }
    };

    protected GridCacheContext<?, ?> ctx;

    /**
     * @param tbl Table.
     */
    @SuppressWarnings("MapReplaceableByEnumMap")
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
     * Puts row.
     *
     * @param row Row.
     * @return {@code True} if existing row row has been replaced.
     */
    public abstract boolean putx(GridH2Row row);

    /**
     * Remove row from index.
     *
     * @param row Row.
     * @return Removed row.
     */
    public abstract GridH2Row remove(SearchRow row);

    /**
     * Removes row from index.
     *
     * @param row Row.
     * @return {@code True} if row has been removed.
     */
    public abstract boolean removex(SearchRow row);

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
            return CollocationModel.MULTIPLIER_COLLOCATED;

        // We have to clear this cache because normally sub-query plan cost does not depend on anything
        // other than index condition masks and sort order, but in our case it can depend on order
        // of previous table filters.
        clearViewIndexCache(ses);

        assert filters != null;

        CollocationModel c = buildCollocationModel(qctx, ses.getSubQueryInfo(), filters, filter, false);

        return c.calculateMultiplier();
    }

    /** {@inheritDoc} */
    @Override public GridH2Table getTable() {
        return (GridH2Table)super.getTable();
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

        return new DistributedLookupBatch(this, cctx, ucast, affColId);
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
    public void send(Collection<ClusterNode> nodes, Message msg) {
        if (!getTable().rowDescriptor().indexing().send(msgTopic,
            -1,
            nodes,
            msg,
            null,
            locNodeHnd,
            GridIoPolicy.IDX_POOL,
            false))
            throw H2Utils.retryException("Failed to send message to nodes: " + nodes);
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

                    src = new RangeSource(this, msg.bounds(), msg.segment(), filter(qctx));
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
     * @param qctx Query context.
     * @return Row filter.
     */
    protected BPlusTree.TreeRowClosure<GridH2SearchRow, GridH2Row> filter(GridH2QueryContext qctx) {
        throw new UnsupportedOperationException();
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
    public GridH2RowMessage toSearchRowMessage(SearchRow row) {
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
     * @return Index segments count.
     */
    public abstract int segmentsCount();

    /**
     * @param partition Partition idx.
     * @return Segment ID for given key
     */
    public int segmentForPartition(int partition){
        return segmentsCount() == 1 ? 0 : (partition % segmentsCount());
    }

    /**
     * @param row Table row.
     * @return Segment ID for given row.
     */
    @SuppressWarnings("IfMayBeConditional")
    protected int segmentForRow(SearchRow row) {
        assert row != null;

        if (segmentsCount() == 1 || ctx == null)
            return 0;

        CacheObject key;

        final Value keyColValue = row.getValue(KEY_COL);

        assert keyColValue != null;

        final Object o = keyColValue.getObject();

        if (o instanceof CacheObject)
            key = (CacheObject)o;
        else
            key = ctx.toCacheKeyObject(o);

        return segmentForPartition(ctx.affinity().partition(key));
    }

    /**
     * Find rows for the segments (distributed joins).
     *
     * @param bounds Bounds.
     * @param segment Segment.
     * @param filter Filter.
     * @return Iterator.
     */
    @SuppressWarnings("unchecked")
    public Iterator<GridH2Row> findForSegment(GridH2RowRangeBounds bounds, int segment,
        BPlusTree.TreeRowClosure<GridH2SearchRow, GridH2Row> filter) {
        SearchRow first = toSearchRow(bounds.first());
        SearchRow last = toSearchRow(bounds.last());

        IgniteTree t = treeForRead(segment);

        try {
            GridCursor<GridH2Row> range = ((BPlusTree)t).find(first, last, filter, null);

            if (range == null)
                range = H2Utils.EMPTY_CURSOR;

            H2Cursor cur = new H2Cursor(range);

            return new CursorIteratorWrapper(cur);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
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
     * Re-assign column ids after removal of column(s).
     */
    public void refreshColumnIds() {
        assert columnIds.length == columns.length;

        for (int pos = 0; pos < columnIds.length; ++pos)
            columnIds[pos] = columns[pos].getColumnId();
    }
}
