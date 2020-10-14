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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexValueCursor;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRow;
import org.apache.ignite.cache.query.index.sorted.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyImpl;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.join.CursorIteratorWrapper;
import org.apache.ignite.internal.processors.query.h2.opt.join.DistributedJoinContext;
import org.apache.ignite.internal.processors.query.h2.opt.join.DistributedLookupBatch;
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
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexCondition;
import org.h2.index.IndexLookupBatch;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;

import static java.util.Collections.singletonList;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_ERROR;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_NOT_FOUND;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_OK;
import static org.apache.ignite.internal.processors.tracing.SpanTags.ERROR;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_IDX_RANGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_TABLE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_RESP;
import static org.h2.result.Row.MEMORY_CALCULATE;

/**
 * H2 Index over {@link BPlusTree}.
 */
@SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
public class H2TreeIndex extends H2TreeIndexBase {

    private InlineIndex queryIndex;

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Cache context. */
    private GridCacheContext<?, ?> cctx;

    /** Table name. */
    private String tblName;

    /** */
    private final CIX2<ClusterNode,Message> locNodeHnd = new CIX2<ClusterNode,Message>() {
        @Override public void applyx(ClusterNode locNode, Message msg) {
            onMessage0(locNode.id(), msg);
        }
    };

    public H2TreeIndex(InlineIndex queryIndex, GridH2Table tbl, IndexColumn[] cols, boolean pk) {
        super(tbl, queryIndex.name(), cols,
            pk ? IndexType.createPrimaryKey(false, false) :
                IndexType.createNonUnique(false, false, false));

        cctx = tbl.cacheContext();
        ctx = cctx.kernalContext();

        this.tblName = tbl.getName();

        this.queryIndex = queryIndex;
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return queryIndex.inlineSize();
    }

    /**
     * Check if index exists in store.
     *
     * @return {@code True} if exists.
     */
    // TODO: what is it?
    public boolean rebuildRequired() {
        return queryIndex.isCreated();
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        return queryIndex.segmentsCount();
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        assert lower == null || lower instanceof H2Row : lower;
        assert upper == null || upper instanceof H2Row : upper;

        try {
            ThreadLocalSessionHolder.setSession(ses);

            int idxColsLen = indexColumns.length;

            Object[] left = lower == null ? null : new Object[idxColsLen];
            Object[] right = upper == null ? null : new Object[idxColsLen];

            if (lower != null || upper != null) {
                for (int i = 0; i < idxColsLen; ++i) {
                    int colId = indexColumns[i].column.getColumnId();

                    Value vl = lower != null ? lower.getValue(colId) : null;
                    Value vu = upper != null ? upper.getValue(colId) : null;

                    if (left != null)
                        left[i] = vl != null ? vl.getObject() : null;

                    if (right != null)
                        right[i] = vu != null ? vu.getObject() : null;

                    // TODO: what to do with condition null, null for PK?
                }
            }

            QueryContext qctx = ses != null ? H2Utils.context(ses) : null;

            IndexKey key_left = left == null ? null : new IndexKeyImpl(left);
            IndexKey key_right = right == null ? null : new IndexKeyImpl(right);

            GridCursor<IndexRow> cursor = queryIndex.find(key_left, key_right, qctx.segment(), qctx.filter());

            GridCursor<H2Row> h2cursor = new IndexValueCursor<>(cursor, this::mapIndexRow);

            return new H2Cursor(h2cursor);

        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            ThreadLocalSessionHolder.removeSession();
        }
    }

    private H2Row mapIndexRow(IndexRow row) {
        return new H2CacheRow(rowDescriptor(), row.getCacheDataRow());
    }

    /** {@inheritDoc} */
    @Override public H2CacheRow put(H2CacheRow row) {
        try {
            cctx.shared().database().checkpointLockIsHeldByThread();

            CacheDataRow cacheRow = queryIndex.put(row);

            return new H2CacheRow(rowDescriptor(), cacheRow);

        }
        catch (Throwable t) {
            ctx.failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw DbException.convert(t);
        }
    }

    /**
     * @param row Row to validate.
     * @throws IgniteSQLException on error (field type mismatch).
     */
    private void validateRowFields(H2CacheRow row) {
        for (int col : columnIds)
            row.getValue(col);
    }

    /** {@inheritDoc} */
    @Override public boolean putx(H2CacheRow row) {
        validateRowFields(row);

        try {
            assert cctx.shared().database().checkpointLockIsHeldByThread();

            // TODO: remove
            if (queryIndex == null)
                return false;

            return queryIndex.putx(row);
        }
        catch (Throwable t) {
            ctx.failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw DbException.convert(t);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        assert row instanceof H2Row : row;

        try {
            assert cctx.shared().database().checkpointLockIsHeldByThread();

            // TODO: how to remove SearchRow?
//            return queryIndex.removex((H2Row) row);

            return false;


        }
        catch (Throwable t) {
            ctx.failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw DbException.convert(t);
        }
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        try {
            QueryContext qctx = H2Utils.context(ses);

            return queryIndex.count(qctx.segment(), qctx.filter());
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean b) {
        try {
            QueryContext qctx = H2Utils.context(ses);

            GridCursor<IndexRow> cursor = queryIndex.findFirstOrLast(b, qctx.segment(), qctx.filter());

            return new H2Cursor(new IndexValueCursor<>(cursor, this::mapIndexRow));
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Determines if provided row can be treated as expired at the current moment.
     *
     * @param row row to check.
     * @throws NullPointerException if provided row is {@code null}.
     */
    private static boolean isExpired(@NotNull H2Row row) {
        return row.expireTime() > 0 && row.expireTime() <= U.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean rmvIdx) {
        queryIndex.destroy(!rmvIdx);
    }

    /** {@inheritDoc} */
    @Override public IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        QueryContext qctx = H2Utils.context(filters[filter].getSession());

        if (qctx == null || qctx.distributedJoinContext() == null || !getTable().isPartitioned())
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

        return new DistributedLookupBatch(this, cctx, ucast, affColId);
    }

    /**
     * @param nodes Nodes.
     * @param msg Message.
     */
    public void send(Collection<ClusterNode> nodes, Message msg) {
        boolean res = getTable().rowDescriptor().indexing().send("msgTopic", // TODO
            -1,
            nodes,
            msg,
            null,
            locNodeHnd,
            GridIoPolicy.IDX_POOL,
            false
        );

        if (!res)
            throw H2Utils.retryException("Failed to send message to nodes: " + nodes);
    }

    /**
     * @param nodeId Source node ID.
     * @param msg Message.
     */
    private void onMessage0(UUID nodeId, Object msg) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            return;

        try {
            if (msg instanceof GridH2IndexRangeRequest)
                onIndexRangeRequest(node, (GridH2IndexRangeRequest)msg);
            else if (msg instanceof GridH2IndexRangeResponse)
                onIndexRangeResponse(node, (GridH2IndexRangeResponse)msg);
        }
        catch (Throwable th) {
//            U.error(log, "Failed to handle message[nodeId=" + nodeId + ", msg=" + msg + "]", th);

            if (th instanceof Error)
                throw th;
        }
    }

    /**
     * @param node Requesting node.
     * @param msg Request message.
     */
    // TODO
    private void onIndexRangeRequest(final ClusterNode node, final GridH2IndexRangeRequest msg) {
        // We don't use try with resources on purpose - the catch block must also be executed in the context of this span.
        TraceSurroundings trace = MTC.support(ctx.tracing().create(SQL_IDX_RANGE_REQ, MTC.span()));

        Span span = MTC.span();

        try {
//            span.addTag(SQL_IDX, () -> idxName);
            span.addTag(SQL_TABLE, () -> tblName);

            GridH2IndexRangeResponse res = new GridH2IndexRangeResponse();

            res.originNodeId(msg.originNodeId());
            res.queryId(msg.queryId());
            res.originSegmentId(msg.originSegmentId());
            res.segment(msg.segment());
            res.batchLookupId(msg.batchLookupId());

            QueryContext qctx = null;
//                qryCtxRegistry.getShared(
//                msg.originNodeId(),
//                msg.queryId(),
//                msg.originSegmentId()
//            );

            if (qctx == null)
                res.status(STATUS_NOT_FOUND);
            else {
                DistributedJoinContext joinCtx = qctx.distributedJoinContext();

                assert joinCtx != null;

                try {
                    RangeSource src;

                    if (msg.bounds() != null) {
                        // This is the first request containing all the search rows.
                        assert !msg.bounds().isEmpty() : "empty bounds";

                        src = new RangeSource(this, msg.bounds(), msg.segment(), null);
                    }
                    else {
                        // This is request to fetch next portion of data.
                        src = joinCtx.getSource(node.id(), msg.segment(), msg.batchLookupId());

                        assert src != null;
                    }

                    List<GridH2RowRange> ranges = new ArrayList<>();

                    int maxRows = joinCtx.pageSize();

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
                            joinCtx.putSource(node.id(), msg.segment(), msg.batchLookupId(), src);
                    }
                    else if (msg.bounds() == null) {
                        // Drop saved source.
                        joinCtx.putSource(node.id(), msg.segment(), msg.batchLookupId(), null);
                    }

                    res.ranges(ranges);
                    res.status(STATUS_OK);

                    span.addTag(SQL_IDX_RANGE_ROWS, () ->
                        Integer.toString(ranges.stream().mapToInt(GridH2RowRange::rowsSize).sum()));
                }
                catch (Throwable th) {
                    span.addTag(ERROR, th::getMessage);

//                    U.error(log, "Failed to process request: " + msg, th);

                    res.error(th.getClass() + ": " + th.getMessage());
                    res.status(STATUS_ERROR);
                }
            }

            send(singletonList(node), res);
        }
        catch (Throwable th) {
            span.addTag(ERROR, th::getMessage);

            throw th;
        }
        finally {
            if (trace != null)
                trace.close();
        }
    }

    /**
     * @param node Responded node.
     * @param msg Response message.
     */
    // TODO
    private void onIndexRangeResponse(ClusterNode node, GridH2IndexRangeResponse msg) {
        try (TraceSurroundings ignored = MTC.support(ctx.tracing().create(SQL_IDX_RANGE_RESP, MTC.span()))) {
            QueryContext qctx = null;
//                qryCtxRegistry.getShared(
//                msg.originNodeId(),
//                msg.queryId(),
//                msg.originSegmentId()
//            );

            if (qctx == null)
                return;

            DistributedJoinContext joinCtx = qctx.distributedJoinContext();

            assert joinCtx != null;

            Map<SegmentKey, RangeStream> streams = joinCtx.getStreams(msg.batchLookupId());

            if (streams == null)
                return;

            RangeStream stream = streams.get(new SegmentKey(node, msg.segment()));

            assert stream != null;

            stream.onResponse(msg);
        }
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
    public Iterator<H2Row> findForSegment(GridH2RowRangeBounds bounds, int segment,
        BPlusTree.TreeRowClosure<H2Row, H2Row> filter) {
        SearchRow first = toSearchRow(bounds.first());
        SearchRow last = toSearchRow(bounds.last());

//        IgniteTree t = segments[segment];
        IgniteTree t = null;

        try {
            GridCursor<H2Row> range = ((BPlusTree)t).find(first, last, filter, null);

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
     * @param msg Row message.
     * @return Search row.
     */
    private SearchRow toSearchRow(GridH2RowMessage msg) {
        if (msg == null)
            return null;

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
     * Returns number of elements in the tree by scanning pages of the bottom (leaf) level.
     *
     * @return Number of elements in the tree.
     * @throws IgniteCheckedException If failed.
     */
    public long size() throws IgniteCheckedException {
        return queryIndex.totalCount();
    }
}
