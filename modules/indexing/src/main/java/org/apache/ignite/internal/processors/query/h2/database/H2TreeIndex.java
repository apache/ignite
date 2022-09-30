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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexSearchRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexValueCursor;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridQueryRowDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContextRegistry;
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
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
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
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_ERROR;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_NOT_FOUND;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_OK;
import static org.apache.ignite.internal.processors.tracing.SpanTags.ERROR;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_IDX;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_IDX_RANGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_TABLE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_RESP;
import static org.h2.result.Row.MEMORY_CALCULATE;

/**
 * H2 Index over {@link BPlusTree}.
 */
@SuppressWarnings({"unchecked"})
public class H2TreeIndex extends H2TreeIndexBase {
    /** Underlying Ignite index. */
    private final InlineIndexImpl queryIndex;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** Table name. */
    private final String tblName;

    /** Index name. */
    private final String idxName;

    /** */
    private final IgniteLogger log;

    /** */
    private final Object msgTopic;

    /** Query context registry. */
    private final QueryContextRegistry qryCtxRegistry;

    /** */
    private final GridMessageListener msgLsnr;

    /** */
    private final CIX2<ClusterNode, Message> locNodeHnd = new CIX2<ClusterNode, Message>() {
        @Override public void applyx(ClusterNode locNode, Message msg) {
            onMessage0(locNode.id(), msg);
        }
    };

    /** */
    public H2TreeIndex(InlineIndexImpl queryIndex, GridH2Table tbl, IndexColumn[] cols, boolean pk, IgniteLogger log) {
        super(tbl, queryIndex.name(), cols,
            pk ? IndexType.createPrimaryKey(false, false) :
                IndexType.createNonUnique(false, false, false));

        cctx = tbl.cacheContext();
        ctx = cctx.kernalContext();

        tblName = tbl.getName();
        idxName = queryIndex.name();

        this.log = log;

        this.queryIndex = queryIndex;

        qryCtxRegistry = ((IgniteH2Indexing)(ctx.query().getIndexing())).queryContextRegistry();

        // Initialize distributed joins.
        msgTopic = new IgniteBiTuple<>(GridTopic.TOPIC_QUERY, tbl.identifierString() + '.' + getName());

        msgLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                GridSpinBusyLock l = tbl.tableDescriptor().indexing().busyLock();

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

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return queryIndex.inlineSize();
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        return queryIndex.segmentsCount();
    }

    /** {@inheritDoc} */
    @Override public long totalRowCount(IndexingQueryCacheFilter partsFilter) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        assert lower == null || lower instanceof H2Row : lower;
        assert upper == null || upper instanceof H2Row : upper;

        try {
            T2<IndexRow, IndexRow> key = prepareIndexKeys(lower, upper);

            QueryContext qctx = ses != null ? H2Utils.context(ses) : null;

            GridCursor<IndexRow> cursor = queryIndex.find(key.get1(), key.get2(), true, true, segment(qctx), idxQryContext(qctx));

            GridCursor<H2Row> h2cursor = new IndexValueCursor<>(cursor, this::mapIndexRow);

            return new H2Cursor(h2cursor);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** */
    private IndexQueryContext idxQryContext(QueryContext qctx) {
        assert qctx != null || !cctx.mvccEnabled();

        if (qctx == null)
            return null;

        return new IndexQueryContext(qctx.filter(), null, qctx.mvccSnapshot());
    }

    /** */
    private T2<IndexRow, IndexRow> prepareIndexKeys(SearchRow lower, SearchRow upper) {
        InlineIndexRowHandler rowHnd = queryIndex.segment(0).rowHandler();

        return new T2<>(prepareIndexKey(lower, rowHnd), prepareIndexKey(upper, rowHnd));
    }

    /** */
    private IndexRow prepareIndexKey(SearchRow row, InlineIndexRowHandler rowHnd) {
        if (row == null)
            return null;

        else if (row instanceof H2CacheRow)
            return new IndexRowImpl(rowHnd, (CacheDataRow)row, getCachedKeys((H2CacheRow)row));
        else
            return preparePlainIndexKey(row, rowHnd);
    }

    /** Extract cached values of a row to skip the double work of getting index keys from Ignite cache. */
    private IndexKey[] getCachedKeys(H2CacheRow row) {
        IndexKey[] cached = new IndexKey[columnIds.length];

        for (int i = 0; i < columnIds.length; ++i) {
            Object key = row.getCached(columnIds[i]);
            if (key == null)
                break;

            cached[i] = IndexKeyFactory.wrap(
                key, columns[i].getType(), cctx.cacheObjectContext(), queryIndex.keyTypeSettings());
        }

        return cached;
    }

    /** */
    private IndexRow preparePlainIndexKey(SearchRow row, InlineIndexRowHandler rowHnd) {
        int idxColsLen = indexColumns.length;

        IndexKey[] keys = row == null ? null : new IndexKey[idxColsLen];

        for (int i = 0; i < idxColsLen; ++i) {
            int colId = indexColumns[i].column.getColumnId();

            Value v = row.getValue(colId);

            IndexKeyType colType = rowHnd.indexKeyDefinitions().get(i).idxType();

            if (v == null)
                break;

            // If it's possible to convert search row to index value type - do it. In this case converted value
            // can be used for the inline search.
            // Otherwise, we can use search row as index find bound only if types have the same comparison rules.
            // If types have different comparison rules (for example, '2' > '10' for strings and 2 < 10 for integers)
            // best we can do here is leave search bound empty. In this case index scan by bounds can be extended to
            // full index scan and rows will be filtered out by original condition on H2 level.
            if (colType.code() != v.getType()) {
                if (Value.getHigherOrder(colType.code(), v.getType()) == colType.code())
                    v = v.convertTo(colType.code());
                else {
                    InlineIndexKeyType colKeyType = InlineIndexKeyTypeRegistry.get(colType, queryIndex.keyTypeSettings());

                    IndexKey idxKey = IndexKeyFactory.wrap(v.getObject(), v.getType(), cctx.cacheObjectContext(),
                        queryIndex.keyTypeSettings());

                    if (colKeyType.isComparableTo(idxKey)) {
                        keys[i] = idxKey;

                        continue;
                    }

                    LT.warn(log, "Provided value can't be used as index search bound due to column data type " +
                        "mismatch. This can lead to full index scans instead of range index scans. [index=" +
                        idxName + ", colType=" + colType + ", valType=" + IndexKeyType.forCode(v.getType()) + ']');

                    break;
                }
            }

            keys[i] = IndexKeyFactory.wrap(
                v.getObject(), v.getType(), cctx.cacheObjectContext(), queryIndex.keyTypeSettings());
        }

        return new IndexSearchRowImpl(keys, rowHnd);
    }

    /** */
    private H2Row mapIndexRow(IndexRow row) {
        if (row == null)
            return null;

        return new H2CacheRow(rowDescriptor(), row.cacheDataRow());
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        try {
            QueryContext qctx = H2Utils.context(ses);

            return queryIndex.count(segment(qctx), idxQryContext(qctx));
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean b) {
        try {
            QueryContext qctx = H2Utils.context(ses);

            IndexQueryContext qryCtx = idxQryContext(qctx);

            GridCursor<IndexRow> cursor = b ?
                queryIndex.findFirst(segment(qctx), qryCtx)
                : queryIndex.findLast(segment(qctx), qryCtx);

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
    @Override public void destroy() {
        try {
            super.destroy();
        }
        finally {
            if (msgLsnr != null)
                ctx.io().removeMessageListener(msgTopic, msgLsnr);
        }
    }

    /** {@inheritDoc} */
    @Override public H2CacheRow put(H2CacheRow row) {
        throw new IllegalStateException("Must not be invoked.");
    }

    /** {@inheritDoc} */
    @Override public boolean putx(H2CacheRow row) {
        throw new IllegalStateException("Must not be invoked.");
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        throw new IllegalStateException("Must not be invoked.");
    }

    /** {@inheritDoc} */
    @Override public IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        QueryContext qctx = H2Utils.context(filters[filter].getSession());

        if (qctx == null || qctx.distributedJoinContext() == null || !getTable().isPartitioned())
            return null;

        IndexColumn affCol = getTable().getAffinityKeyColumn();
        GridQueryRowDescriptor desc = getTable().rowDescriptor();

        int affColId = -1;
        boolean ucast = false;

        if (affCol != null) {
            affColId = affCol.column.getColumnId();
            int[] masks = filters[filter].getMasks();

            if (masks != null) {
                ucast = (masks[affColId] & IndexCondition.EQUALITY) != 0 ||
                    (masks[QueryUtils.KEY_COL] & IndexCondition.EQUALITY) != 0 ||
                    (masks[desc.getAlternativeColumnId(QueryUtils.KEY_COL)] & IndexCondition.EQUALITY) != 0;
            }
        }

        return new DistributedLookupBatch(this, cctx, ucast, affColId);
    }

    /**
     * @param nodes Nodes.
     * @param msg Message.
     */
    public void send(Collection<ClusterNode> nodes, Message msg) {
        boolean res = getTable().tableDescriptor().indexing().send(msgTopic,
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
            U.error(log, "Failed to handle message[nodeId=" + nodeId + ", msg=" + msg + "]", th);

            if (th instanceof Error)
                throw th;
        }
    }

    /**
     * @param node Requesting node.
     * @param msg Request message.
     */
    private void onIndexRangeRequest(final ClusterNode node, final GridH2IndexRangeRequest msg) {
        // We don't use try with resources on purpose - the catch block must also be executed in the context of this span.
        TraceSurroundings trace = MTC.support(ctx.tracing().create(SQL_IDX_RANGE_REQ, MTC.span()));

        Span span = MTC.span();

        try {
            span.addTag(SQL_IDX, () -> idxName);
            span.addTag(SQL_TABLE, () -> tblName);

            GridH2IndexRangeResponse res = new GridH2IndexRangeResponse();

            res.originNodeId(msg.originNodeId());
            res.queryId(msg.queryId());
            res.originSegmentId(msg.originSegmentId());
            res.segment(msg.segment());
            res.batchLookupId(msg.batchLookupId());

            QueryContext qctx = qryCtxRegistry.getShared(
                msg.originNodeId(),
                msg.queryId(),
                msg.originSegmentId()
            );

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

                        src = new RangeSource(this, msg.bounds(), msg.segment(), idxQryContext(qctx));
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

                    U.error(log, "Failed to process request: " + msg, th);

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
    private void onIndexRangeResponse(ClusterNode node, GridH2IndexRangeResponse msg) {
        try (TraceSurroundings ignored = MTC.support(ctx.tracing().create(SQL_IDX_RANGE_RESP, MTC.span()))) {
            QueryContext qctx = qryCtxRegistry.getShared(
                msg.originNodeId(),
                msg.queryId(),
                msg.originSegmentId()
            );

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
     * @param qryCtx Index query context.
     * @return Iterator.
     */
    public Iterator<H2Row> findForSegment(GridH2RowRangeBounds bounds, int segment, IndexQueryContext qryCtx) {
        SearchRow lower = toSearchRow(bounds.first());
        SearchRow upper = toSearchRow(bounds.last());

        T2<IndexRow, IndexRow> key = prepareIndexKeys(lower, upper);

        try {
            GridCursor<IndexRow> range = queryIndex.find(key.get1(), key.get2(), true, true, segment, qryCtx);

            if (range == null)
                range = IndexValueCursor.EMPTY;

            GridCursor<H2Row> h2cursor = new IndexValueCursor<>(range, this::mapIndexRow);

            H2Cursor cur = new H2Cursor(h2cursor);

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

    /**
     * Creates a new index that is an exact copy of this index.
     *
     * @return New index.
     */
    public H2TreeIndex createCopy(InlineIndexImpl inlineIndex, SortedIndexDefinition idxDef) throws IgniteCheckedException {
        return new H2TreeIndex(inlineIndex, tbl, indexColumns, idxDef.primary(), log);
    }

    /**
     * @return Index's id.
     */
    public UUID indexId() {
        return queryIndex.id();
    }

    /**
     * @return Index.
     */
    public InlineIndexImpl index() {
        return queryIndex;
    }

    /** {@inheritDoc} */
    @Override public <T extends Index> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(queryIndex))
            return clazz.cast(queryIndex);

        return super.unwrap(clazz);
    }
}
