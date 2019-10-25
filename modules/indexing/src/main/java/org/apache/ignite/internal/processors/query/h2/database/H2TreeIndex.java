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
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
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
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexCondition;
import org.h2.index.IndexLookupBatch;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.metric.IoStatisticsType.SORTED_INDEX;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_ERROR;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_NOT_FOUND;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_OK;
import static org.h2.result.Row.MEMORY_CALCULATE;

/**
 * H2 Index over {@link BPlusTree}.
 */
@SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
public class H2TreeIndex extends H2TreeIndexBase {
    /** */
    private final H2Tree[] segments;

    /** */
    private final List<InlineIndexHelper> inlineIdxs;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** Table name. */
    private final String tblName;

    /** */
    private final boolean pk;

    /** */
    private final boolean affinityKey;

    /** */
    private final String idxName;

    /** Tree name. */
    private final String treeName;

    /** */
    private final IgniteLogger log;

    /** */
    private final Object msgTopic;

    /** */
    private final GridMessageListener msgLsnr;

    /** */
    private final CIX2<ClusterNode,Message> locNodeHnd = new CIX2<ClusterNode,Message>() {
        @Override public void applyx(ClusterNode locNode, Message msg) {
            onMessage0(locNode.id(), msg);
        }
    };

    /** Query context registry. */
    private final QueryContextRegistry qryCtxRegistry;

    /**
     * @param cctx Cache context.
     * @param rowCache Row cache.
     * @param table Table.
     * @param idxName Index name.
     * @param pk Primary key.
     * @param affinityKey {@code true} for affinity key.
     * @param unwrappedColsList Unwrapped index columns for complex types.
     * @param wrappedColsList Index columns as is.
     * @param inlineSize Inline size.
     * @param segmentsCnt Count of tree segments.
     * @param qryCtxRegistry Query context registry.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("MapReplaceableByEnumMap")
    public H2TreeIndex(
        GridCacheContext<?, ?> cctx,
        @Nullable H2RowCache rowCache,
        GridH2Table table,
        String idxName,
        boolean pk,
        boolean affinityKey,
        List<IndexColumn> unwrappedColsList,
        List<IndexColumn> wrappedColsList,
        int inlineSize,
        int segmentsCnt,
        QueryContextRegistry qryCtxRegistry
    ) throws IgniteCheckedException {
        super(table);

        assert segmentsCnt > 0 : segmentsCnt;

        ctx = cctx.kernalContext();
        log = ctx.log(getClass());

        this.cctx = cctx;

        this.pk = pk;
        this.affinityKey = affinityKey;

        tblName = table.getName();

        this.idxName = idxName;

        this.table = table;
        this.qryCtxRegistry = qryCtxRegistry;

        GridQueryTypeDescriptor typeDesc = table.rowDescriptor().type();

        int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

        treeName = BPlusTree.treeName((table.rowDescriptor() == null ? "" : typeId + "_") + idxName, "H2Tree");

        IndexColumnsInfo unwrappedColsInfo = new IndexColumnsInfo(unwrappedColsList, inlineSize);

        IndexColumnsInfo wrappedColsInfo = new IndexColumnsInfo(wrappedColsList, inlineSize);

        IndexColumn[] cols;

        assert cctx.affinityNode();

        segments = new H2Tree[segmentsCnt];

        IgniteCacheDatabaseSharedManager db = cctx.shared().database();

        AtomicInteger maxCalculatedInlineSize = new AtomicInteger();

        IoStatisticsHolder stats = new IoStatisticsHolderIndex(
            SORTED_INDEX,
            cctx.name(),
            idxName,
            ctx.metric());

        for (int i = 0; i < segments.length; i++) {
            db.checkpointReadLock();

            try {
                RootPage page = getMetaPage(i);

                segments[i] = new H2Tree(
                    cctx,
                    table,
                    treeName,
                    idxName,
                    tblName,
                    table.cacheName(),
                    cctx.offheap().reuseListForIndex(treeName),
                    cctx.groupId(),
                    cctx.group().name(),
                    cctx.dataRegion().pageMemory(),
                    cctx.shared().wal(),
                    cctx.offheap().globalRemoveId(),
                    page.pageId().pageId(),
                    page.isAllocated(),
                    unwrappedColsInfo,
                    wrappedColsInfo,
                    maxCalculatedInlineSize,
                    pk,
                    affinityKey,
                    cctx.mvccEnabled(),
                    rowCache,
                    cctx.kernalContext().failure(),
                    log,
                    stats
                );
            }
            finally {
                db.checkpointReadUnlock();
            }
        }

        boolean useUnwrappedCols = segments[0].unwrappedPk();

        IndexColumnsInfo colsInfo = useUnwrappedCols ? unwrappedColsInfo : wrappedColsInfo;

        cols = colsInfo.cols();

        inlineIdxs = colsInfo.inlineIdx();

        IndexColumn.mapColumns(cols, table);

        initBaseIndex(table, 0, idxName, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        // Initialize distributed joins.
        msgTopic = new IgniteBiTuple<>(GridTopic.TOPIC_QUERY, table.identifierString() + '.' + getName());

        msgLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                GridSpinBusyLock l = table.rowDescriptor().indexing().busyLock();

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
        return segments[0].inlineSize();
    }

    /**
     * Check if index exists in store.
     *
     * @return {@code True} if exists.
     */
    public boolean rebuildRequired() {
        assert segments != null;

        for (int i = 0; i < segments.length; i++) {
            try {
                H2Tree segment = segments[i];

                if (segment.created())
                    return true;
            }
            catch (Exception e) {
                throw new IgniteException("Failed to check index tree root page existence [cacheName=" + cctx.name() +
                    ", tblName=" + tblName + ", idxName=" + idxName + ", segment=" + i + ']');
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override protected void warnCantBeInlined(IndexColumn col) {
        String idxType = pk ? "PRIMARY KEY" : affinityKey ? "AFFINITY KEY (implicit)" : "SECONDARY";

        U.warn(log, "Column cannot be inlined into the index because it's type doesn't support inlining, " +
            "index access may be slow due to additional page reads (change column type if possible) " +
            "[cacheName=" + cctx.name() +
            ", tableName=" + tblName +
            ", idxName=" + idxName +
            ", idxType=" + idxType +
            ", colName=" + col.columnName +
            ", columnType=" + InlineIndexHelper.nameTypeBycode(col.column.getType()) + ']'
        );
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        return segments.length;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        assert lower == null || lower instanceof H2Row : lower;
        assert upper == null || upper instanceof H2Row : upper;

        try {
            int seg = threadLocalSegment();

            H2Tree tree = treeForRead(seg);

            // If it is known that only one row will be returned an optimization is employed
            if (isSingleRowLookup(lower, upper, tree)) {
                H2Row row = tree.findOne((H2Row)lower, filter(qryCtxRegistry.getThreadLocal()), null);

                if (row == null || isExpired(row))
                    return GridH2Cursor.EMPTY;

                return new SingleRowCursor(row);
            }
            else {
                return new H2Cursor(tree.find((H2Row)lower,
                    (H2Row)upper, filter(qryCtxRegistry.getThreadLocal()), null));
            }
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** */
    private boolean isSingleRowLookup(SearchRow lower, SearchRow upper, H2Tree tree) {
        return !cctx.mvccEnabled() && indexType.isPrimaryKey() && lower != null && upper != null &&
            tree.compareRows((H2Row)lower, (H2Row)upper) == 0 && hasAllIndexColumns(lower);
    }

    /** */
    private boolean hasAllIndexColumns(SearchRow searchRow) {
        for (Column c : columns) {
            // Java null means that column is not specified in a search row, for SQL NULL a special constant is used
            if (searchRow.getValue(c.getColumnId()) == null)
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public H2CacheRow put(H2CacheRow row) {
        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(cctx, row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return (H2CacheRow)tree.put(row);
        }
        catch (Throwable t) {
            ctx.failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw DbException.convert(t);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putx(H2CacheRow row) {
        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(cctx, row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.putx(row);
        }
        catch (Throwable t) {
            ctx.failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw DbException.convert(t);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        assert row instanceof H2Row : row;

        try {
            InlineIndexHelper.setCurrentInlineIndexes(inlineIdxs);

            int seg = segmentForRow(cctx, row);

            H2Tree tree = treeForRead(seg);

            assert cctx.shared().database().checkpointLockIsHeldByThread();

            return tree.removex((H2Row)row);
        }
        catch (Throwable t) {
            ctx.failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw DbException.convert(t);
        }
        finally {
            InlineIndexHelper.clearCurrentInlineIndexes();
        }
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        try {
            int seg = threadLocalSegment();

            H2Tree tree = treeForRead(seg);

            QueryContext qctx = qryCtxRegistry.getThreadLocal();

            return tree.size(filter(qctx));
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean b) {
        try {
            H2Tree tree = treeForRead(threadLocalSegment());
            QueryContext qctx = qryCtxRegistry.getThreadLocal();

            H2Row found = b ? tree.findFirst(filter(qctx)) : tree.findLast(filter(qctx));

            if (found == null || isExpired(found))
                return GridH2Cursor.EMPTY;

            return new SingleRowCursor(found);
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
        try {
            if (cctx.affinityNode() && rmvIdx) {
                assert cctx.shared().database().checkpointLockIsHeldByThread();

                for (int i = 0; i < segments.length; i++) {
                    H2Tree tree = segments[i];

                    tree.destroy();

                    dropMetaPage(i);
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            if (msgLsnr != null)
                ctx.io().removeMessageListener(msgTopic, msgLsnr);
        }
    }

    /**
     * @param segment Segment Id.
     * @return Snapshot for requested segment if there is one.
     */
    private H2Tree treeForRead(int segment) {
        return segments[segment];
    }

    /**
     * @param qctx Query context.
     * @return Row filter.
     */
    private BPlusTree.TreeRowClosure<H2Row, H2Row> filter(QueryContext qctx) {
        if (qctx == null) {
            assert !cctx.mvccEnabled();

            return null;
        }

        IndexingQueryFilter f = qctx.filter();
        IndexingQueryCacheFilter p = f == null ? null : f.forCache(getTable().cacheName());
        MvccSnapshot v = qctx.mvccSnapshot();

        assert !cctx.mvccEnabled() || v != null;

        if(p == null && v == null)
            return null;

        return new H2TreeFilterClosure(p, v, cctx, log);
    }

    /**
     * @param segIdx Segment index.
     * @return RootPage for meta page.
     * @throws IgniteCheckedException If failed.
     */
    private RootPage getMetaPage(int segIdx) throws IgniteCheckedException {
        return cctx.offheap().rootPageForIndex(cctx.cacheId(), treeName, segIdx);
    }

    /**
     * @param segIdx Segment index.
     * @throws IgniteCheckedException If failed.
     */
    private void dropMetaPage(int segIdx) throws IgniteCheckedException {
        cctx.offheap().dropRootPageForIndex(cctx.cacheId(), treeName, segIdx);
    }

    /** {@inheritDoc} */
    @Override public void refreshColumnIds() {
        super.refreshColumnIds();

        if (inlineIdxs == null)
            return;

        List<InlineIndexHelper> inlineHelpers = getAvailableInlineColumns(indexColumns);

        assert inlineIdxs.size() == inlineHelpers.size();

        for (int pos = 0; pos < inlineHelpers.size(); ++pos)
            inlineIdxs.set(pos, inlineHelpers.get(pos));
    }

    /** {@inheritDoc} */
    @Override public long totalRowCount(IndexingQueryCacheFilter partsFilter) {
        try {
            H2TreeFilterClosure filter = partsFilter == null ? null :
                new H2TreeFilterClosure(partsFilter, null, cctx, log);

            long cnt = 0;

            for (int seg = 0; seg < segmentsCount(); seg++)
                cnt += segments[seg].size(filter);

            return cnt;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        QueryContext qctx = qryCtxRegistry.getThreadLocal();

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

        GridCacheContext<?, ?> cctx = getTable().rowDescriptor().context();

        return new DistributedLookupBatch(this, cctx, queryContextRegistry(), ucast, affColId);
    }

    /**
     * @param nodes Nodes.
     * @param msg Message.
     */
    public void send(Collection<ClusterNode> nodes, Message msg) {
        boolean res = getTable().rowDescriptor().indexing().send(msgTopic,
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

                    src = new RangeSource(this, msg.bounds(), msg.segment(), filter(qctx));
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

        IgniteTree t = treeForRead(segment);

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
     *
     */
    @SuppressWarnings({"PublicInnerClass", "AssignmentOrReturnOfFieldWithMutableType"})
    public class IndexColumnsInfo {
        /** */
        private final int inlineSize;

        /** */
        private final IndexColumn[] cols;

        /** */
        private final List<InlineIndexHelper> inlineIdx;

        /**
         * @param colsList Index columns list
         * @param cfgInlineSize Inline size from cache config.
         */
        @SuppressWarnings("ZeroLengthArrayAllocation")
        public IndexColumnsInfo(List<IndexColumn> colsList, int cfgInlineSize) {
            cols = colsList.toArray(new IndexColumn[0]);

            inlineIdx = getAvailableInlineColumns(cols);
            inlineSize = computeInlineSize(inlineIdx, cfgInlineSize, cctx.config());
        }

        /**
         * @return Inline size.
         */
        public int inlineSize() {
            return inlineSize;
        }

        /**
         * @return Index columns.
         */
        public IndexColumn[] cols() {
            return cols;
        }

        /**
         * @return Inline indexes.
         */
        public List<InlineIndexHelper> inlineIdx() {
            return inlineIdx;
        }
    }
}
