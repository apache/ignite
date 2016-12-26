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

import java.lang.reflect.Field;
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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRange;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeMap;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.index.Cursor;
import org.h2.index.IndexCondition;
import org.h2.index.IndexLookupBatch;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.util.DoneFuture;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.KEY_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_ERROR;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_NOT_FOUND;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse.STATUS_OK;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds.rangeBounds;

/**
 * Stripped snapshotable tree index
 */
public class GridH2StripedTreeIndex extends GridH2AbstractTreeIndex {
    /** */
    private final ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row>[] segments;

    /** */
    private final boolean snapshotEnabled;

    /** */
    private final GridH2RowDescriptor desc;

    /**
     * Constructor with index initialization.
     *
     * @param name Index name.
     * @param tbl Table.
     * @param pk If this index is primary key.
     * @param colsList Index columns list.
     */
    @SuppressWarnings("unchecked")
    public GridH2StripedTreeIndex(String name, GridH2Table tbl, boolean pk, List<IndexColumn> colsList,
        int parallelizmLevel) {
        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        desc = tbl.rowDescriptor();

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createUnique(false, false) : IndexType.createNonUnique(false, false, false));

        segments = new ConcurrentNavigableMap[parallelizmLevel];

        final GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (desc == null || desc.memory() == null) {
            snapshotEnabled = desc == null || desc.snapshotableIndex();

            if (snapshotEnabled) {
                for (int i = 0; i < parallelizmLevel; i++) {
                    segments[i] = new SnapTreeMap<GridSearchRowPointer, GridH2Row>(this) {
                        @Override
                        protected void afterNodeUpdate_nl(Node<GridSearchRowPointer, GridH2Row> node, Object val) {
                            if (val != null)
                                node.key = (GridSearchRowPointer)val;
                        }

                        @Override protected Comparable<? super GridSearchRowPointer> comparable(Object key) {
                            if (key instanceof ComparableRow)
                                return (Comparable<? super SearchRow>)key;

                            return super.comparable(key);
                        }
                    };
                }
            }
            else {
                for (int i = 0; i < parallelizmLevel; i++) {
                    segments[i] = new ConcurrentSkipListMap<>(
                        new Comparator<GridSearchRowPointer>() {
                            @Override public int compare(GridSearchRowPointer o1, GridSearchRowPointer o2) {
                                if (o1 instanceof ComparableRow)
                                    return ((ComparableRow)o1).compareTo(o2);

                                if (o2 instanceof ComparableRow)
                                    return -((ComparableRow)o2).compareTo(o1);

                                return compareRows(o1, o2);
                            }
                        }
                    );
                }
            }
        }
        else {
            assert desc.snapshotableIndex() : desc;

            snapshotEnabled = true;

            for (int i = 0; i < parallelizmLevel; i++) {
                segments[i] = new GridOffHeapSnapTreeMap<GridSearchRowPointer, GridH2Row>(desc, desc, desc.memory(), desc.guard(), this) {
                    @Override protected void afterNodeUpdate_nl(long node, GridH2Row val) {
                        final long oldKey = keyPtr(node);

                        if (val != null) {
                            key(node, val);

                            guard.finalizeLater(new Runnable() {
                                @Override public void run() {
                                    desc.createPointer(oldKey).decrementRefCount();
                                }
                            });
                        }
                    }

                    @Override protected Comparable<? super GridSearchRowPointer> comparable(Object key) {
                        if (key instanceof ComparableRow)
                            return (Comparable<? super SearchRow>)key;

                        return super.comparable(key);
                    }
                };
            }
        }

        initDistributedJoinMessaging(tbl);
    }

    /** {@inheritDoc} */
    @Override protected Object doTakeSnapshot() {
        assert snapshotEnabled;

        return tree() instanceof SnapTreeMap ?
            ((SnapTreeMap)tree()).clone() :
            ((GridOffHeapSnapTreeMap)tree()).clone();
    }

    /** {@inheritDoc} */
    protected ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> treeForRead() {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        int segment = qctx != null ? qctx.segment() : 0;

        return treeForRead(segment);
    }

    /**
     * @return Index segment snapshot for current thread if there is one.
     */
    private ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> treeForRead(int seg) {
        if (!isSnapshotEnabled())
            return segments[seg];

        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> res = threadLocalSnapshot();

        if (res == null)
            return segments[seg];

        return res;
    }

    /** {@inheritDoc} */
    protected boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }

    /** {@inheritDoc} */
    public GridH2Row findOne(GridSearchRowPointer row) {
        return tree().get(row);
    }

    /** */
    private ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree() {
        GridH2QueryContext ctx = GridH2QueryContext.get();

        assert ctx != null;

        int seg = ctx.segment();

        return segments[seg];
    }

    /** {@inheritDoc} */
    @Override public GridH2StripedTreeIndex rebuild() throws InterruptedException {
        IndexColumn[] cols = getIndexColumns();

        GridH2StripedTreeIndex idx = new GridH2StripedTreeIndex(getName(), getTable(),
            getIndexType().isUnique(), F.asList(cols), segments.length);

        Thread thread = Thread.currentThread();

        long j = 0;

        for (int i = 0; i < segments.length; i++) {
            for (GridH2Row row : segments[i].values()) {
                // Check for interruptions every 1000 iterations.
                if ((++j & 1023) == 0 && thread.isInterrupted())
                    throw new InterruptedException();

                idx.put(row);
            }
        }

        return idx;
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        int seg = segment(row);

        return segments[seg].put(row, row);
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        GridSearchRowPointer comparable = comparable(row, 0);

        //TODO: avoid iteration
        GridH2Row res;

        for (int i = 0; i < segments.length; i++) {
            res = segments[i].remove(comparable);

            if (res != null)
                return res;
        }

        return null;
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

        GridCacheContext<?, ?> cctx = getTable().rowDescriptor().context();

        return new DistributedLookupBatch(cctx, ucast, affColId);
    }

    /**
     * @param qctx Query context.
     * @param batchLookupId Batch lookup ID.
     * @return Index range request.
     */
    protected static GridH2IndexRangeRequest createRequest(GridH2QueryContext qctx, int batchLookupId, int segmentId) {
        GridH2IndexRangeRequest req = new GridH2IndexRangeRequest();

        req.originNodeId(qctx.originNodeId());
        req.queryId(qctx.queryId());
        req.originSegmentId(qctx.segment());
        req.segment(segmentId);
        req.batchLookupId(batchLookupId);

        return req;
    }

    /** */
    private static Field KEY_FIELD;

    /** */
    static {
        try {
            KEY_FIELD = GridH2AbstractKeyValueRow.class.getDeclaredField("key");
            KEY_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            KEY_FIELD = null;
        }
    }

    /**
     * @param partition Parttition idx.
     * @return index currentSegment Id for given key
     */
    private int segment(int partition) {
        return partition % segments.length;
    }

    /**
     * @param row
     * @return index currentSegment Id for given row
     */
    private int segment(GridH2Row row) {
        assert row != null;

        CacheObject key;

        if (desc != null && desc.context() != null) {
            GridCacheContext<?, ?> ctx = desc.context();

            assert ctx != null;

            if (row instanceof GridH2AbstractKeyValueRow && KEY_FIELD != null) {
                try {
                    Object o = KEY_FIELD.get(row);

                    if (o instanceof CacheObject)
                        key = (CacheObject)o;
                    else
                        key = ctx.toCacheKeyObject(o);

                }
                catch (IllegalAccessException e) {
                    throw new IllegalStateException(e);
                }
            }
            else
                key = ctx.toCacheKeyObject(row.getValue(0));

            return segment(ctx.affinity().partition(key));
        }
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        assert threadLocalSnapshot() == null;

        for (int i = 0; i < segments.length; i++) {
            if (segments[i] instanceof AutoCloseable)
                U.closeQuiet((AutoCloseable)segments[i]);
        }

        super.destroy();
    }

    /**
     * @param node Requesting node.
     * @param msg Request message.
     */
    protected void onIndexRangeRequest(final ClusterNode node, final GridH2IndexRangeRequest msg) {
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
                    ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> snapshot0 = qctx.getSnapshot(idxId);

                    assert !msg.bounds().isEmpty() : "empty bounds";

                    src = new RangeSource(msg.bounds(), msg.segment(), snapshot0, qctx.filter());
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

                if (src.hasMoreRows()) {
                    // Save source for future fetches.
                    if (msg.bounds() != null)
                        qctx.putSource(node.id(), msg.segment(), msg.batchLookupId(), src);
                }
                else if (msg.bounds() == null) {
                    // Drop saved source.
                    qctx.putSource(node.id(), msg.segment(), msg.batchLookupId(), null);
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
    protected void onIndexRangeResponse(ClusterNode node, GridH2IndexRangeResponse msg) {
        GridH2QueryContext qctx = GridH2QueryContext.get(kernalContext().localNodeId(),
            msg.originNodeId(), msg.queryId(),
            msg.originSegmentId(),
            MAP);

        if (qctx == null)
            return;

        Map<RangeKey, RangeStream> streams = qctx.getStreams(msg.batchLookupId());

        if (streams == null)
            return;

        RangeStream stream = streams.get(new RangeKey(node.id(), msg.segment()));

        assert stream != null;

        stream.onResponse(msg);
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
        BroadcastCursor(int rangeId, Collection<ClusterNode> nodes, Map<RangeKey, RangeStream> rangeStreams) {

            this.rangeId = rangeId;

            streams = new RangeStream[nodes.size() * segments.length];

            int i = 0;

            for (ClusterNode node : nodes) {
                for (int seg = 0; seg < segments.length; seg++) {
                    RangeStream stream = rangeStreams.get(new RangeKey(node.id(), seg));

                    assert stream != null;

                    streams[i++] = stream;
                }
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
        final GridCacheContext<?, ?> cctx;

        /** */
        final boolean ucast;

        /** */
        final int affColId;

        /** */
        GridH2QueryContext qctx;

        /** */
        int batchLookupId;

        /** */
        Map<RangeKey, RangeStream> rangeStreams = Collections.emptyMap();

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
        private DistributedLookupBatch(GridCacheContext<?, ?> cctx, boolean ucast, int affColId) {
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
                    broadcastNodes = broadcastNodes(qctx, cctx); //TODO: replace with list of segments

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
                for (int seg = 0; seg < segments.length; seg++) {
                    ClusterNode node = nodes.get(i);
                    assert node != null;

                    RangeKey rangeKey = new RangeKey(node.id(), seg);

                    RangeStream stream = rangeStreams.get(rangeKey);

                    List<GridH2RowRangeBounds> bounds;

                    if (stream == null) {
                        stream = new RangeStream(qctx, node);

                        stream.req = createRequest(qctx, batchLookupId, seg);
                        stream.req.bounds(bounds = new ArrayList<>());

                        rangeStreams.put(rangeKey, stream);
                    }
                    else
                        bounds = stream.req.bounds();

                    bounds.add(rangeBounds);

                    // If at least one node will have a full batch then we are ok.
                    if (bounds.size() >= qctx.pageSize())
                        batchFull = true;
                }
            }

            fut = new DoneFuture<>(new BroadcastCursor(rangeId, nodes, rangeStreams));

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

    /** */
    private class RangeKey {
        /** */
        UUID nodeId;
        /** */
        int segmentId;

        public RangeKey(UUID nodeId, int segmentId) {
            this.nodeId = nodeId;
            this.segmentId = segmentId;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            RangeKey key = (RangeKey)o;

            if (segmentId != key.segmentId)
                return false;
            return nodeId != null ? nodeId.equals(key.nodeId) : key.nodeId == null;

        }

        @Override public int hashCode() {
            int result = nodeId != null ? nodeId.hashCode() : 0;
            result = 31 * result + segmentId;
            return result;
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
        private final int segment;

        /** */
        final IndexingQueryFilter filter;

        /**
         * @param bounds Bounds.
         * @param tree Snapshot.
         * @param filter Filter.
         */
        RangeSource(
            Iterable<GridH2RowRangeBounds> bounds,
            int segment,
            ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree,
            IndexingQueryFilter filter
        ) {
            this.segment = segment;
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

            for (; ; ) {
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

                ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> t = tree != null ? tree : treeForRead(segment);

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

            for (int attempt = 0; ; attempt++) {
                if (qctx.isCleared())
                    throw new GridH2RetryException("Query is cancelled.");

                if (kernalContext().isStopping())
                    throw new GridH2RetryException("Stopping node.");

                GridH2IndexRangeResponse res;

                try {
                    res = respQueue.poll(500, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
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
            for (; ; ) {
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
}
