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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRange;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeMap;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.util.DoneFuture;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyIterator;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowRangeBounds.rangeBounds;

/**
 * Base class for snapshotable tree indexes.
 */
@SuppressWarnings("ComparatorNotSerializable")
public class GridH2TreeIndex extends GridH2IndexBase implements Comparator<GridSearchRowPointer> {
    /** */
    protected final ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree;

    /** */
    private final Object msgTopic;

    /** */
    private final GridMessageListener msgLsnr;

    /** */
    private final GridKernalContext ctx;

    /**
     * Constructor with index initialization.
     *
     * @param name Index name.
     * @param tbl Table.
     * @param pk If this index is primary key.
     * @param colsList Index columns list.
     */
    @SuppressWarnings("unchecked")
    public GridH2TreeIndex(String name, GridH2Table tbl, boolean pk, List<IndexColumn> colsList) {
        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createUnique(false, false) : IndexType.createNonUnique(false, false, false));

        final GridH2RowDescriptor desc = tbl.rowDescriptor();

        tree = desc == null || desc.memory() == null ? new SnapTreeMap<GridSearchRowPointer, GridH2Row>(this) {
            @Override protected void afterNodeUpdate_nl(Node<GridSearchRowPointer, GridH2Row> node, Object val) {
                if (val != null)
                    node.key = (GridSearchRowPointer)val;
            }

            @Override protected Comparable<? super GridSearchRowPointer> comparable(Object key) {
                if (key instanceof ComparableRow)
                    return (Comparable<? super SearchRow>)key;

                return super.comparable(key);
            }
        } : new GridOffHeapSnapTreeMap<GridSearchRowPointer, GridH2Row>(desc, desc, desc.memory(), desc.guard(), this) {
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

        if (desc != null && desc.context() != null) {
            ctx = desc.context().kernalContext();

            msgTopic = new IgniteBiTuple<>(GridTopic.TOPIC_QUERY, tbl.identifier() + '.' + getName());

            msgLsnr = new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    ClusterNode node = ctx.discovery().node(nodeId);

                    if (node == null)
                        return;

                    if (msg instanceof GridH2IndexRangeRequest)
                        onIndexRangeRequest(node, (GridH2IndexRangeRequest)msg);
                    else if (msg instanceof GridH2IndexRangeResponse)
                        onIndexRangeResponse(node, (GridH2IndexRangeResponse)msg);
                }
            };

            ctx.io().addMessageListener(msgTopic, msgLsnr);
        }
        else {
            msgTopic = null;
            msgLsnr = null;
            ctx =  null;
        }
    }

    /**
     * @param node Requesting node.
     * @param msg Request message.
     */
    private void onIndexRangeRequest(ClusterNode node, GridH2IndexRangeRequest msg) {
        GridH2QueryContext qctx = GridH2QueryContext.get(ctx.localNodeId(), msg.originNodeId(), msg.queryId(), MAP);

        if (qctx == null) {
            // TODO respond NOT_FOUND

            return;
        }

        RangeSource src;

        if (msg.bounds() != null) {
            ConcurrentNavigableMap<GridSearchRowPointer,GridH2Row> snapshot0 = qctx.getSnapshot(idxId);

            assert snapshot0 != null;

            src = new RangeSource(msg.bounds(), snapshot0);
        }
        else
            src = qctx.getSource(idxId);

        List<GridH2RowRange> ranges = new ArrayList<>();

        int maxRows = qctx.pageSize();

        while (maxRows > 0) {
            GridH2RowRange range = src.next(maxRows);

            if (range == null)
                break;

            maxRows -= range.rows().size();
        }

        if (src.hasMoreRows()) {
            if (msg.bounds() != null)
                qctx.putSource(idxId, src);
        }
        else if (msg.bounds() == null)
            qctx.putSource(idxId, null);

        assert !ranges.isEmpty();

        GridH2IndexRangeResponse res = new GridH2IndexRangeResponse();

        res.originNodeId(msg.originNodeId());
        res.queryId(msg.queryId());
        res.ranges(ranges);

        // TODO send res
    }

    /**
     * @param node Responded node.
     * @param msg Response message.
     */
    private void onIndexRangeResponse(ClusterNode node, GridH2IndexRangeResponse msg) {
        GridH2QueryContext qctx = GridH2QueryContext.get(ctx.localNodeId(), msg.originNodeId(), msg.queryId(), MAP);

        if (qctx == null)
            return;

        Map<ClusterNode, RangeStream> streams = qctx.getStreams(idxId);

        if (streams == null)
            return;

        RangeStream stream = streams.get(node);

        assert stream != null;

        stream.onResponse(msg);
    }

    /** {@inheritDoc} */
    @Override public GridH2Table getTable() {
        return (GridH2Table)super.getTable();
    }

    /** {@inheritDoc} */
    @Override protected Object doTakeSnapshot() {
        return tree instanceof SnapTreeMap ?
            ((SnapTreeMap)tree).clone() :
            ((GridOffHeapSnapTreeMap)tree).clone();
    }

    /**
     * @return Snapshot for current thread if there is one.
     */
    private ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> treeForRead() {
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> res = threadLocalSnapshot();

        if (res == null)
            res = tree;

        return res;
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        assert threadLocalSnapshot() == null;

        if (tree instanceof AutoCloseable)
            U.closeQuiet((AutoCloseable)tree);

        if (msgLsnr != null)
            ctx.io().removeMessageListener(msgTopic, msgLsnr);
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(@Nullable Session ses) {
        IndexingQueryFilter f = filter();

        // Fast path if we don't need to perform any filtering.
        if (f == null || f.forSpace((getTable()).spaceName()) == null)
            return treeForRead().size();

        Iterator<GridH2Row> iter = doFind(null, false, null);

        long size = 0;

        while (iter.hasNext()) {
            iter.next();

            size++;
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return tree.size();
    }

    /** {@inheritDoc} */
    @Override public int compare(GridSearchRowPointer r1, GridSearchRowPointer r2) {
        // Second row here must be data row if first is a search row.
        return -compareRows(r2, r1);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB((indexType.isUnique() ? "Unique index '" : "Index '") + getName() + "' [");

        boolean first = true;

        for (IndexColumn col : getIndexColumns()) {
            if (first)
                first = false;
            else
                sb.a(", ");

            sb.a(col.getSQL());
        }

        sb.a(" ]");

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return getCostRangeIndex(masks, getRowCountApproximation(), filter, sortOrder);
    }

    /** {@inheritDoc} */
    @Override public boolean canFindNext() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, @Nullable SearchRow first, @Nullable SearchRow last) {
        return new GridH2Cursor(doFind(first, true, last));
    }

    /** {@inheritDoc} */
    @Override public Cursor findNext(Session ses, SearchRow higherThan, SearchRow last) {
        return new GridH2Cursor(doFind(higherThan, false, last));
    }

    /**
     * Finds row with key equal one in given search row.
     * WARNING!! Method call must be protected by {@link GridUnsafeGuard#begin()}
     * {@link GridUnsafeGuard#end()} block.
     *
     * @param row Search row.
     * @return Row.
     */
    public GridH2Row findOne(GridSearchRowPointer row) {
        return tree.get(row);
    }

    /**
     * Returns sub-tree bounded by given values.
     *
     * @param first Lower bound.
     * @param includeFirst Whether lower bound should be inclusive.
     * @param last Upper bound always inclusive.
     * @return Iterator over rows in given range.
     */
    @SuppressWarnings("unchecked")
    private Iterator<GridH2Row> doFind(@Nullable SearchRow first, boolean includeFirst, @Nullable SearchRow last) {
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> t = treeForRead();

        return doFind0(t, first, includeFirst, last);
    }

    /**
     * @param t Tree.
     * @param first Lower bound.
     * @param includeFirst Whether lower bound should be inclusive.
     * @param last Upper bound always inclusive.
     * @return Iterator over rows in given range.
     */
    private Iterator<GridH2Row> doFind0(
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> t,
        @Nullable SearchRow first,
        boolean includeFirst,
        @Nullable SearchRow last
    ) {
        includeFirst &= first != null;

        NavigableMap<GridSearchRowPointer, GridH2Row> range = subTree(t, comparable(first, includeFirst ? -1 : 1),
            comparable(last, 1));

        if (range == null)
            return new GridEmptyIterator<>();

        return filter(range.values().iterator());
    }

    /**
     * @param row Row.
     * @param bias Bias.
     * @return Comparable row.
     */
    private GridSearchRowPointer comparable(SearchRow row, int bias) {
        if (row == null)
            return null;

        if (bias == 0 && row instanceof GridH2Row)
            return (GridSearchRowPointer)row;

        return new ComparableRow(row, bias);
    }

    /**
     * Takes sup-map from given one.
     *
     * @param map Map.
     * @param first Lower bound.
     * @param last Upper bound.
     * @return Sub-map.
     */
    @SuppressWarnings({"IfMayBeConditional", "TypeMayBeWeakened"})
    private NavigableMap<GridSearchRowPointer, GridH2Row> subTree(NavigableMap<GridSearchRowPointer, GridH2Row> map,
        @Nullable GridSearchRowPointer first, @Nullable GridSearchRowPointer last) {
        // We take exclusive bounds because it is possible that one search row will be equal to multiple key rows
        // in tree and we must return them all.
        if (first == null) {
            if (last == null)
                return map;
            else
                return map.headMap(last, false);
        }
        else {
            if (last == null)
                return map.tailMap(first, false);
            else {
                if (compare(first, last) > 0)
                    return null;

                return map.subMap(first, false, last, false);
            }
        }
    }

    /**
     * Gets iterator over all rows in this index.
     *
     * @return Rows iterator.
     */
    Iterator<GridH2Row> rows() {
        return doFind(null, false, null);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree = treeForRead();

        Iterator<GridH2Row> iter = filter(first ? tree.values().iterator() : tree.descendingMap().values().iterator());

        GridSearchRowPointer res = null;

        if (iter.hasNext())
            res = iter.next();

        return new SingleRowCursor((Row)res);
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        return tree.put(row, row);
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        return tree.remove(comparable(row, 0));
    }

    /**
     * Comparable row with bias. Will be used for queries to have correct bounds (in case of multicolumn index
     * and query on few first columns we will multiple equal entries in tree).
     */
    private class ComparableRow implements GridSearchRowPointer, Comparable<SearchRow> {
        /** */
        private final SearchRow row;

        /** */
        private final int bias;

        /**
         * @param row Row.
         * @param bias Bias.
         */
        private ComparableRow(SearchRow row, int bias) {
            this.row = row;
            this.bias = bias;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(SearchRow o) {
            int res = compareRows(o, row);

            if (res == 0)
                return bias;

            return -res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            throw new IllegalStateException("Should never be called.");
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return row.getColumnCount();
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            return row.getValue(idx);
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            row.setValue(idx, v);
        }

        /** {@inheritDoc} */
        @Override public void setKeyAndVersion(SearchRow old) {
            row.setKeyAndVersion(old);
        }

        /** {@inheritDoc} */
        @Override public int getVersion() {
            return row.getVersion();
        }

        /** {@inheritDoc} */
        @Override public void setKey(long key) {
            row.setKey(key);
        }

        /** {@inheritDoc} */
        @Override public long getKey() {
            return row.getKey();
        }

        /** {@inheritDoc} */
        @Override public int getMemory() {
            return row.getMemory();
        }

        /** {@inheritDoc} */
        @Override public long pointer() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public void incrementRefCount() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public void decrementRefCount() {
            throw new IllegalStateException();
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2TreeIndex rebuild() throws InterruptedException {
        IndexColumn[] cols = getIndexColumns();

        GridH2TreeIndex idx = new GridH2TreeIndex(getName(), getTable(),
            getIndexType().isUnique(), F.asList(cols));

        Thread thread = Thread.currentThread();

        long i = 0;

        for (GridH2Row row : tree.values()) {
            // Check for interruptions every 1000 iterations.
            if (++i % 1000 == 0 && thread.isInterrupted())
                throw new InterruptedException();

            idx.tree.put(row, row);
        }

        return idx;
    }

    /** {@inheritDoc} */
    @Override public int getPreferedLookupBatchSize() {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        if (qctx == null || !qctx.distributedJoins())
            return 0;

        return qctx.pageSize();
    }

    /** {@inheritDoc} */
    @Override public List<Future<Cursor>> findBatched(TableFilter filter, List<SearchRow> firstLastPairs) {
        if (F.isEmpty(firstLastPairs))
            throw new IllegalStateException("Empty batch.");

        GridCacheContext<?,?> cctx = getTable().rowDescriptor().context();
        GridH2QueryContext qctx = GridH2QueryContext.get();
        Map<ClusterNode, RangeStream> rangeStreams = new HashMap<>();
        Collection<ClusterNode> broadcastNodes = null;

        assert qctx != null;

        IndexColumn affCol = getTable().getAffinityKeyColumn();
        int affColId = affCol.column.getColumnId();

        List<Future<Cursor>> res = new ArrayList<>(firstLastPairs.size() >>> 1);

        // Compose messages and create respective futures.
        for (int i = 0; i < firstLastPairs.size(); i += 2) {
            SearchRow firstRow = firstLastPairs.get(i);
            SearchRow lastRow = firstLastPairs.get(i + 1);

            // Create messages.
            GridH2RowMessage first = toSearchRowMessage(firstRow);
            GridH2RowMessage last = toSearchRowMessage(lastRow);

            Value affKeyFirst = firstRow.getValue(affColId);
            Value affKeyLast = lastRow.getValue(affColId);

            Collection<ClusterNode> nodes;
            Future<Cursor> fut;

            if (affKeyFirst != null && equal(affKeyFirst, affKeyLast)) {
                if (affKeyFirst.getType() == Value.NULL) // Affinity key is explicit null, we will not find anything.
                    nodes = null;
                else
                    nodes = F.asList(rangeNode(cctx, qctx, affKeyFirst));
            }
            else {
                // Affinity key is not provided or is not the same in upper and lower bounds, we have to broadcast.
                if (broadcastNodes == null)
                    broadcastNodes = broadcastNodes(qctx, cctx);

                nodes = broadcastNodes;
            }

            if (nodes != null) {
                assert !F.isEmpty(nodes);

                final int rangeId = i >>> 1;

                // Range containing upper and lower bounds.
                GridH2RowRangeBounds rangeBounds = rangeBounds(rangeId, first, last);

                // Add range to every message of every participating node.
                for (ClusterNode node : nodes) {
                    assert node != null;

                    RangeStream stream = rangeStreams.get(node);

                    if (stream == null) {
                        stream = new RangeStream(node);

                        stream.req = createRequest(qctx);

                        stream.req.bounds(new ArrayList<GridH2RowRangeBounds>());

                        rangeStreams.put(node, stream);
                    }

                    stream.req.bounds().add(rangeBounds);
                }

                fut = new DoneFuture<>(nodes.size() == 1 ?
                    new SimpleCursor(rangeId, rangeStreams.get(F.first(nodes))) :
                    new MergeCursor(rangeId, nodes, rangeStreams));
            }
            else
                fut = null;

            res.add(fut);
        }

        qctx.putStreams(idxId, rangeStreams);

        // Start streaming.
        for (RangeStream stream : rangeStreams.values())
            stream.start();

        return res;
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
     * @return Index range request.
     */
    private static GridH2IndexRangeRequest createRequest(GridH2QueryContext qctx) {
        GridH2IndexRangeRequest req = new GridH2IndexRangeRequest();

        req.originNodeId(qctx.originNodeId());
        req.queryId(qctx.queryId());

        return req;
    }

    /**
     * @param qctx Query context.
     * @param cctx Cache context.
     * @return Collection of nodes for broadcasting.
     */
    private Collection<ClusterNode> broadcastNodes(GridH2QueryContext qctx, GridCacheContext<?,?> cctx) {
        Map<UUID,int[]> partMap = qctx.partitionsMap();

        Collection<ClusterNode> res;

        if (partMap == null)
            res = CU.affinityNodes(cctx, qctx.topologyVersion());
        else {
            res = new ArrayList<>(partMap.size());

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
     * @param affKey Affinity key.
     * @return Cluster nodes or {@code null} if affinity key is a null value.
     */
    private ClusterNode rangeNode(GridCacheContext<?,?> cctx, GridH2QueryContext qctx, Value affKey) {
        assert affKey != null;

        Object affKeyObj = affKey.getObject();

        if (affKeyObj == null)
            return null; // Affinity key is explicit null value, we will not find anything.

        ClusterNode node;

        if (qctx.partitionsMap() != null) {
            // If we have explicit partitions map, we have to use it to calculate affinity node.
            UUID nodeId = qctx.nodeForPartition(cctx.affinity().partition(affKeyObj), cctx);

            node = cctx.discovery().node(nodeId);
        }
        else // Get primary node for current topology version.
            node = cctx.affinity().primary(affKeyObj, qctx.topologyVersion());

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

        List<GridH2ValueMessage> vals = new ArrayList<>(row.getColumnCount());

        for (int i = 0; i < vals.size(); i++) {
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

        Value[] vals = new Value[getTable().getColumns().length];

        for (int i = 0; i < indexColumns.length; i++) {
            try {
                vals[indexColumns[i].column.getColumnId()] = msg.values().get(i).value(ctx);
            }
            catch (IgniteCheckedException e) {
                throw new CacheException(e);
            }
        }

        return new GridH2Row(vals);
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

        List<GridH2ValueMessage> vals = msg.values();

        Value[] vals0 = new Value[vals.size()];

        for (int i = 0; i < vals0.length; i++) {
            try {
                vals0[i] = vals.get(i).value(ctx);
            }
            catch (IgniteCheckedException e) {
                throw new CacheException(e);
            }
        }

        return new GridH2Row(vals0);
    }

    /**
     * Simple cursor from a single node.
     */
    private static class SimpleCursor implements Cursor {
        /** */
        final int rangeId;

        /** */
        RangeStream stream;

        /**
         * @param rangeId Range ID.
         * @param stream Stream.
         */
        private SimpleCursor(int rangeId, RangeStream stream) {
            assert stream != null;

            this.rangeId = rangeId;
            this.stream = stream;
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
    private class MergeCursor implements Cursor, Comparator<RangeStream> {
        /** */
        final int rangeId;

        /** */
        final RangeStream[] streams;

        /** */
        boolean first = true;

        /**
         * @param rangeId Range ID.
         * @param nodes Remote nodes.
         * @param rangeStreams Range streams.
         */
        private MergeCursor(int rangeId, Collection<ClusterNode> nodes, Map<ClusterNode,RangeStream> rangeStreams) {
            this.rangeId = rangeId;

            streams = new RangeStream[nodes.size()];

            int i = 0;

            for (ClusterNode node : nodes)
                streams[i++] = rangeStreams.get(node);
        }

        /** {@inheritDoc} */
        @Override public int compare(RangeStream o1, RangeStream o2) {
            if (o1 == o2)
                return 0;

            // Nulls are at the end of array.
            if (o1 == null)
                return 1;

            if (o2 == null)
                return -1;

            return compareRows(o1.get(rangeId), o2.get(rangeId));
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (streams[0] == null)
                return false;

            assert streams.length > 1;

            if (streams[1] == null) {
                // Only one stream left.
                assert !first;

                RangeStream s = streams[0];

                if (!s.next(rangeId))
                    streams[0] = null;
            }
            else if (first) {
                // Pull from all the streams at the first time and sort them.
                for (int i = 0; i < streams.length; i++) {
                    if (!streams[i].next(rangeId))
                        streams[i] = null;
                }

                Arrays.sort(streams, this);

                first = false;
            }
            else {
                // Get next one on current min and bubble up.
                if (!streams[0].next(rangeId))
                    streams[0] = null;

                for (int i = 0, last = streams.length - 1; i < last; i++) {
                    int next = i + 1;

                    if (compare(streams[i], streams[next]) > 0) {
                        RangeStream tmp = streams[i];
                        streams[i] = streams[next];
                        streams[next] = tmp;
                    }
                }
            }

            return streams[0] != null;
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return streams[0].get(rangeId);
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
     * Per node range stream.
     */
    private class RangeStream {
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
         * @param node Node.
         */
        RangeStream(ClusterNode node) {
            this.node = node;
        }

        /**
         * Start streaming.
         */
        private void start() {
            remainingRanges = req.bounds().size();

            // TODO send req
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

            for (int attempt = 0; attempt < 40; attempt++) {
                GridH2IndexRangeResponse res;

                try {
                    res = respQueue.poll(500, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedException(e);
                }

                if (res != null) {
                    switch (res.status()) {
                        case GridH2IndexRangeResponse.STATUS_OK:
                            List<GridH2RowRange> ranges0 = res.ranges();

                            remainingRanges -= ranges0.size();

                            if (ranges0.get(ranges0.size() - 1).isPartial())
                                remainingRanges++;

                            if (remainingRanges > 0) {
                                // TODO request next
                            }

                            req = null;

                            return res;

                        case GridH2IndexRangeResponse.STATUS_NOT_FOUND:
                            if (req == null) // We already received the first response.
                                throw new GridH2RetryException("Failure on remote node.");

                            try {
                                U.sleep(10 * attempt);
                            }
                            catch (IgniteInterruptedCheckedException e) {
                                throw new IgniteInterruptedException(e.getMessage());
                            }

                            // TODO resend req

                            break;

                        case GridH2IndexRangeResponse.STATUS_ERROR:
                            throw new CacheException(res.error());

                        default:
                            assert false;
                    }
                }

                if (!ctx.discovery().alive(node))
                    throw new GridH2RetryException("Node left.");
            }

            throw new GridH2RetryException("Attempts exceeded.");
        }

        /**
         * @param rangeId Requested range ID.
         * @return {@code true} If row for the requested range was found.
         */
        private boolean next(final int rangeId) {
            for (;;) {
                if (rangeId != cursorRangeId) {
                    assert rangeId < cursorRangeId;

                    return false;
                }

                if (cursor.next())
                    return true;

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

                final Iterator<GridH2RowMessage> it = range.rows().iterator();

                if (it.hasNext()) {
                    cursor = new GridH2Cursor(new Iterator<Row>() {
                        @Override public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override public Row next() {
                            return toRow(it.next());
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    });
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
        int curRangeId;

        /** */
        Iterator<GridH2Row> curRange = emptyIterator();

        /** */
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree;

        /**
         * @param bounds Bounds.
         * @param tree Snapshot.
         */
        RangeSource(
            Iterable<GridH2RowRangeBounds> bounds,
            ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree
        ) {
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
            for (;;) {
                if (curRange.hasNext()) {
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

                curRange = doFind0(tree, first, true, last);

                if (!curRange.hasNext()) {
                    // We have to return empty range.
                    GridH2RowRange nextRange = new GridH2RowRange();

                    nextRange.rangeId(curRangeId);

                    return nextRange;
                }
            }
        }
    }
}