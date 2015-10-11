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

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.Future;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2IndexRangeResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2RowMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeMap;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
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
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor.QUERY_POOL;

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

    /** */
    private final ConcurrentMap<Long,BlockingQueue<T2<ClusterNode,GridH2IndexRangeResponse>>> msgQueues =
        new ConcurrentHashMap8<>();

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

            String schemaName = tbl.getSchema().getName();
            String tblName = tbl.getName();
            String idxName = getName();

            msgTopic = new IgniteBiTuple<>(GridTopic.TOPIC_QUERY, schemaName + '.' + tblName + '.' + idxName);

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

    private void onIndexRangeRequest(ClusterNode node, GridH2IndexRangeRequest msg) {

    }

    private void onIndexRangeResponse(ClusterNode node, GridH2IndexRangeResponse msg) {
        BlockingQueue<T2<ClusterNode, GridH2IndexRangeResponse>> q = msgQueues.get(msg.id());

        if (q != null)
            q.add(new T2<>(node, msg));
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
    @Override public void close(Session ses) {
        assert threadLocalSnapshot() == null;

        if (tree instanceof Closeable)
            U.closeQuiet((Closeable)tree);

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
        return true;
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
    private Iterator<GridH2Row> doFind(@Nullable SearchRow first, boolean includeFirst,
        @Nullable SearchRow last) {
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> t = treeForRead();

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
        return 0; // TODO
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public List<Future<Cursor>> findBatched(TableFilter filter, List<SearchRow> firstLastPairs) {
        List<Range> ranges = toRanges(firstLastPairs);



        // TODO Topology version + explicit partitions

        return null;
    }

    /**
     * @param firstLastPairs First last pairs.
     * @return Ranges list.
     */
    private List<Range> toRanges(List<SearchRow> firstLastPairs) {
        if (F.isEmpty(firstLastPairs))
            throw new IllegalStateException();

        IndexColumn affCol = getTable().getAffinityKeyColumn();

        List<Range> ranges = new ArrayList<>(firstLastPairs.size() >>> 1);

        for (int i = 0; i < firstLastPairs.size(); i += 2) {
            Value affKey = firstLastPairs.get(i).getValue(affCol.column.getColumnId());

            GridH2RowMessage first = toRowMessage(firstLastPairs.get(i));
            GridH2RowMessage last = toRowMessage(firstLastPairs.get(i + 1));

            ranges.add(new Range(ranges.size(), affKey == null ? null : affKey.getObject(), first, last));
        }

        return ranges;
    }

    /**
     * @param affKey Affinity key.
     * @param topVer Topology version.
     * @return Cluster nodes
     */
    private Collection<ClusterNode> rangeNodes(Value affKey, AffinityTopologyVersion topVer) {
        GridCacheContext<?,?> cctx = getTable().rowDescriptor().context();

        Collection<ClusterNode> nodes;

        if (affKey != null) {
            if (affKey.getType() == Value.NULL) {
                // TODO
            }

            ClusterNode node = cctx.affinity().primary(affKey.getObject(), topVer);

            if (node == null)
                throw new CacheException();

            nodes = Collections.singleton(node);
        }
        else {
            nodes = CU.affinityNodes(cctx, topVer);

            if (F.isEmpty(nodes))
                throw new CacheException();
        }

        return nodes;
    }

    private IgniteInternalFuture<GridH2IndexRangeResponse> send(
        ClusterNode node,
        GridH2IndexRangeRequest req
    ) {
        try {
            ctx.io().send(node, msgTopic, req, QUERY_POOL);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }

        return null;
    }

    /**
     * @param row Row.
     * @return Row message.
     */
    private GridH2RowMessage toRowMessage(SearchRow row) {
        List<GridH2ValueMessage> vals = new ArrayList<>();

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
     * Streams rows for ranges from remote nodes.
     */
    private class RangeStream {
        /** */
        final Queue<RangeData> ranges;

        private RangeStream(int rangesNum) {
            ranges = new ArrayDeque<>(rangesNum);

            for (int i = 0; i < rangesNum; i++)
                ranges.add(new RangeData(i));
        }
    }

    /**
     *
     */
    private static class Range {
        /** */
        final int id;

        /** */
        final Object affKey;

        /** */
        final GridH2RowMessage first;

        /** */
        final GridH2RowMessage last;

        /**
         * @param id Range ID.
         * @param affKey Affinity key.
         * @param first Lower bound.
         * @param last Upper bound.
         */
        private Range(int id, Object affKey, GridH2RowMessage first, GridH2RowMessage last) {
            this.id = id;
            this.affKey = affKey;
            this.first = first;
            this.last = last;
        }
    }

    /**
     * Range data.
     */
    private class RangeData {
        /** */
        final int id;

        /** */
        List<Row> rows = Collections.emptyList();

        /**
         * @param id ID.
         */
        private RangeData(int id) {
            this.id = id;
        }

        /**
         * @param other Rows.
         */
        void merge(List<Row> other) {
            if (F.isEmpty(other))
                return;

            if (rows.isEmpty()) {
                rows = other;

                return;
            }

            List<Row> res = new ArrayList<>(rows.size() + other.size());

            Cursor c1 = new GridH2Cursor(rows.iterator());
            Cursor c2 = new GridH2Cursor(other.iterator());

            if (!c1.next() || !c2.next())
                throw new IllegalStateException();

            Cursor c;

            do {
                c = compareRows(c1.get(), c2.get()) < 0 ? c1 : c2;

                res.add(c.get());
            }
            while (c.next());

            // Switch to non-empty one.
            c = c == c1 ? c2 : c1;

            do {
                res.add(c.get());
            }
            while (c.next());

            rows = res;
        }
    }
}