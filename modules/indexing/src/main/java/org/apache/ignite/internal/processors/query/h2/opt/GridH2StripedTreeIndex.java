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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeMap;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.IndexType;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;

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

        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree = segmentTree();

        return tree instanceof SnapTreeMap ?
            ((SnapTreeMap)tree).clone() :
            ((GridOffHeapSnapTreeMap)tree).clone();
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
    protected ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> treeForRead(int seg) {
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
        return segmentTree().get(row);
    }

    /** */
    protected ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> segmentTree() {
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

        int seg = segment(row);

        return segments[seg].remove(comparable);
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

    /** {@inheritDoc} */
    @Override protected int segmentsCount() {
        return segments.length;
    }

    /**
     * @param partition Parttition idx.
     * @return index currentSegment Id for given key
     */
    protected int segment(int partition) {
        return partition % segments.length;
    }

    /**
     * @param row
     * @return index currentSegment Id for given row
     */
    private int segment(SearchRow row) {
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
}
