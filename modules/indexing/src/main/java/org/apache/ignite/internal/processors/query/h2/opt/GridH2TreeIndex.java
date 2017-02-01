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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeMap;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.IndexType;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;

/**
 * Snapshotable tree index.
 */
@SuppressWarnings("ComparatorNotSerializable")
public class GridH2TreeIndex extends GridH2AbstractTreeIndex {
    /** */
    private final ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> tree;

    /** */
    private final boolean snapshotEnabled;

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
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        final GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (desc == null || desc.memory() == null) {
            snapshotEnabled = desc == null || desc.snapshotableIndex();

            if (snapshotEnabled) {
                tree = new SnapTreeMap<GridSearchRowPointer, GridH2Row>(this) {
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
            else {
                tree = new ConcurrentSkipListMap<>(
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
        else {
            assert desc.snapshotableIndex() : desc;

            snapshotEnabled = true;

            tree = new GridOffHeapSnapTreeMap<GridSearchRowPointer, GridH2Row>(desc, desc, desc.memory(), desc.guard(), this) {
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

        initDistributedJoinMessaging(tbl);
    }

    /** {@inheritDoc} */
    @Override protected Object doTakeSnapshot() {
        assert snapshotEnabled;

        return tree instanceof SnapTreeMap ?
            ((SnapTreeMap)tree).clone() :
            ((GridOffHeapSnapTreeMap)tree).clone();
    }

    /** {@inheritDoc} */
    protected final ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> treeForRead() {
        return treeForRead(0);
    }

    /** {@inheritDoc} */
    protected final ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> treeForRead(int segment) {
        assert segment == 0;

        if (!isSnapshotEnabled())
            return tree;

        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> res = threadLocalSnapshot();

        if (res == null)
            res = tree;

        return res;
    }

    /** {@inheritDoc} */
    protected boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }

    /** {@inheritDoc} */
    public GridH2Row findOne(GridSearchRowPointer row) {
        return tree.get(row);
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        return tree.put(row, row);
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        return tree.remove(comparable(row, 0));
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
    @Override public void destroy() {
        assert threadLocalSnapshot() == null;

        if (tree instanceof AutoCloseable)
            U.closeQuiet((AutoCloseable)tree);

        super.destroy();
    }

    /**
     * @param qctx Query context.
     * @param cctx Cache context.
     * @return Collection of nodes for broadcasting.
     */
    private List<SegmentKey> broadcastSegments(GridH2QueryContext qctx, GridCacheContext<?, ?> cctx) {
        Map<UUID, int[]> partMap = qctx.partitionsMap();

        List<ClusterNode> nodes;

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

        List<SegmentKey> res = new ArrayList<>(nodes.size());

        for (ClusterNode node : nodes) {
            res.add(new SegmentKey(node, 0));
        }

        return res;
    }

}