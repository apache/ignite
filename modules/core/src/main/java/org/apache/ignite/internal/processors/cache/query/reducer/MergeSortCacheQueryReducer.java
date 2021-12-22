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

package org.apache.ignite.internal.processors.cache.query.reducer;

import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;

/**
 * Reducer of cache query results that sort result through all nodes. Note that it's assumed that every node
 * returns pre-sorted collection of data.
 */
abstract class MergeSortCacheQueryReducer<R> extends CacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Queue of pages from all nodes. Order of streams controls with order of head items of pages. */
    private PriorityQueue<NodePage<R>> nodePages;

    /**
     * Page are iterated within the {@link #nextX()} method. In case a page is done, the corresponding stream is
     * asked for new page. We have to wait it in {@link #hasNextX()}.
     */
    private UUID pendingNodeId;

    /** */
    private ConcurrentHashMap<UUID, Integer> available;

    /** */
    private final int pageSize;

    /** */
    protected MergeSortCacheQueryReducer(final Map<UUID, NodePageStream<R>> pageStreams, int limit, int pageSize) {
        super(pageStreams, limit);

        this.pageSize = pageSize;

        if (limitEnabled()) {
            available = new ConcurrentHashMap<>();

            for (NodePageStream<R> s: pageStreams.values())
                available.put(s.nodeId(), 0);
        }
    }

    /** @return Comparator for pages from nodes. */
    protected abstract CompletableFuture<Comparator<NodePage<R>>> pageComparator();

    /** {@inheritDoc} */
    @Override public boolean hasNext0() throws IgniteCheckedException {
        // Initial sort.
        if (nodePages == null) {
            // Compares head pages from all nodes to get the lowest value at the moment.
            nodePages = new PriorityQueue<>(pageStreams.size(), get(pageComparator()));

            for (NodePageStream<R> s : pageStreams.values()) {
                NodePage<R> p = get(s.headPage());

                if (p == null || !p.hasNext())
                    continue;

                nodePages.add(p);
            }
        }

        if (pendingNodeId != null) {
            NodePageStream<R> stream = pageStreams.get(pendingNodeId);

            if (!stream.closed()) {
                NodePage<R> p = get(stream.headPage());

                if (p != null && p.hasNext())
                    nodePages.add(p);
            }

            pendingNodeId = null;
        }

        return !nodePages.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public R next0() {
        if (nodePages.isEmpty())
            throw new NoSuchElementException("No next element. Please, be sure to invoke hasNext() before next().");

        NodePage<R> page = nodePages.poll();

        R o = page.next();

        if (page.hasNext())
            nodePages.offer(page);
        else if (!pageStreams.get(page.nodeId()).closed())
            pendingNodeId = page.nodeId();

        return o;
    }

    /** {@inheritDoc} */
    @Override protected boolean checkLimit(int cnt) {
        int l = limit;

        for (NodePageStream<R> s: pageStreams.values()) {
            if (l <= 0)
                return true;

            // Stream returns all data.
            if (s.closed()) {
                l -= s.dataSize();

                continue;
            }

            int ds = s.dataSize();

            // If data size equals to 2 pages, it means we don't request for new pages yet. So, don't know how many data delivered.
            while (ds > pageSize * 2) {
                l -= pageSize;

                ds -= pageSize;
            }
        }

        return false;
    }
}
