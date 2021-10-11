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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.ScoredCacheEntry;

/**
 * Reducer of distirbuted query that sort result through all nodes. Note that it's assumed that every node
 * returns pre-sorted collection of data.
 */
public class MergeSortDistributedCacheQueryReducer<R> extends CacheQueryReducer<R> {
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
    public MergeSortDistributedCacheQueryReducer(final Map<UUID, NodePageStream<R>> pageStreams) {
        super(pageStreams);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        // Initial sort.
        if (nodePages == null) {
            // Compares head pages from all nodes to get the lowest value at the moment.
            Comparator<NodePage<R>> pageCmp = (o1, o2) -> textResultComparator.compare(
                (ScoredCacheEntry<?, ?>)o1.head(), (ScoredCacheEntry<?, ?>)o2.head());

            nodePages = new PriorityQueue<>(pageStreams.size(), pageCmp);

            for (NodePageStream<R> s : pageStreams.values()) {
                NodePage<R> p = page(s.headPage());

                if (p == null || !p.hasNext())
                    continue;

                nodePages.add(p);
            }
        }

        if (pendingNodeId != null) {
            NodePageStream<R> stream = pageStreams.get(pendingNodeId);

            if (!stream.closed()) {
                NodePage<R> p = page(stream.headPage());

                if (p != null && p.hasNext())
                    nodePages.add(p);
            }
        }

        return !nodePages.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public R nextX() throws IgniteCheckedException {
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

    /** Compares rows for {@code TextQuery} results for ordering results in MergeSort reducer. */
    private static final Comparator<ScoredCacheEntry<?, ?>> textResultComparator = (c1, c2) ->
        Float.compare(c2.score(), c1.score());
}
