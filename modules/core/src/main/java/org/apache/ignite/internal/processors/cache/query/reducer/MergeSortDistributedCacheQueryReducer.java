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

/**
 * Reducer of distirbuted query that sort result through all nodes. Note that it's assumed that every node
 * returns pre-sorted collection of data.
 */
public class MergeSortDistributedCacheQueryReducer<R> extends DistributedCacheQueryReducer<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Queue of pages from all nodes. Order of streams controls with order of head items of pages.
     */
    private final PriorityQueue<NodePage<R>> nodePages;

    /**
     * @param rowCmp Comparator to sort query results from different nodes.
     */
    public MergeSortDistributedCacheQueryReducer(
        final Map<UUID, NodePageStream<R>> pageStreams,
        Comparator<R> rowCmp,
        long endTime
    ) {
        super(pageStreams, endTime);

        // Compares head pages from all nodes to get the lowest value at the moment.
        Comparator<NodePage<R>> pageCmp = (o1, o2) -> rowCmp.compare(o1.head(), o2.head());

        nodePages = new PriorityQueue<>(pageStreams.size(), pageCmp);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        // Initial sort.
        if (nodePages.isEmpty()) {
            for (NodePageStream<R> s: pageStreams.values()) {
                NodePage<R> p = page(s.nodeId(), s.nextPage());

                if (p == null || !p.hasNext())
                    continue;

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
        else {
            NodePage<R> p = page(page.nodeId(), pageStreams.get(page.nodeId()).nextPage());

            if (p != null && p.hasNext())
                nodePages.offer(p);
        }

        return o;
    }
}
