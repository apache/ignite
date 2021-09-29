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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.query.CacheQueryPageRequester;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryFutureAdapter;

/**
 * Reducer of distirbuted query that sort result through all nodes. Note that it's assumed that every node
 * returns pre-sorted collection of data.
 */
public class MergeSortDistributedCacheQueryReducer<R> extends AbstractDistributedCacheQueryReducer<R> {
    /**
     * Queue of pages from all nodes. Order of streams controls with order of head items of pages.
     */
    private final PriorityQueue<NodePage<R>> nodePages;

    /**
     * @param rowCmp Comparator to sort query results from different nodes.
     */
    public MergeSortDistributedCacheQueryReducer(
        GridCacheQueryFutureAdapter<?, ?, ?> fut, long reqId, CacheQueryPageRequester pageRequester,
        Collection<ClusterNode> nodes, Comparator<R> rowCmp
    ) {
        super(fut, reqId, pageRequester, nodes);

        // Compares head pages from all nodes to get the lowest value at the moment.
        Comparator<NodePage<R>> pageCmp = (o1, o2) -> rowCmp.compare(o1.head(), o2.head());

        nodePages = new PriorityQueue<>(nodes.size(), pageCmp);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        // Initial sort.
        if (nodePages.isEmpty() && !streams.isEmpty()) {
            Set<UUID> nodes = new HashSet<>(streams.keySet());

            for (UUID nodeId : nodes)
                fillWithPage(nodeId);
        }

        return !nodePages.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public R next() throws IgniteCheckedException {
        if (nodePages.isEmpty())
            throw new NoSuchElementException("No next element. Please, be sure to invoke hasNext() before next().");

        NodePage<R> page = nodePages.poll();

        R o = page.next();

        if (page.hasNext())
            nodePages.offer(page);
        else
            fillWithPage(page.nodeId());

        return o;
    }

    /** */
    private void fillWithPage(UUID nodeId) throws IgniteCheckedException {
        NodePage<R> page = streams.get(nodeId).nextPage();

        if (!page.hasNext())
            streams.remove(nodeId);
        else
            nodePages.offer(page);
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        super.cancel();

        nodePages.clear();
    }
}
