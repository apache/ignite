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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.query.CacheQueryPageRequester;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryFutureAdapter;

/**
 * Reducer of distirbuted query that sort result through all nodes. Note that it's assumed that every node
 * returns pre-sorted collection of data.
 */
public class MergeSortDistributedCacheQueryReducer<R> extends AbstractDistributedCacheQueryReducer<R> {
    /** Map of Node ID to stream. Used for inserting new pages. */
    private final Map<UUID, NodePageStream<R>> streamsMap;

    /** Queue of nodes result pages streams. Order of streams is set with {@link #streamCmp}. */
    private final PriorityQueue<NodePageStream<R>> streams;

    /** If {@code true} then there wasn't call {@link #hasNext()} on this reducer. */
    private boolean first = true;

    /** Compares head of streams to get lowest value at the moment. */
    private final Comparator<NodePageStream<R>> streamCmp;

    /**
     * @param fut Cache query future.
     * @param reqId Cache query request ID.
     * @param pageRequester Provides a functionality to request pages from remote nodes.
     * @param queueLock Lock object that is shared between GridCacheQueryFuture and reducer.
     * @param nodes Collection of nodes this query applies to.
     * @param rowCmp Comparator to sort query results from different nodes.
     */
    public MergeSortDistributedCacheQueryReducer(
        GridCacheQueryFutureAdapter fut, long reqId, CacheQueryPageRequester pageRequester,
        Object queueLock, Collection<ClusterNode> nodes, Comparator<R> rowCmp
    ) {
        super(fut, reqId, pageRequester);

        streamCmp = (o1, o2) -> {
            // Nulls on the top to make initial sort in hasNext().
            if (o1.head() == null)
                return -1;

            if (o2.head() == null)
                return 1;

            return rowCmp.compare(o1.head(), o2.head());
        };

        streamsMap = new ConcurrentHashMap<>(nodes.size());
        streams = new PriorityQueue<>(nodes.size(), streamCmp);

        for (ClusterNode node: nodes) {
            NodePageStream<R> s = new NodePageStream<>(
                fut.query().query(), queueLock, fut.endTime(), node.id(), this::requestPages);

            streams.offer(s);

            streamsMap.put(node.id(), s);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        if (first) {
            // Initial sort.
            int size = streams.size();

            for (int i = 0; i < size; i++) {
                NodePageStream<R> s = streams.poll();

                if (s.hasNext())
                    streams.offer(s);
                else
                    streamsMap.remove(s.nodeId());
            }

            first = false;
        }

        return !streams.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public R next() throws IgniteCheckedException {
        if (streams.isEmpty())
            throw new NoSuchElementException("No next element. Please, be sure to invoke hasNext() before next().");

        NodePageStream<R> s = streams.poll();

        R o = s.next();

        if (s.hasNext())
            streams.add(s);
        else
            streamsMap.remove(s.nodeId());

        return o;
    }

    /** {@inheritDoc} */
    @Override public boolean onPage(UUID nodeId, Collection<R> data, boolean last) {
        NodePageStream<R> stream = streamsMap.get(nodeId);

        if (stream == null)
            return streamsMap.isEmpty();

        stream.addPage(nodeId, data, last);

        return last && (streamsMap.remove(nodeId) != null) && streamsMap.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public void onError() {
        for (NodePageStream<R> s: streamsMap.values())
            s.onError();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        List<UUID> nodes = new ArrayList<>();

        for (NodePageStream<R> s: streamsMap.values()) {
            Collection<UUID> n = s.cancelNodes();

            nodes.addAll(n);
        }

        streamsMap.clear();

        cancel(nodes);
    }

    /** {@inheritDoc} */
    @Override public boolean mapNode(UUID nodeId) {
        return streamsMap.containsKey(nodeId);
    }
}
