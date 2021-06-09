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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.CacheQueryPageRequester;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Reducer of distirbuted query that sort result through all nodes. Note that it's assumed that every node
 * returns pre-sorted collection of data.
 */
public class MergeSortDistributedCacheQueryReducer<R> extends AbstractDistributedCacheQueryReducer<R> {
    /** Map of Node ID to stream. Used for inserting new pages. */
    private final Map<UUID, NodePageStream<R>> streamsMap;

    /** Array of streams. Used for iteration over result pages. */
    private final NodePageStream<R>[] streams;

    /** If {@code true} then there wasn't call {@link #hasNext()} on this reducer. */
    private boolean first = true;

    /**
     * Offset of current stream to get next value. This offset only increments if a stream is done. The array {@link
     * #streams} is sorted to put stream with lowest value on this offset.
     */
    private int streamOff;

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

        streamsMap = new ConcurrentHashMap<>(nodes.size());
        streams = (NodePageStream<R>[])Array.newInstance(NodePageStream.class, nodes.size());

        int i = 0;

        for (ClusterNode node : nodes) {
            streams[i] = new NodePageStream<>(fut.query().query(), queueLock, fut.endTime(), node.id(), (ns, all) ->
                pageRequester.requestPages(reqId, fut, ns, all)
            );

            streamsMap.put(node.id(), streams[i++]);
        }

        streamCmp = (o1, o2) -> {
            if (o1 == o2)
                return 0;

            if (o1 == null)
                return -1;

            if (o2 == null)
                return 1;

            return rowCmp.compare(o1.head(), o2.head());
        };
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        for (int i = streamOff; i < streams.length; i++) {
            if (streams[i].hasNext())
                return true;

            // Nullify obsolete stream, move left bound.
            streamsMap.remove(streams[i].nodeId());
            streams[i] = null;
            streamOff++;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public R next() throws IgniteCheckedException {
        for (int i = streamOff; i < streams.length; i++) {
            NodePageStream s = streams[i];

            // Skip if stream has head: on previous next() head extracts from stream[streamOff], others streams do
            // have head or it is the first run.
            if (s.head() != null)
                break;

            if (!s.hasNext()) {
                // Nullify obsolete stream, move left bound.
                streamsMap.remove(s.nodeId());
                streams[i] = null;
                streamOff++;
            }
            else {
                // Prefetch head value.
                s.next();
            }
        }

        if (streamOff == streams.length)
            throw new NoSuchElementException("No next element. Please, be sure to invoke hasNext() before next().");

        if (first) {
            first = false;

            Arrays.sort(streams, streamCmp);
        } else
            bubbleUp(streams, streamOff, streamCmp);

        return streams[streamOff].get();
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
    @Override public void requestFullPages() throws IgniteInterruptedCheckedException {
        awaitInitialization();

        if (loadAllowed()) {
            for (NodePageStream<R> s: streamsMap.values())
                s.requestFullPages();
        }
    }

    /** {@inheritDoc} */
    @Override public void onCancel() {
        for (NodePageStream<R> s: streamsMap.values())
            s.cancel(ns -> pageRequester.cancelQueryRequest(reqId, ns, fut.fields()));

        streamsMap.clear();
    }

    /** {@inheritDoc} */
    @Override public boolean queryNode(UUID nodeId) {
        return streamsMap.containsKey(nodeId);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param cmp Comparator.
     */
    private static <Z> void bubbleUp(Z[] arr, int off, Comparator<Z> cmp) {
        for (int i = off, last = arr.length - 1; i < last; i++) {
            if (cmp.compare(arr[i], arr[i + 1]) <= 0)
                break;

            U.swap(arr, i, i + 1);
        }
    }
}
