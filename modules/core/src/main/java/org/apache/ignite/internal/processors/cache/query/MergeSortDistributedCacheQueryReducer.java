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

package org.apache.ignite.internal.processors.cache.query;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Reducer of distirbuted query that sort result through all nodes. Note that it's assumed that every node
 * returns pre-sorted collection of data.
 */
public class MergeSortDistributedCacheQueryReducer<R> extends UnsortedDistributedCacheQueryReducer<R> {
    /** Map of Node ID to stream. Used for inserting new pages. */
    private final Map<UUID, NodePageStream> streamsMap;

    /** Array of streams. Used for iteration over result pages. */
    private final NodePageStream[] streams;

    /** If {@code true} then there wasn't call {@link #hasNext()} on this reducer. */
    private boolean first = true;

    /**
     * Offset of current stream to get next value. This offset only increments if a stream is done. The array {@link
     * #streams} is sorted to put stream with lowest value on this offset.
     */
    private int streamOff;

    /** Compares head of streams to get lowest value at the moment. */
    private final Comparator<NodePageStream> streamCmp;

    /**
     * @param rowCmp Comparator to sort query results from different nodes.
     */
    public MergeSortDistributedCacheQueryReducer(GridCacheQueryFutureAdapter fut, long reqId,
        CacheQueryPageRequester fetcher, Collection<ClusterNode> nodes, Comparator<R> rowCmp
    ) {
        super(fut, reqId, fetcher, nodes);

        synchronized (sharedLock()) {
            streamsMap = new ConcurrentHashMap<>(nodes.size());
            streams = (NodePageStream[])Array.newInstance(NodePageStream.class, nodes.size());

            int i = 0;

            for (ClusterNode node : nodes) {
                streams[i] = new NodePageStream(node.id());
                streamsMap.put(node.id(), streams[i++]);
            }
        }

        streamCmp = (o1, o2) -> {
            if (o1 == o2)
                return 0;

            if (o1 == null)
                return -1;

            if (o2 == null)
                return 1;

            return rowCmp.compare(o1.head, o2.head);
        };
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
                streamsMap.remove(s.nodeId);
                streams[i] = null;
                streamOff++;
            }
            else {
                // Prefetch head value.
                s.next();
            }
        }

        if (finished())
            throw new NoSuchElementException("No next element. Please, be sure to invoke hasNext() before next().");

        if (first) {
            first = false;

            Arrays.sort(streams, streamCmp);
        } else
            bubbleUp(streams, streamOff, streamCmp);

        return streams[streamOff].get();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        for (int i = streamOff; i < streams.length; i++) {
            if (streams[i].hasNext())
                return true;

            // Nullify obsolete stream, move left bound.
            streamsMap.remove(streams[i].nodeId);
            streams[i] = null;
            streamOff++;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override protected void loadPage() {
        assert !Thread.holdsLock(sharedLock());

        List<UUID> nodes;

        synchronized (sharedLock()) {
            if (!loadAllowed)
                return;

            nodes = new ArrayList<>();

            for (int i = streamOff; i < streams.length; i++) {
                UUID nodeId = streams[i].nodeId;

                // Try to contain 2 pages for every stream to avoid waits. A node has to be present in collections:
                // 1. rcvd - as requested and already handled before, so currently no parallel request to this node.
                // 2. subgrid - as this node still has pages to request.
                if (streams[i].queue.size() < 1 && rcvd.remove(nodeId) && subgrid.contains(nodeId))
                    nodes.add(nodeId);
            }
        }

        if (nodes != null)
            pageRequester.requestPages(reqId, fut, nodes, false);
    }

    /** {@inheritDoc} */
    @Override public boolean onPage(@Nullable UUID nodeId, boolean last) {
        boolean qryLast = super.onPage(nodeId, last);

        if (nodeId == null)
            nodeId = cctx.localNodeId();

        if (last && streamsMap.containsKey(nodeId)) {
            streamsMap.get(nodeId).allPagesReady = true;

            streamsMap.remove(nodeId);
        }

        return qryLast;
    }

    /** {@inheritDoc} */
    @Override public void addPage(@Nullable UUID nodeId, Collection<R> data) {
        assert Thread.holdsLock(sharedLock());

        // Local node.
        if (nodeId == null) {
            nodeId = cctx.localNodeId();

            // If nodeId is NULL and query doesn't execute on local node, then it is error, notify all streams.
            if (!streamsMap.containsKey(nodeId)) {
                assert data.isEmpty();

                for (PageStream stream: streamsMap.values())
                    stream.addPage(data);

                return;
            }
        }

        assert streamsMap.containsKey(nodeId) : "Get result from unexpected node: " + nodeId;

        streamsMap.get(nodeId).addPage(data);
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        super.cancel();

        for (int i = streamOff; i < streams.length; i++) {
            streams[i].clear();
            streams[i] = null;
        }

        streamsMap.clear();
    }

    /** */
    private boolean finished() {
        return streamOff == streams.length;
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

    /** Stream attached to a single node. */
    private class NodePageStream extends PageStream {
        /** Head value of current stream. */
        private R head;

        /** Node ID. */
        private final UUID nodeId;

        /** Whether this stream fetch all pages. */
        volatile boolean allPagesReady;

        /** */
        NodePageStream(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public R next() throws IgniteCheckedException {
            head = streamIterator().next();

            return head;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() throws IgniteCheckedException {
            if (head != null)
                return true;

            return streamIterator().hasNext();
        }

        /**
         * Note, that this method should be invoked after {@link #next()}.
         *
         * @return Head of this stream. Can be invoked multiple times.
         */
        public R head() {
            return head;
        }

        /**
         * @return Head of this stream. Then clean head value.
         */
        public R get() {
            assert head != null;

            R val = head;

            head = null;

            return val;
        }

        /** {@inheritDoc} */
        @Override protected boolean allPagesReady() {
            return MergeSortDistributedCacheQueryReducer.this.allPagesReady || allPagesReady || finished();
        }
    }
}
