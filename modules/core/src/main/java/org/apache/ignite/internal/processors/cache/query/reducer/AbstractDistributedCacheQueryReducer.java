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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.CacheQueryPageRequester;
import org.apache.ignite.internal.processors.cache.query.DistributedCacheQueryReducer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryFutureAdapter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Abstract class for distributed reducer implementations.
 */
abstract class AbstractDistributedCacheQueryReducer<R> implements DistributedCacheQueryReducer<R> {
    /** Collection of streams that don't deliver all pages yet. */
    private final Map<UUID, NodePageStream<R>> remoteStreams;

    /** Set of nodes that deliver their first page. */
    private Set<UUID> rcvdFirstPage = new GridConcurrentHashSet<>();

    /** Collection of streams. */
    protected final Map<UUID, NodePageStream<R>> streams;

    /** Query request ID. */
    private final long reqId;

    /** Query future. */
    private final GridCacheQueryFutureAdapter fut;

    /** Cache query page requester. */
    private final CacheQueryPageRequester pageRequester;

    /**
     * Whether it is allowed to send cache query result requests to nodes.
     * It is set to {@code false} if query doesn't accept initial pages from all nodes, or query is finished or failed.
     */
    private volatile boolean loadAllowed;

    /**
     * Count down this latch when every node responses on initial cache query request.
     */
    private final CountDownLatch firstPageLatch = new CountDownLatch(1);

    /**
     * @param fut Cache query future.
     * @param reqId Cache query request ID.
     * @param pageRequester Provides a functionality to request pages from remote nodes.
     * @param queueLock Lock object that is shared between GridCacheQueryFuture and reducer.
     * @param nodes Collection of nodes this query applies to.
     */
    protected AbstractDistributedCacheQueryReducer(GridCacheQueryFutureAdapter fut, long reqId,
        CacheQueryPageRequester pageRequester, Object queueLock, Collection<ClusterNode> nodes) {
        this.fut = fut;
        this.reqId = reqId;
        this.pageRequester = pageRequester;

        streams = new HashMap<>(nodes.size());

        for (ClusterNode node: nodes) {
            NodePageStream<R> s = new NodePageStream<>(
                fut.query().query(), queueLock, fut.endTime(), node.id(), this::requestPages);

            streams.put(node.id(), s);
        }

        remoteStreams = new ConcurrentHashMap<>(streams);
    }

    /** {@inheritDoc} */
    @Override public void onFinish() {
        loadAllowed = false;

        firstPageLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void awaitInitialization() throws IgniteInterruptedCheckedException {
        U.await(firstPageLatch);
    }

    /**
     * Callback that invoked when all nodes response with initial page.
     */
    private void onFirstItemReady() {
        loadAllowed = true;

        firstPageLatch.countDown();
    }

    /**
     * Send request to fetch new pages.
     *
     * @param nodes Collection of nodes to send request.
     * @param all Whether page will contain all data from node.
     */
    private void requestPages(Collection<UUID> nodes, boolean all) {
        pageRequester.requestPages(reqId, fut, nodes, all);
    }

    /** {@inheritDoc} */
    @Override public void onError() {
        for (NodePageStream<R> s: streams.values())
            s.onError();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        List<UUID> nodes = new ArrayList<>();

        for (NodePageStream<R> s: remoteStreams.values()) {
            s.cancel();

            nodes.add(s.nodeId());
        }

        remoteStreams.clear();
        streams.clear();

        pageRequester.cancelQuery(reqId, nodes, fut.fields());
    }

    /** {@inheritDoc} */
    @Override public boolean onPage(UUID nodeId, Collection<R> data, boolean last) {
        if (!loadAllowed) {
            rcvdFirstPage.add(nodeId);

            if (rcvdFirstPage.size() == streams.size()) {
                onFirstItemReady();

                rcvdFirstPage.clear();
                rcvdFirstPage = null;
            }
        }

        NodePageStream<R> stream = remoteStreams.get(nodeId);

        if (stream == null)
            return remoteStreams.isEmpty();

        stream.addPage(nodeId, data, last);

        return last && (remoteStreams.remove(nodeId) != null) && remoteStreams.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean remoteQueryNode(UUID nodeId) {
        return remoteStreams.containsKey(nodeId);
    }
}
