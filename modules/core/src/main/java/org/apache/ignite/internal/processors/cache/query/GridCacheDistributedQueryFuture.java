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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.reducer.MergeSortDistributedCacheQueryReducer;
import org.apache.ignite.internal.processors.cache.query.reducer.NodePageStream;
import org.apache.ignite.internal.processors.cache.query.reducer.UnsortedDistributedCacheQueryReducer;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.TEXT;

/**
 * Distributed query future.
 */
public class GridCacheDistributedQueryFuture<K, V, R> extends GridCacheQueryFutureAdapter<K, V, R> {
    /** */
    private final long reqId;

    /** Helps to send cache query requests to other nodes. */
    private final GridCacheDistributedQueryManager<K, V> qryMgr;

    /** Collection of streams. */
    private final Map<UUID, NodePageStream<R>> streams;

    /** Count of streams that finish receiving remote pages. */
    private final AtomicInteger noRemotePagesStreamsCnt = new AtomicInteger();

    /** Count down this latch when every node responses on initial cache query request. */
    private final CountDownLatch firstPageLatch = new CountDownLatch(1);

    /** Set of nodes that deliver their first page. */
    private Set<UUID> rcvdFirstPage = ConcurrentHashMap.newKeySet();

    /**
     * @param ctx Cache context.
     * @param reqId Request ID.
     * @param qry Query.
     */
    protected GridCacheDistributedQueryFuture(
        GridCacheContext<K, V> ctx,
        long reqId,
        GridCacheQueryBean qry,
        Collection<ClusterNode> nodes
    ) {
        super(ctx, qry, false);

        assert reqId > 0;

        this.reqId = reqId;

        qryMgr = (GridCacheDistributedQueryManager<K, V>) ctx.queries();

        streams = new ConcurrentHashMap<>(nodes.size());

        for (ClusterNode node : nodes) {
            NodePageStream<R> s = new NodePageStream<>(node.id(), () -> requestPages(node.id()));

            streams.put(node.id(), s);
        }

        Map<UUID, NodePageStream<R>> unmodified = Collections.unmodifiableMap(streams);

        reducer = qry.query().type() == TEXT ?
            new MergeSortDistributedCacheQueryReducer<>(unmodified, endTime())
            : new UnsortedDistributedCacheQueryReducer<>(unmodified, endTime());
    }

    /** {@inheritDoc} */
    @Override protected void cancelQuery() {
        firstPageLatch.countDown();

        List<UUID> nodes = new ArrayList<>();

        for (NodePageStream<R> s : streams.values()) {
            if (s.hasRemotePages()) {
                s.cancel();

                nodes.add(s.nodeId());
            }
        }

        try {
            GridCacheQueryRequest req = GridCacheQueryRequest.cancelRequest(cctx, reqId, fields());

            qryMgr.sendRequest(null, req, nodes);
        }
        catch (IgniteCheckedException e) {
            if (logger() != null)
                U.error(logger(), "Failed to send cancel request (will cancel query in any case).", e);
        }

        cctx.queries().onQueryFutureCanceled(reqId);

        clear();
    }

    /** {@inheritDoc} */
    @Override protected void onNodeLeft(UUID nodeId) {
        boolean hasRemotePages = streams.get(nodeId).hasRemotePages();

        if (hasRemotePages) {
            onPage(nodeId, null,
                new ClusterTopologyCheckedException("Remote node has left topology: " + nodeId), true);
        }
    }

    /** {@inheritDoc} */
    @Override protected void onPageError(Throwable err) {
        onError(err);
    }

    /** {@inheritDoc} */
    @Override protected void onPage(UUID nodeId, Collection<R> data, boolean last) {
        synchronized (firstPageLatch) {
            if (rcvdFirstPage != null) {
                rcvdFirstPage.add(nodeId);

                if (rcvdFirstPage.size() == streams.size()) {
                    firstPageLatch.countDown();

                    rcvdFirstPage.clear();
                    rcvdFirstPage = null;
                }
            }
        }

        NodePageStream<R> stream = streams.get(nodeId);

        if (stream == null)
            return;

        stream.addPage(data, last);

        if (last) {
            int cnt;

            do {
                cnt = noRemotePagesStreamsCnt.get();

            } while (!noRemotePagesStreamsCnt.compareAndSet(cnt, cnt + 1));

            if (cnt + 1 >= streams.size())
                onDone();
        }
    }

    /** {@inheritDoc} */
    @Override public void awaitFirstItemAvailable() throws IgniteCheckedException {
        U.await(firstPageLatch);

        if (isDone() && error() != null)
            // Throw the exception if future failed.
            super.get();
    }

    /** {@inheritDoc} */
    @Override public Collection<R> get() throws IgniteCheckedException {
        return get0();
    }

    /** {@inheritDoc} */
    @Override public Collection<R> get(long timeout, TimeUnit unit) throws IgniteCheckedException {
        return get0();
    }

    /** {@inheritDoc} */
    @Override public Collection<R> getUninterruptibly() throws IgniteCheckedException {
        return get0();
    }

    /**
     * Completion of distributed query future depends on user that iterates over query result with lazy page loading.
     * Then {@link #get()} can lock on unpredictably long period of time. So we should avoid call it.
     */
    private Collection<R> get0() {
        throw new IgniteIllegalStateException("Unexpected lock on iterator over distributed cache query result.");
    }

    /** {@inheritDoc} */
    @Override void clear() {
        assert isDone() : this;

        GridCacheDistributedQueryManager<K, V> qryMgr = (GridCacheDistributedQueryManager<K, V>)cctx.queries();

        if (qryMgr != null)
            qryMgr.removeQueryFuture(reqId);
    }

    /** @return Request ID. */
    long requestId() {
        return reqId;
    }

    /**
     * Send request to fetch new pages.
     *
     * @param node Node to send request.
     */
    private void requestPages(UUID node) {
        try {
            GridCacheQueryRequest req = GridCacheQueryRequest.pageRequest(cctx, reqId, query().query(), fields());

            qryMgr.sendRequest(null, req, Collections.singletonList(node));
        }
        catch (IgniteCheckedException e) {
            onError(e);
        }
    }

    /** Handle receiving error page. */
    private void onError(Throwable err) {
        streams.values().forEach(NodePageStream::cancel);

        onDone(err);

        firstPageLatch.countDown();
    }
}
