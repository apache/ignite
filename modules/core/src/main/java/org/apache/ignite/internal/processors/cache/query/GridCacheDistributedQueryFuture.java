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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCompartorImpl;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.reducer.MergeSortCacheQueryReducer;
import org.apache.ignite.internal.processors.cache.query.reducer.NodePage;
import org.apache.ignite.internal.processors.cache.query.reducer.NodePageStream;
import org.apache.ignite.internal.processors.cache.query.reducer.UnsortedCacheQueryReducer;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.typedef.internal.U;

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
            streams.computeIfAbsent(node.id(), nodeId ->
                new NodePageStream<>(nodeId, () -> requestPages(nodeId), () -> cancelPages(nodeId)));
        }

        Map<UUID, NodePageStream<R>> streamsMap = Collections.unmodifiableMap(streams);

        switch (qry.query().type()) {
            case TEXT: reducer = new MergeSortCacheQueryReducer<>(streamsMap,
                (o1, o2) -> Float.compare(
                    ((ScoredCacheEntry<?, ?>)o1.head()).score(), ((ScoredCacheEntry<?, ?>)o2.head()).score()));
                break;

            case INDEX: reducer = new MergeSortCacheQueryReducer<>(streamsMap, new IndexedNodePageComparator<>());
                break;

            default: reducer = new UnsortedCacheQueryReducer<>(streamsMap);
        }
    }

    /** Comparing rows by indexed keys. */
    private static class IndexedNodePageComparator<R> implements Comparator<NodePage<R>>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Every node will return the same key types for the same index, then it's possible to use simple comparator. */
        private final IndexRowComparator idxRowComp = new IndexRowCompartorImpl();

        /** {@inheritDoc} */
        @Override public int compare(NodePage<R> o1, NodePage<R> o2) {
            IndexedCacheEntry<?, ?> e1 = (IndexedCacheEntry<?, ?>)o1.head();
            IndexedCacheEntry<?, ?> e2 = (IndexedCacheEntry<?, ?>)o2.head();

            try {
                for (int i = 0; i < e1.keysSize(); i++) {
                    int cmp = idxRowComp.compareKey(e1.key(i), e2.key(i));

                    if (cmp != 0) {
                        boolean desc = ((e1.orderMask() >> i) & 1) > 0;

                        return desc ? -cmp : cmp;
                    }
                }

                return 0;

            } catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to sort remote index rows", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void cancelQuery(Throwable err) {
        firstPageLatch.countDown();

        for (NodePageStream<R> s : streams.values())
            s.cancel(err);

        cctx.queries().onQueryFutureCanceled(reqId);

        clear();
    }

    /** {@inheritDoc} */
    @Override protected void onNodeLeft(UUID nodeId) {
        boolean hasRemotePages = streams.get(nodeId).hasRemotePages();

        if (hasRemotePages)
            onError(new ClusterTopologyCheckedException("Remote node has left topology: " + nodeId));
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
     * Send initial query request to query nodes.
     */
    public void startQuery() {
        try {
            GridCacheQueryRequest req = GridCacheQueryRequest.startQueryRequest(cctx, reqId, this);

            qryMgr.sendRequest(this, req, streams.keySet());
        }
        catch (IgniteCheckedException e) {
            onError(e);
        }
    }

    /**
     * Send request to fetch new pages.
     *
     * @param nodeId Node to send request.
     */
    private void requestPages(UUID nodeId) {
        try {
            GridCacheQueryRequest req = GridCacheQueryRequest.pageRequest(cctx, reqId, query().query(), fields());

            qryMgr.sendRequest(this, req, Collections.singletonList(nodeId));
        }
        catch (IgniteCheckedException e) {
            onError(e);
        }
    }

    /**
     * Cancels remote pages.
     *
     * @param nodeId Node to send request.
     */
    // TODO IGNITE-15731: Refactor how CacheQueryReducer handles remote nodes.
    private void cancelPages(UUID nodeId) {
        try {
            GridCacheQueryRequest req = GridCacheQueryRequest.cancelRequest(cctx, reqId, fields());

            if (nodeId.equals(cctx.localNodeId())) {
                // Process cancel query directly (without sending) for local node,
                cctx.closures().callLocalSafe(new GridPlainCallable<Object>() {
                    @Override public Object call() {
                        qryMgr.processQueryRequest(cctx.localNodeId(), req);

                        return null;
                    }
                });
            }
            else {
                try {
                    cctx.io().send(nodeId, req, cctx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    if (cctx.io().checkNodeLeft(nodeId, e, false)) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send cancel request, node failed: " + nodeId);
                    }
                    else
                        U.error(log, "Failed to send cancel request [node=" + nodeId + ']', e);
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(logger(), "Failed to send cancel request (will cancel query in any case).", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void onError(Throwable err) {
        if (onDone(err)) {
            streams.values().forEach(s -> s.cancel(err));

            firstPageLatch.countDown();
        }
    }
}
