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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.GridBoundedConcurrentOrderedSet;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;

/**
 * Distributed query manager (for cache in REPLICATED / PARTITIONED cache mode).
 */
public class GridCacheDistributedQueryManager<K, V> extends GridCacheQueryManager<K, V> {
    /** */
    private static final int MAX_CANCEL_IDS = 1000;

    /** Query response frequency. */
    private static final long RESEND_FREQ = 3000;

    /** Query response attempts. */
    private static final int RESEND_ATTEMPTS = 5;

    /** Prefix for communication topic. */
    private static final String TOPIC_PREFIX = "QUERY";

    /** {request ID -> thread} */
    private ConcurrentMap<Long, Thread> threads = new ConcurrentHashMap8<>();

    /** {request ID -> future} */
    private ConcurrentMap<Long, GridCacheDistributedQueryFuture<?, ?, ?>> futs =
        new ConcurrentHashMap8<>();

    /** Received requests to cancel. */
    private Collection<CancelMessageId> cancelIds =
        new GridBoundedConcurrentOrderedSet<>(MAX_CANCEL_IDS);

    /** Canceled queries. */
    private Collection<Long> cancelled = new GridBoundedConcurrentOrderedSet<>(MAX_CANCEL_IDS);

    /** Query response handler. */
    private IgniteBiInClosure<UUID,GridCacheQueryResponse> resHnd = new CI2<UUID, GridCacheQueryResponse>() {
        @Override public void apply(UUID nodeId, GridCacheQueryResponse res) {
            processQueryResponse(nodeId, res);
        }
    };

    /** Event listener. */
    private GridLocalEventListener lsnr;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        super.start0();

        assert cctx.config().getCacheMode() != LOCAL;

        cctx.io().addHandler(cctx.cacheId(), GridCacheQueryRequest.class, new CI2<UUID, GridCacheQueryRequest>() {
            @Override public void apply(UUID nodeId, GridCacheQueryRequest req) {
                processQueryRequest(nodeId, req);
            }
        });

        lsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                for (GridCacheDistributedQueryFuture fut : futs.values())
                    fut.onNodeLeft(discoEvt.eventNode().id());
            }
        };

        cctx.events().addListener(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        super.onKernalStop0(cancel);

        cctx.events().removeListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(reconnectFut,
            "Query was cancelled, client node disconnected.");

        for (Map.Entry<Long, GridCacheDistributedQueryFuture<?, ?, ?>> e : futs.entrySet()) {
            GridCacheDistributedQueryFuture<?, ?, ?> fut = e.getValue();

            fut.onPage(null, null, err, true);

            futs.remove(e.getKey(), fut);
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        super.printMemoryStats();

        X.println(">>>   threadsSize: " + threads.size());
        X.println(">>>   futsSize: " + futs.size());
    }

    /**
     * Removes query future from futures map.
     *
     * @param reqId Request id.
     * @param fut Query future.
     */
    protected void addQueryFuture(long reqId, GridCacheDistributedQueryFuture<?, ?, ?> fut) {
        futs.put(reqId, fut);

        if (cctx.kernalContext().clientDisconnected()) {
            IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(
                cctx.kernalContext().cluster().clientReconnectFuture(),
                "Query was cancelled, client node disconnected.");

            fut.onDone(err);
        }
    }

    /**
     * Removes query future from futures map.
     *
     * @param reqId Request id.
     */
    protected void removeQueryFuture(long reqId) {
        futs.remove(reqId);
    }

    /**
     * Gets query future from futures map.
     *
     * @param reqId Request id.
     * @return Found future or null.
     */
    protected GridCacheDistributedQueryFuture<?, ?, ?> getQueryFuture(long reqId) {
        return futs.get(reqId);
    }

    /**
     * Processes cache query request.
     *
     * @param sndId Sender node id.
     * @param req Query request.
     */
    @SuppressWarnings("unchecked")
    @Override void processQueryRequest(UUID sndId, GridCacheQueryRequest req) {
        if (req.cancel()) {
            cancelIds.add(new CancelMessageId(req.id(), sndId));

            if (req.fields())
                removeFieldsQueryResult(sndId, req.id());
            else
                removeQueryResult(sndId, req.id());
        }
        else {
            if (!cancelIds.contains(new CancelMessageId(req.id(), sndId))) {
                if (!F.eq(req.cacheName(), cctx.name())) {
                    GridCacheQueryResponse res = new GridCacheQueryResponse(
                        cctx.cacheId(),
                        req.id(),
                        new IgniteCheckedException("Received request for incorrect cache [expected=" + cctx.name() +
                            ", actual=" + req.cacheName()),
                        cctx.deploymentEnabled());

                    sendQueryResponse(sndId, res, 0);
                }
                else {
                    threads.put(req.id(), Thread.currentThread());

                    try {
                        GridCacheQueryInfo info = distributedQueryInfo(sndId, req);

                        if (info == null)
                            return;

                        if (req.fields())
                            runFieldsQuery(info);
                        else
                            runQuery(info);
                    }
                    catch (Throwable e) {
                        U.error(log(), "Failed to run query.", e);

                        sendQueryResponse(sndId, new GridCacheQueryResponse(cctx.cacheId(), req.id(), e.getCause(),
                            cctx.deploymentEnabled()), 0);

                        if (e instanceof Error)
                            throw (Error)e;
                    }
                    finally {
                        threads.remove(req.id());
                    }
                }
            }
        }
    }

    /**
     * @param sndId Sender node id.
     * @param req Query request.
     * @return Query info.
     */
    @Nullable private GridCacheQueryInfo distributedQueryInfo(UUID sndId, GridCacheQueryRequest req) {
        IgniteReducer<Object, Object> rdc = req.reducer();
        IgniteClosure<Object, Object> trans = (IgniteClosure<Object, Object>)req.transformer();

        ClusterNode sndNode = cctx.node(sndId);

        if (sndNode == null)
            return null;

        GridCacheQueryAdapter<?> qry =
            new GridCacheQueryAdapter<>(
                cctx,
                req.type(),
                log,
                req.pageSize(),
                0,
                false,
                req.includeBackups(),
                false,
                null,
                req.keyValueFilter(),
                req.partition() == -1 ? null : req.partition(),
                req.className(),
                req.clause(),
                req.includeMetaData(),
                req.keepBinary(),
                req.subjectId(),
                req.taskHash()
            );

        return new GridCacheQueryInfo(
            false,
            trans,
            rdc,
            qry,
            null,
            sndId,
            req.id(),
            req.includeMetaData(),
            req.allPages(),
            req.arguments()
        );
    }

    /**
     * Sends cache query response.
     *
     * @param nodeId Node to send response.
     * @param res Cache query response.
     * @param timeout Message timeout.
     * @return {@code true} if response was sent, {@code false} otherwise.
     */
    private boolean sendQueryResponse(UUID nodeId, GridCacheQueryResponse res, long timeout) {
        ClusterNode node = cctx.node(nodeId);

        if (node == null)
            return false;

        int attempt = 1;

        IgniteCheckedException err = null;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Send query response: " + res);

                Object topic = topic(nodeId, res.requestId());

                cctx.io().sendOrderedMessage(
                    node,
                    topic,
                    res,
                    cctx.ioPolicy(),
                    timeout > 0 ? timeout : Long.MAX_VALUE);

                return true;
            }
            catch (ClusterTopologyCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send query response since node left grid [nodeId=" + nodeId +
                        ", res=" + res + "]");

                return false;
            }
            catch (IgniteCheckedException e) {
                if (err == null)
                    err = e;

                if (Thread.currentThread().isInterrupted())
                    break;

                if (attempt < RESEND_ATTEMPTS) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send queries response (will try again) [nodeId=" + nodeId + ", res=" +
                            res + ", attempt=" + attempt + ", err=" + e + "]");

                    if (!Thread.currentThread().isInterrupted())
                        try {
                            U.sleep(RESEND_FREQ);
                        }
                        catch (IgniteInterruptedCheckedException e1) {
                            U.error(log,
                                "Waiting for queries response resending was interrupted (response will not be sent) " +
                                "[nodeId=" + nodeId + ", response=" + res + "]", e1);

                            return false;
                        }
                }
                else {
                    U.error(log, "Failed to sender cache response [nodeId=" + nodeId + ", response=" + res + "]", err);

                    return false;
                }
            }

            attempt++;
        }

        return false;
    }

    /**
     * Processes cache query response.
     *
     * @param sndId Sender node id.
     * @param res Query response.
     */
    @SuppressWarnings("unchecked")
    private void processQueryResponse(UUID sndId, GridCacheQueryResponse res) {
        if (log.isDebugEnabled())
            log.debug("Received query response: " + res);

        GridCacheQueryFutureAdapter fut = getQueryFuture(res.requestId());

        if (fut != null)
            if (res.fields())
                ((GridCacheDistributedFieldsQueryFuture)fut).onPage(
                    sndId,
                    res.metadata(),
                    (Collection<Map<String, Object>>)((Collection)res.data()),
                    res.error(),
                    res.isFinished());
            else
                fut.onPage(sndId, res.data(), res.error(), res.isFinished());
        else if (!cancelled.contains(res.requestId()))
            U.warn(log, "Received response for finished or unknown query [rmtNodeId=" + sndId +
                ", res=" + res + ']');
    }

    /** {@inheritDoc} */
    @Override void onQueryFutureCanceled(long reqId) {
        cancelled.add(reqId);
    }

    /** {@inheritDoc} */
    @Override void onCancelAtStop() {
        super.onCancelAtStop();

        for (GridCacheQueryFutureAdapter fut : futs.values())
            try {
                fut.cancel();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to cancel running query future: " + fut, e);
            }

        U.interrupt(threads.values());
    }

    /** {@inheritDoc} */
    @Override void onWaitAtStop() {
        super.onWaitAtStop();

        // Wait till all requests will be finished.
        for (GridCacheQueryFutureAdapter fut : futs.values())
            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Received query error while waiting for query to finish [queryFuture= " + fut +
                        ", error= " + e + ']');
            }
    }

    /** {@inheritDoc} */
    @Override protected boolean onPageReady(boolean loc, GridCacheQueryInfo qryInfo,
        Collection<?> data, boolean finished, Throwable e) {
        GridCacheLocalQueryFuture<?, ?, ?> fut = qryInfo.localQueryFuture();

        if (loc)
            assert fut != null;

        if (e != null) {
            if (loc)
                fut.onPage(null, null, e, true);
            else
                sendQueryResponse(qryInfo.senderId(),
                    new GridCacheQueryResponse(cctx.cacheId(), qryInfo.requestId(), e, cctx.deploymentEnabled()),
                    qryInfo.query().timeout());

            return true;
        }

        if (loc)
            fut.onPage(null, data, null, finished);
        else {
            GridCacheQueryResponse res = new GridCacheQueryResponse(cctx.cacheId(), qryInfo.requestId(),
                /*finished*/false, /*fields*/false, cctx.deploymentEnabled());

            res.data(data);
            res.finished(finished);

            if (!sendQueryResponse(qryInfo.senderId(), res, qryInfo.query().timeout()))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override protected boolean onFieldsPageReady(boolean loc, GridCacheQueryInfo qryInfo,
        @Nullable List<GridQueryFieldMetadata> metadata,
        @Nullable Collection<?> entities,
        @Nullable Collection<?> data,
        boolean finished, @Nullable Throwable e) {
        assert qryInfo != null;

        if (e != null) {
            if (loc) {
                GridCacheLocalFieldsQueryFuture fut = (GridCacheLocalFieldsQueryFuture)qryInfo.localQueryFuture();

                fut.onPage(null, null, null, e, true);
            }
            else
                sendQueryResponse(qryInfo.senderId(),
                    new GridCacheQueryResponse(cctx.cacheId(), qryInfo.requestId(), e, cctx.deploymentEnabled()),
                    qryInfo.query().timeout());

            return true;
        }

        if (loc) {
            GridCacheLocalFieldsQueryFuture fut = (GridCacheLocalFieldsQueryFuture)qryInfo.localQueryFuture();

            fut.onPage(null, metadata, data, null, finished);
        }
        else {
            GridCacheQueryResponse res = new GridCacheQueryResponse(cctx.cacheId(), qryInfo.requestId(),
                finished, qryInfo.reducer() == null, cctx.deploymentEnabled());

            res.metadata(metadata);
            res.data(entities != null ? entities : data);

            if (!sendQueryResponse(qryInfo.senderId(), res, qryInfo.query().timeout()))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public CacheQueryFuture<?> queryDistributed(GridCacheQueryBean qry, final Collection<ClusterNode> nodes) {
        assert cctx.config().getCacheMode() != LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing distributed query: " + qry);

        long reqId = cctx.io().nextIoId();

        final GridCacheDistributedQueryFuture<K, V, ?> fut =
            new GridCacheDistributedQueryFuture<>(cctx, reqId, qry, nodes);

        try {
            qry.query().validate();

            String clsName = qry.query().queryClassName();

            final GridCacheQueryRequest req = new GridCacheQueryRequest(
                cctx.cacheId(),
                reqId,
                cctx.name(),
                qry.query().type(),
                false,
                qry.query().clause(),
                clsName,
                qry.query().scanFilter(),
                qry.query().partition(),
                qry.reducer(),
                qry.transform(),
                qry.query().pageSize(),
                qry.query().includeBackups(),
                qry.arguments(),
                false,
                qry.query().keepBinary(),
                qry.query().subjectId(),
                qry.query().taskHash(),
                queryTopologyVersion(),
                // Force deployment anyway if scan query is used.
                cctx.deploymentEnabled() || (qry.query().scanFilter() != null && cctx.gridDeploy().enabled()));

            addQueryFuture(req.id(), fut);

            final Object topic = topic(cctx.nodeId(), req.id());

            cctx.io().addOrderedHandler(topic, resHnd);

            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> fut) {
                    cctx.io().removeOrderedHandler(topic);
                }
            });

            sendRequest(fut, req, nodes);
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "serial"})
    @Override public GridCloseableIterator scanQueryDistributed(final GridCacheQueryAdapter qry,
        Collection<ClusterNode> nodes) throws IgniteCheckedException {
        assert !cctx.isLocal() : cctx.name();
        assert qry.type() == GridCacheQueryType.SCAN: qry;

        GridCloseableIterator locIter0 = null;

        for (ClusterNode node : nodes) {
            if (node.isLocal()) {
                locIter0 = scanQueryLocal(qry, false);

                Collection<ClusterNode> rmtNodes = new ArrayList<>(nodes.size() - 1);

                for (ClusterNode n : nodes) {
                    // Equals by reference can be used here.
                    if (n != node)
                        rmtNodes.add(n);
                }

                nodes = rmtNodes;

                break;
            }
        }

        final GridCloseableIterator locIter = locIter0;

        final GridCacheQueryBean bean = new GridCacheQueryBean(qry, null, qry.<K, V>transform(), null);

        final CacheQueryFuture fut = (CacheQueryFuture)queryDistributed(bean, nodes);

        return new GridCloseableIteratorAdapter() {
            /** */
            private Object cur;

            @Override protected Object onNext() throws IgniteCheckedException {
                if (!onHasNext())
                    throw new NoSuchElementException();

                Object e = cur;

                cur = null;

                return e;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (cur != null)
                    return true;

                if (locIter != null && locIter.hasNextX())
                    cur = locIter.nextX();

                return cur != null || (cur = fut.next()) != null;
            }

            @Override protected void onClose() throws IgniteCheckedException {
                super.onClose();

                if (locIter != null)
                    locIter.close();

                if (fut != null)
                    fut.cancel();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void loadPage(long id, GridCacheQueryAdapter<?> qry, Collection<ClusterNode> nodes, boolean all) {
        assert cctx.config().getCacheMode() != LOCAL;
        assert qry != null;
        assert nodes != null;

        GridCacheDistributedQueryFuture<?, ?, ?> fut = futs.get(id);

        assert fut != null;

        try {
            GridCacheQueryRequest req = new GridCacheQueryRequest(
                cctx.cacheId(),
                id,
                cctx.name(),
                qry.pageSize(),
                qry.includeBackups(),
                fut.fields(),
                all,
                qry.keepBinary(),
                qry.subjectId(),
                qry.taskHash(),
                queryTopologyVersion(),
                // Force deployment anyway if scan query is used.
                cctx.deploymentEnabled() || (qry.scanFilter() != null && cctx.gridDeploy().enabled()));

            sendRequest(fut, req, nodes);
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQueryFuture<?> queryFieldsLocal(GridCacheQueryBean qry) {
        assert cctx.config().getCacheMode() != LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing query on local node: " + qry);

        GridCacheLocalFieldsQueryFuture fut = new GridCacheLocalFieldsQueryFuture(cctx, qry);

        try {
            qry.query().validate();

            fut.execute();
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public CacheQueryFuture<?> queryFieldsDistributed(GridCacheQueryBean qry,
        Collection<ClusterNode> nodes) {
        assert cctx.config().getCacheMode() != LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing distributed query: " + qry);

        long reqId = cctx.io().nextIoId();

        final GridCacheDistributedFieldsQueryFuture fut =
            new GridCacheDistributedFieldsQueryFuture(cctx, reqId, qry, nodes);

        try {
            qry.query().validate();

            GridCacheQueryRequest req = new GridCacheQueryRequest(
                cctx.cacheId(),
                reqId,
                cctx.name(),
                qry.query().type(),
                true,
                qry.query().clause(),
                null,
                null,
                null,
                qry.reducer(),
                qry.transform(),
                qry.query().pageSize(),
                qry.query().includeBackups(),
                qry.arguments(),
                qry.query().includeMetadata(),
                qry.query().keepBinary(),
                qry.query().subjectId(),
                qry.query().taskHash(),
                queryTopologyVersion(),
                cctx.deploymentEnabled());

            addQueryFuture(req.id(), fut);

            final Object topic = topic(cctx.nodeId(), req.id());

            cctx.io().addOrderedHandler(topic, resHnd);

            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> fut) {
                    cctx.io().removeOrderedHandler(topic);
                }
            });

            sendRequest(fut, req, nodes);
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /**
     * Sends query request.
     *
     * @param fut Distributed future.
     * @param req Request.
     * @param nodes Nodes.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("unchecked")
    private void sendRequest(
        final GridCacheDistributedQueryFuture<?, ?, ?> fut,
        final GridCacheQueryRequest req,
        Collection<ClusterNode> nodes
    ) throws IgniteCheckedException {
        assert fut != null;
        assert req != null;
        assert nodes != null;

        final UUID locNodeId = cctx.localNodeId();

        ClusterNode locNode = null;

        Collection<ClusterNode> rmtNodes = null;

        for (ClusterNode n : nodes) {
            if (n.id().equals(locNodeId))
                locNode = n;
            else {
                if (rmtNodes == null)
                    rmtNodes = new ArrayList<>(nodes.size());

                rmtNodes.add(n);
            }
        }

        // Request should be sent to remote nodes before the query is processed on the local node.
        // For example, a remote reducer has a state, we should not serialize and then send
        // the reducer changed by the local node.
        if (!F.isEmpty(rmtNodes)) {
            cctx.io().safeSend(rmtNodes, req, cctx.ioPolicy(), new P1<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    fut.onNodeLeft(node.id());

                    return !fut.isDone();
                }
            });
        }

        if (locNode != null) {
            cctx.closures().callLocalSafe(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    req.beforeLocalExecution(cctx);

                    processQueryRequest(locNodeId, req);

                    return null;
                }
            });
        }
    }

    /**
     * Gets topic for ordered response messages.
     *
     * @param nodeId Node ID.
     * @param reqId Request ID.
     * @return Topic.
     */
    private Object topic(UUID nodeId, long reqId) {
        return TOPIC_CACHE.topic(TOPIC_PREFIX, nodeId, reqId);
    }

    /**
     * Cancel message ID.
     */
    private static class CancelMessageId implements Comparable<CancelMessageId> {
        /** Message ID. */
        private long reqId;

        /** Node ID. */
        private UUID nodeId;

        /**
         * @param reqId Message ID.
         * @param nodeId Node ID.
         */
        private CancelMessageId(long reqId, UUID nodeId) {
            this.reqId = reqId;
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(CancelMessageId m) {
            int res = Long.compare(reqId, m.reqId);

            if (res == 0)
                res = m.nodeId.compareTo(nodeId);

            return res;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean equals(Object obj) {
            if (obj == this)
                return true;

            CancelMessageId other = (CancelMessageId)obj;

            return reqId == other.reqId && nodeId.equals(other.nodeId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * ((int)(reqId ^ (reqId >>> 32))) + nodeId.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CancelMessageId.class, this);
        }
    }
}
