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

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridTopic.*;

/**
 * Distributed query manager.
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
    private IgniteBiInClosure<UUID,GridCacheQueryResponse<K,V>> resHnd = new CI2<UUID, GridCacheQueryResponse<K, V>>() {
        @Override public void apply(UUID nodeId, GridCacheQueryResponse<K, V> res) {
            processQueryResponse(nodeId, res);
        }
    };

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        super.start0();

        assert cctx.config().getCacheMode() != LOCAL;

        cctx.io().addHandler(cctx.cacheId(), GridCacheQueryRequest.class, new CI2<UUID, GridCacheQueryRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridCacheQueryRequest<K, V> req) {
                processQueryRequest(nodeId, req);
            }
        });

        cctx.events().addListener(new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                for (GridCacheDistributedQueryFuture fut : futs.values())
                    fut.onNodeLeft(discoEvt.eventNode().id());
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);
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
                            ", actual=" + req.cacheName()));

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

                        sendQueryResponse(sndId, new GridCacheQueryResponse(cctx.cacheId(), req.id(), e.getCause()), 0);
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
     * @throws ClassNotFoundException If class not found.
     */
    @Nullable private GridCacheQueryInfo distributedQueryInfo(UUID sndId, GridCacheQueryRequest<K, V> req)
        throws ClassNotFoundException {
        IgnitePredicate<Cache.Entry<Object, Object>> prjPred = req.projectionFilter() == null ?
            F.<Cache.Entry<Object, Object>>alwaysTrue() : req.projectionFilter();

        IgniteReducer<Object, Object> rdc = req.reducer();
        IgniteClosure<Object, Object> trans = req.transformer();

        ClusterNode sndNode = cctx.node(sndId);

        if (sndNode == null)
            return null;

        GridCacheQueryAdapter<?> qry =
            new GridCacheQueryAdapter<>(
                cctx,
                prjPred,
                req.type(),
                log,
                req.pageSize(),
                0,
                false,
                req.includeBackups(),
                false,
                null,
                req.keyValueFilter(),
                req.className(),
                req.clause(),
                req.includeMetaData(),
                req.keepPortable(),
                req.subjectId(),
                req.taskHash()
            );

        return new GridCacheQueryInfo(
            false,
            prjPred,
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
    private boolean sendQueryResponse(UUID nodeId, GridCacheQueryResponse<K, V> res, long timeout) {
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
                    sndId, res.metadata(), res.data(), res.error(), res.isFinished());
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
                    new GridCacheQueryResponse<K, V>(cctx.cacheId(), qryInfo.requestId(), e),
                    qryInfo.query().timeout());

            return true;
        }

        if (loc)
            fut.onPage(null, data, null, finished);
        else {
            GridCacheQueryResponse<K, V> res = new GridCacheQueryResponse<>(cctx.cacheId(), qryInfo.requestId(),
                /*finished*/false, /*fields*/false);

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
                    new GridCacheQueryResponse<K, V>(cctx.cacheId(), qryInfo.requestId(), e),
                    qryInfo.query().timeout());

            return true;
        }

        if (loc) {
            GridCacheLocalFieldsQueryFuture fut = (GridCacheLocalFieldsQueryFuture)qryInfo.localQueryFuture();

            fut.onPage(null, metadata, data, null, finished);
        }
        else {
            GridCacheQueryResponse<K, V> res = new GridCacheQueryResponse<>(cctx.cacheId(), qryInfo.requestId(),
                finished, qryInfo.reducer() == null);

            res.metadata(metadata);
            res.data(entities != null ? entities : data);

            if (!sendQueryResponse(qryInfo.senderId(), res, qryInfo.query().timeout()))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public CacheQueryFuture<?> queryLocal(GridCacheQueryBean qry) {
        assert cctx.config().getCacheMode() != LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing query on local node: " + qry);

        GridCacheLocalQueryFuture<K, V, ?> fut = new GridCacheLocalQueryFuture<>(cctx, qry);

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
    @Override public CacheQueryFuture<?> queryDistributed(GridCacheQueryBean qry, Collection<ClusterNode> nodes) {
        assert cctx.config().getCacheMode() != LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing distributed query: " + qry);

        long reqId = cctx.io().nextIoId();

        final GridCacheDistributedQueryFuture<K, V, ?> fut =
            new GridCacheDistributedQueryFuture<>(cctx, reqId, qry, nodes);

        try {
            qry.query().validate();

            String clsName = qry.query().queryClassName();

            GridCacheQueryRequest req = new GridCacheQueryRequest(
                cctx.cacheId(),
                reqId,
                cctx.name(),
                qry.query().type(),
                false,
                qry.query().clause(),
                clsName,
                qry.query().scanFilter(),
                qry.query().projectionFilter(),
                qry.reducer(),
                qry.transform(),
                qry.query().pageSize(),
                qry.query().includeBackups(),
                qry.arguments(),
                false,
                qry.query().keepPortable(),
                qry.query().subjectId(),
                qry.query().taskHash());

            addQueryFuture(req.id(), fut);

            final Object topic = topic(cctx.nodeId(), req.id());

            cctx.io().addOrderedHandler(topic, resHnd);

            fut.listenAsync(new CI1<IgniteInternalFuture<?>>() {
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
    @Override public void loadPage(long id, GridCacheQueryAdapter<?> qry, Collection<ClusterNode> nodes, boolean all) {
        assert cctx.config().getCacheMode() != LOCAL;
        assert qry != null;
        assert nodes != null;

        GridCacheDistributedQueryFuture<?, ?, ?> fut = futs.get(id);

        assert fut != null;

        try {
            GridCacheQueryRequest<K, V> req = new GridCacheQueryRequest<>(
                cctx.cacheId(),
                id,
                cctx.name(),
                qry.pageSize(),
                qry.includeBackups(),
                fut.fields(),
                all,
                qry.keepPortable(),
                qry.subjectId(),
                qry.taskHash());

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
                qry.query().projectionFilter(),
                qry.reducer(),
                qry.transform(),
                qry.query().pageSize(),
                qry.query().includeBackups(),
                qry.arguments(),
                qry.query().includeMetadata(),
                qry.query().keepPortable(),
                qry.query().subjectId(),
                qry.query().taskHash());

            addQueryFuture(req.id(), fut);

            final Object topic = topic(cctx.nodeId(), req.id());

            cctx.io().addOrderedHandler(topic, resHnd);

            fut.listenAsync(new CI1<IgniteInternalFuture<?>>() {
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
        final GridCacheQueryRequest<K, V> req,
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
            if (m.reqId == reqId)
                return m.nodeId.compareTo(nodeId);

            return reqId < m.reqId ? -1 : 1;
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
