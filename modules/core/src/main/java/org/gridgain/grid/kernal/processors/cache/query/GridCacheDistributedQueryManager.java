/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;

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
    private GridBiInClosure<UUID,GridCacheQueryResponse<K,V>> resHnd = new CI2<UUID, GridCacheQueryResponse<K, V>>() {
        @Override public void apply(UUID nodeId, GridCacheQueryResponse<K, V> res) {
            processQueryResponse(nodeId, res);
        }
    };

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        super.start0();

        assert cctx.config().getCacheMode() != LOCAL;

        cctx.io().addHandler(GridCacheQueryRequest.class, new CI2<UUID, GridCacheQueryRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridCacheQueryRequest<K, V> req) {
                processQueryRequest(nodeId, req);
            }
        });

        cctx.events().addListener(new GridLocalEventListener() {
            @Override public void onEvent(GridEvent evt) {
                GridDiscoveryEvent discoEvt = (GridDiscoveryEvent)evt;

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
                removeQueryIterator(sndId, req.id());
        }
        else {
            if (!cancelIds.contains(new CancelMessageId(req.id(), sndId))) {
                if (!F.eq(req.cacheName(), cctx.name())) {
                    GridCacheQueryResponse res = new GridCacheQueryResponse(req.id(),
                        new GridException("Received request for incorrect cache [expected=" + cctx.name() +
                            ", actual=" + req.cacheName()));

                    sendQueryResponse(sndId, res, 0);
                }
                else {
                    threads.put(req.id(), Thread.currentThread());

                    try {
                        GridCacheQueryInfo info = distributedQueryInfo(sndId, req);

                        if (req.fields())
                            runFieldsQuery(info);
                        else
                            runQuery(info);
                    }
                    catch (Throwable e) {
                        U.error(log(), "Failed to run query.", e);

                        sendQueryResponse(sndId, new GridCacheQueryResponse(req.id(), e.getCause()), 0);
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
    private GridCacheQueryInfo distributedQueryInfo(UUID sndId, GridCacheQueryRequest<K, V> req) {
        GridPredicate<GridCacheEntry<Object, Object>> prjPred = req.projectionFilter() == null ?
            F.<GridCacheEntry<Object, Object>>alwaysTrue() : req.projectionFilter();

        GridReducer<Object, Object> rdc = req.reducer();
        GridClosure<Object, Object> trans = req.transformer();

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
        GridNode node = cctx.node(nodeId);

        if (node == null)
            return false;

        int attempt = 1;

        GridException err = null;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Send query response: " + res);

                Object topic = topic(nodeId, res.requestId());

                cctx.io().sendOrderedMessage(
                    node,
                    topic,
                    cctx.io().messageId(topic, nodeId),
                    res,
                    timeout > 0 ? timeout : Long.MAX_VALUE);

                return true;
            }
            catch (GridTopologyException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send query response since node left grid [nodeId=" + nodeId +
                        ", res=" + res + "]");

                return false;
            }
            catch (GridException e) {
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
                        catch (GridInterruptedException e1) {
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

    /** {@inheritDoc} */
    @Override protected void removeQueryIterator(@Nullable UUID sndId, long reqId) {
        super.removeQueryIterator(sndId, reqId);

        if (sndId != null) {
            Object topic = topic(sndId, reqId);

            cctx.io().removeMessageId(topic);
        }
    }

    /** {@inheritDoc} */
    @Override protected void removeFieldsQueryResult(@Nullable UUID sndId, long reqId) {
        super.removeFieldsQueryResult(sndId, reqId);

        if (sndId != null) {
            Object topic = topic(sndId, reqId);

            cctx.io().removeMessageId(topic);
        }
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
            catch (GridException e) {
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
            catch (GridException e) {
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
                    new GridCacheQueryResponse<K, V>(qryInfo.requestId(), e),
                    qryInfo.query().timeout());

            return true;
        }

        if (loc)
            fut.onPage(null, data, null, finished);
        else {
            GridCacheQueryResponse<K, V> res = new GridCacheQueryResponse<>(qryInfo.requestId(), false, false);

            res.data(data);
            res.finished(finished);

            if (!sendQueryResponse(qryInfo.senderId(), res, qryInfo.query().timeout()))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean onFieldsPageReady(boolean loc, GridCacheQueryInfo qryInfo,
        @Nullable List<GridIndexingFieldMetadata> metadata,
        @Nullable Collection<List<GridIndexingEntity<?>>> entities,
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
                    new GridCacheQueryResponse<K, V>(qryInfo.requestId(), e),
                    qryInfo.query().timeout());

            return true;
        }

        if (loc) {
            GridCacheLocalFieldsQueryFuture fut = (GridCacheLocalFieldsQueryFuture)qryInfo.localQueryFuture();

            fut.onPage(null, metadata, data, null, finished);
        }
        else {
            GridCacheQueryResponse<K, V> res = new GridCacheQueryResponse<>(qryInfo.requestId(), finished,
                qryInfo.reducer() == null);

            res.metadata(metadata);
            res.data(entities != null ? entities : data);

            if (!sendQueryResponse(qryInfo.senderId(), res, qryInfo.query().timeout()))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryFuture<?> queryLocal(GridCacheQueryBean qry) {
        assert cctx.config().getCacheMode() != LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing query on local node: " + qry);

        GridCacheLocalQueryFuture<K, V, ?> fut = new GridCacheLocalQueryFuture<>(cctx, qry);

        try {
            qry.query().validate();

            fut.execute();
        }
        catch (GridException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheQueryFuture<?> queryDistributed(GridCacheQueryBean qry, Collection<GridNode> nodes) {
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

            fut.listenAsync(new CI1<GridFuture<?>>() {
                @Override public void apply(GridFuture<?> fut) {
                    cctx.io().removeOrderedHandler(topic);
                }
            });

            sendRequest(fut, req, nodes);
        }
        catch (GridException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void loadPage(long id, GridCacheQueryAdapter<?> qry, Collection<GridNode> nodes, boolean all) {
        assert cctx.config().getCacheMode() != LOCAL;
        assert qry != null;
        assert nodes != null;

        GridCacheDistributedQueryFuture<?, ?, ?> fut = futs.get(id);

        assert fut != null;

        try {
            GridCacheQueryRequest<K, V> req = new GridCacheQueryRequest<>(id, cctx.name(), qry.pageSize(),
                qry.includeBackups(), fut.fields(), all, qry.keepPortable(), qry.subjectId(), qry.taskHash());

            sendRequest(fut, req, nodes);
        }
        catch (GridException e) {
            fut.onDone(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryFuture<?> queryFieldsLocal(GridCacheQueryBean qry) {
        assert cctx.config().getCacheMode() != LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing query on local node: " + qry);

        GridCacheLocalFieldsQueryFuture fut = new GridCacheLocalFieldsQueryFuture(cctx, qry);

        try {
            qry.query().validate();

            fut.execute();
        }
        catch (GridException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheQueryFuture<?> queryFieldsDistributed(GridCacheQueryBean qry,
        Collection<GridNode> nodes) {
        assert cctx.config().getCacheMode() != LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing distributed query: " + qry);

        long reqId = cctx.io().nextIoId();

        final GridCacheDistributedFieldsQueryFuture fut =
            new GridCacheDistributedFieldsQueryFuture(cctx, reqId, qry, nodes);

        try {
            qry.query().validate();

            GridCacheQueryRequest req = new GridCacheQueryRequest(
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

            fut.listenAsync(new CI1<GridFuture<?>>() {
                @Override public void apply(GridFuture<?> fut) {
                    cctx.io().removeOrderedHandler(topic);
                }
            });

            sendRequest(fut, req, nodes);
        }
        catch (GridException e) {
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
     * @throws GridException In case of error.
     */
    private void sendRequest(final GridCacheDistributedQueryFuture<?, ?, ?> fut,
        final GridCacheQueryRequest<K, V> req, Collection<GridNode> nodes) throws GridException {
        assert fut != null;
        assert req != null;
        assert nodes != null;

        final UUID locNodeId = cctx.localNodeId();

        GridNode locNode = F.find(nodes, null, F.localNode(locNodeId));

        Collection<? extends GridNode> remoteNodes = F.view(nodes, F.remoteNodes(locNodeId));

        // Request should be sent to remote nodes before the query is processed on the local node.
        // For example, a remote reducer has a state, we should not serialize and then send
        // the reducer changed by the local node.
        if (!remoteNodes.isEmpty()) {
            cctx.io().safeSend(remoteNodes, req, new P1<GridNode>() {
                @Override public boolean apply(GridNode node) {
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
    private class CancelMessageId implements Comparable<CancelMessageId> {
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
