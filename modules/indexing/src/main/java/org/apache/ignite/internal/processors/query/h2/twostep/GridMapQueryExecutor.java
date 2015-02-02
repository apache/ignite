/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.h2.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.indexing.*;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.h2.jdbc.*;
import org.h2.result.*;
import org.h2.value.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Map query executor.
 */
public class GridMapQueryExecutor {
    /** */
    private static final Field RESULT_FIELD;

    /**
     * Initialize.
     */
    static {
        try {
            RESULT_FIELD = JdbcResultSet.class.getDeclaredField("result");

            RESULT_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new IllegalStateException("Check H2 version in classpath.", e);
        }
    }

    /** */
    private IgniteLogger log;

    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteH2Indexing h2;

    /** */
    private ConcurrentMap<UUID, ConcurrentMap<Long, QueryResults>> qryRess = new ConcurrentHashMap8<>();

    /**
     * @param ctx Context.
     * @param h2 H2 Indexing.
     * @throws IgniteCheckedException If failed.
     */
    public void start(final GridKernalContext ctx, IgniteH2Indexing h2) throws IgniteCheckedException {
        this.ctx = ctx;
        this.h2 = h2;

        log = ctx.log(GridMapQueryExecutor.class);

        // TODO handle node failures.

        ctx.io().addUserMessageListener(GridTopic.TOPIC_QUERY, new IgniteBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                assert msg != null;

                ClusterNode node = ctx.discovery().node(nodeId);

                if (msg instanceof GridQueryRequest)
                    executeLocalQuery(node, (GridQueryRequest)msg);
                else if (msg instanceof GridNextPageRequest)
                    sendNextPage(node, (GridNextPageRequest)msg);

                return true;
            }
        });
    }

    /**
     * @param node Node.
     * @param req Query request.
     */
    private void executeLocalQuery(ClusterNode node, GridQueryRequest req) {
        h2.setFilters(new IndexingQueryFilter() {
            @Nullable @Override public <K, V> IgniteBiPredicate<K, V> forSpace(String spaceName) {
                final GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(spaceName);

                if (cache.context().isReplicated() || cache.configuration().getBackups() == 0)
                    return null;

                return new IgniteBiPredicate<K, V>() {
                    @Override public boolean apply(K k, V v) {
                        return cache.context().affinity().primary(ctx.discovery().localNode(), k, -1);
                    }
                };
            }
        });

        try {
            QueryResults qr = new QueryResults(req.requestId(), req.queries().size());

            ConcurrentMap<Long, QueryResults> nodeRess = qryRess.get(node.id());

            if (nodeRess == null) {
                nodeRess = new ConcurrentHashMap8<>();

                ConcurrentMap<Long, QueryResults> old = qryRess.putIfAbsent(node.id(), nodeRess);

                if (old != null)
                    nodeRess = old;
            }

            QueryResults old = nodeRess.putIfAbsent(req.requestId(), qr);

            assert old == null;

            // Prepare snapshots for all the needed tables before actual run.
            for (GridCacheSqlQuery qry : req.queries()) {
                // TODO
            }

            // Run queries.
            int i = 0;

            for (GridCacheSqlQuery qry : req.queries()) {
                ResultSet rs = h2.executeSqlQueryWithTimer(h2.connectionForSpace(null), qry.query(),
                    F.asList(qry.parameters()));

                assert rs instanceof JdbcResultSet : rs.getClass();

                ResultInterface res = (ResultInterface)RESULT_FIELD.get(rs);

                qr.results[i] = res;
                qr.resultSets[i] = rs;

                // Send the first page.
                sendNextPage(node, qr, i, req.pageSize(), res.getRowCount());

                i++;
            }
        }
        catch (Throwable e) {
            sendError(node, req.requestId(), e);
        }
        finally {
            h2.setFilters(null);
        }
    }

    /**
     * @param node Node.
     * @param qryReqId Query request ID.
     * @param err Error.
     */
    private void sendError(ClusterNode node, long qryReqId, Throwable err) {
        try {
            ctx.io().sendUserMessage(F.asList(node), new GridQueryFailResponse(qryReqId, err));
        }
        catch (IgniteCheckedException e) {
            e.addSuppressed(err);

            log.error("Failed to send error message.", e);
        }
    }

    /**
     * @param node Node.
     * @param req Request.
     */
    private void sendNextPage(ClusterNode node, GridNextPageRequest req) {
        ConcurrentMap<Long, QueryResults> nodeRess = qryRess.get(node.id());

        QueryResults qr = nodeRess == null ? null : nodeRess.get(req.queryRequestId());

        if (qr == null)
            sendError(node, req.queryRequestId(),
                new IllegalStateException("No query result found for request: " + req));
        else
            sendNextPage(node, qr, req.query(), req.pageSize(), -1);
    }

    /**
     * @param node Node.
     * @param qr Query results.
     * @param qry Query.
     * @param pageSize Page size.
     * @param allRows All rows count.
     */
    private void sendNextPage(ClusterNode node, QueryResults qr, int qry, int pageSize, int allRows) {
        int page;

        List<Value[]> rows = new ArrayList<>(Math.min(64, pageSize));

        ResultInterface res = qr.results[qry];

        assert res != null;

        boolean last = false;

        synchronized (res) {
            page = qr.pages[qry]++;

            for (int i = 0 ; i < pageSize; i++) {
                if (!res.next()) {
                    last = true;

                    break;
                }

                rows.add(res.currentRow());
            }
        }

        try {
            ctx.io().sendUserMessage(F.asList(node),
                new GridNextPageResponse(qr.qryReqId, qry, page, allRows, last, rows),
                GridTopic.TOPIC_QUERY, false, 0);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send message.", e);

            throw new IgniteException(e);
        }
    }

    /**
     *
     */
    private static class QueryResults {
        /** */
        private long qryReqId;

        /** */
        private ResultInterface[] results;

        /** */
        private ResultSet[] resultSets;

        /** */
        private int[] pages;

        /**
         * @param qryReqId Query request ID.
         * @param qrys Queries.
         */
        private QueryResults(long qryReqId, int qrys) {
            this.qryReqId = qryReqId;

            results = new ResultInterface[qrys];
            resultSets = new ResultSet[qrys];
            pages = new int[qrys];
        }
    }
}
