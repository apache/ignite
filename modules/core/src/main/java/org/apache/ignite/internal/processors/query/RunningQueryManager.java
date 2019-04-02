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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;

/**
 * Keep information about all running queries.
 */
public class RunningQueryManager {
    /** Keep registered user queries. */
    private final ConcurrentMap<Long, GridRunningQueryInfo> runs = new ConcurrentHashMap<>();

    /** Unique id for queries on single node. */
    private final AtomicLong qryIdGen = new AtomicLong();

    /** Local node ID. */
    private final UUID localNodeId;

    /** History size. */
    private final int histSz;

    /** Query history tracker. */
    private volatile QueryHistoryTracker qryHistTracker;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public RunningQueryManager(GridKernalContext ctx) {
        localNodeId = ctx.localNodeId();

        histSz = ctx.config().getSqlQueryHistorySize();

        qryHistTracker = new QueryHistoryTracker(histSz);
    }

    /**
     * Register running query.
     *
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param loc Local query flag.
     * @param cancel Query cancel. Should be passed in case query is cancelable, or {@code null} otherwise.
     * @return Id of registered query.
     */
    public Long register(String qry, GridCacheQueryType qryType, String schemaName, boolean loc,
        @Nullable GridQueryCancel cancel) {
        Long qryId = qryIdGen.incrementAndGet();

        GridRunningQueryInfo run = new GridRunningQueryInfo(
            qryId,
            localNodeId,
            qry,
            qryType,
            schemaName,
            System.currentTimeMillis(),
            cancel,
            loc
        );

        GridRunningQueryInfo preRun = runs.putIfAbsent(qryId, run);

        assert preRun == null : "Running query already registered [prev_qry=" + preRun + ", newQry=" + run + ']';

        return qryId;
    }

    /**
     * Unregister running query.
     *
     * @param qryId Query id.
     * @param failed {@code true} In case query was failed.
     */
    public void unregister(Long qryId, boolean failed) {
        if (qryId == null)
            return;

        GridRunningQueryInfo qry = runs.remove(qryId);

        //We need to collect query history only for SQL queries.
        if (qry != null && isSqlQuery(qry)) {
            qry.runningFuture().onDone();

            qryHistTracker.collectMetrics(qry, failed);
        }
    }

    /**
     * Return SQL queries which executing right now.
     *
     * @return List of SQL running queries.
     */
    public List<GridRunningQueryInfo> runningSqlQueries() {
        List<GridRunningQueryInfo> res = new ArrayList<>();

        for (GridRunningQueryInfo run : runs.values()) {
            if (isSqlQuery(run))
                res.add(run);
        }

        return res;
    }

    /**
     * Check belongs running query to an SQL type.
     *
     * @param runningQryInfo Running query info object.
     * @return {@code true} For SQL or SQL_FIELDS query type.
     */
    private boolean isSqlQuery(GridRunningQueryInfo runningQryInfo){
        return runningQryInfo.queryType() == SQL_FIELDS || runningQryInfo.queryType() == SQL;
    }

    /**
     * Return long running user queries.
     *
     * @param duration Duration of long query.
     * @return Collection of queries which running longer than given duration.
     */
    public Collection<GridRunningQueryInfo> longRunningQueries(long duration) {
        Collection<GridRunningQueryInfo> res = new ArrayList<>();

        long curTime = System.currentTimeMillis();

        for (GridRunningQueryInfo runningQryInfo : runs.values()) {
            if (runningQryInfo.longQuery(curTime, duration))
                res.add(runningQryInfo);
        }

        return res;
    }

    /**
     * Cancel query.
     *
     * @param qryId Query id.
     */
    public void cancel(Long qryId) {
        GridRunningQueryInfo run = runs.get(qryId);

        if (run != null)
            run.cancel();
    }

    /**
     * Cancel all executing queries and deregistering all of them.
     */
    public void stop() {
        Iterator<GridRunningQueryInfo> iter = runs.values().iterator();

        while (iter.hasNext()) {
            try {
                GridRunningQueryInfo r = iter.next();

                iter.remove();

                r.cancel();
            }
            catch (Exception ignore) {
                // No-op.
            }
        }
    }

    /**
     * Gets query history statistics. Size of history could be configured via {@link
     * IgniteConfiguration#setSqlQueryHistorySize(int)}
     *
     * @return Queries history statistics aggregated by query text, schema and local flag.
     */
    public Map<QueryHistoryMetricsKey, QueryHistoryMetrics> queryHistoryMetrics() {
        return qryHistTracker.queryHistoryMetrics();
    }

    /**
     * Gets info about running query by their id.
     * @param qryId
     * @return Running query info or {@code null} in case no running query for given id.
     */
    public @Nullable GridRunningQueryInfo runningQueryInfo(Long qryId) {
        return runs.get(qryId);
    }

    /**
     * Reset query history metrics.
     */
    public void resetQueryHistoryMetrics() {
        qryHistTracker = new QueryHistoryTracker(histSz);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RunningQueryManager.class, this);
    }
}
