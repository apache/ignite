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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
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

    /** Prefix of cluster wide query id. */
    private String prefixClusterWideQryId = null;

    /** Context. */
    private GridKernalContext ctx;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public RunningQueryManager(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Register running query.
     *
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param loc Local query flag.
     * @param cancel Query cancel. Should be passed in case query is cancelable, or {@code null} otherwise.
     * @return Registered RunningQueryInfo.
     */
    public GridRunningQueryInfo register(String qry, GridCacheQueryType qryType, String schemaName,
        boolean loc, @Nullable GridQueryCancel cancel) {
        long qryId = qryIdGen.incrementAndGet();

        String clusterWideQryId = clusterWideQueryId(qryId);

        GridRunningQueryInfo run = new GridRunningQueryInfo(
            qryId,
            clusterWideQryId,
            qry,
            qryType,
            schemaName,
            System.currentTimeMillis(),
            cancel,
            loc
        );

        GridRunningQueryInfo preRun = runs.putIfAbsent(qryId, run);

        assert preRun == null : "Running query already registered [prev_qry=" + preRun + ", newQry=" + run + ']';

        return run;
    }

    /**
     * @param qryId Local node query id.
     * @return Cluster wide query id.
     */
    @NotNull private String clusterWideQueryId(long qryId) {
        if (prefixClusterWideQryId == null) {
            long nodeOrder = ctx.cluster().get().localNode().order();

            prefixClusterWideQryId = Long.toHexString(nodeOrder) + "X";
        }

        return prefixClusterWideQryId + Long.toHexString(qryId);
    }

    /**
     * Unregister running query.
     *
     * @param runningQryInfo Running query info..
     * @return Unregistered running query info. {@code null} in case running query is not registered.
     */
    @Nullable public GridRunningQueryInfo unregister(@Nullable GridRunningQueryInfo runningQryInfo) {
        return (runningQryInfo != null) ? unregister(runningQryInfo.id()) : null;
    }

    /**
     * Unregister running query.
     *
     * @param qryId Query id.
     * @return Unregistered running query info. {@code null} in case running query with give id wasn't found.
     */
    @Nullable public GridRunningQueryInfo unregister(Long qryId) {
        if (qryId == null)
            return null;

        return runs.remove(qryId);
    }

    /**
     * Return SQL queries which executing right now.
     *
     * @return List of SQL running queries.
     */
    public List<GridRunningQueryInfo> runningSqlQueries() {
        List<GridRunningQueryInfo> res = new ArrayList<>();

        for (GridRunningQueryInfo runningQryInfo : runs.values()) {
            if (isSqlQuery(runningQryInfo))
                res.add(runningQryInfo);
        }

        return res;
    }

    /**
     * Check belongs running query to an SQL type.
     *
     * @param runningQryInfo
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
        for (GridRunningQueryInfo r : runs.values()) {
            try {
                unregister(r.id());

                r.cancel();
            }
            catch (Exception ignore) {
                // No-op.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RunningQueryManager.class, this);
    }
}
