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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Keep information about all running queries.
 */
public class RunningQueryManager {
    /** Keep registered user queries. */
    private final ConcurrentMap<Long, GridRunningQueryInfo> userQueriesRuns = new ConcurrentHashMap<>();

    /** Keep registered system running queries. */
    private final ConcurrentMap<Long, GridRunningQueryInfo> sysQueriesRuns = new ConcurrentHashMap<>();

    /** Unique id for queries on single node. */
    private AtomicLong qryIdGen = new AtomicLong();

    /**
     * Register running query.
     *
     * @param userQryId Initial user query id. {@code null} in case it's new query.
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param cancel Query cancel.
     * @param loc Local query flag.
     * @return Registered RunningQueryInfo.
     */
    public GridRunningQueryInfo registerRunningQuery(Long userQryId, String qry, GridCacheQueryType qryType,
        String schemaName, GridQueryCancel cancel, boolean loc) {

        return userQryId == null ?
            registerUserRunningQuery(qry, qryType, schemaName, cancel, loc)
            : registerSystemRunningQuery(userQryId, qry, qryType, schemaName, loc);
    }

    /**
     * Register running query.
     *
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param cancel Query cancel.
     * @param loc Local query flag.
     * @return Registered RunningQueryInfo.
     */
    public GridRunningQueryInfo registerUserRunningQuery(String qry, GridCacheQueryType qryType, String schemaName,
        GridQueryCancel cancel, boolean loc) {
        long qryId = createUniqueQueryId();

        return registerRunningQuery(userQueriesRuns, qryId, qryId, qry, qryType, schemaName, cancel, loc);
    }

    /**
     * Register running query.
     *
     * @param userQryId User query id.
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param loc Local query flag.
     * @return Registered RunningQueryInfo.
     */
    public GridRunningQueryInfo registerSystemRunningQuery(long userQryId, String qry, GridCacheQueryType qryType,
        String schemaName, boolean loc) {

        long qryId = createUniqueQueryId();

        GridQueryCancel cancel = new GridQueryCancel();

        return registerRunningQuery(sysQueriesRuns, qryId, userQryId, qry, qryType, schemaName, cancel, loc);
    }

    /**
     * Register running query.
     *
     * @param registerMap Map to register running query information.
     * @param qryId Query id.
     * @param userQryId User query id.
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param cancel Query cancel.
     * @param loc Local query flag.
     * @return Registered RunningQueryInfo.
     */
    private GridRunningQueryInfo registerRunningQuery(ConcurrentMap<Long, GridRunningQueryInfo> registerMap,
        long qryId, long userQryId, String qry, GridCacheQueryType qryType, String schemaName, GridQueryCancel cancel,
        boolean loc) {

        long startTime = U.currentTimeMillis();

        GridRunningQueryInfo runningQryInfo = new GridRunningQueryInfo(qryId, userQryId, qry, qryType,
            schemaName, startTime, cancel, loc);

        GridRunningQueryInfo prevRunningQryInfo = registerMap.putIfAbsent(qryId, runningQryInfo);

        assert prevRunningQryInfo == null : "Running query already registered [prev_qry=" + prevRunningQryInfo +
            ", newQry=" + runningQryInfo;

        return runningQryInfo;
    }

    /**
     * Generate unique query id.
     *
     * @return Generated unique query id.
     */
    public long createUniqueQueryId() {
        return qryIdGen.incrementAndGet();
    }

    /**
     * Unregister running query.
     *
     * @param qryId Query id.
     * @return Unregistered running query info. {@code null} in case running query with give id wasn't found.
     */
    @Nullable public GridRunningQueryInfo unregisterRunningQuery(long qryId) {
        GridRunningQueryInfo rmv = userQueriesRuns.remove(qryId);

        if (rmv == null)
            return sysQueriesRuns.remove(qryId);

        return null;
    }

    /**
     * Unregister running query.
     *
     * @param runningQryInfo Running query info..
     * @return Unregistered running query info. {@code null} in case running query is not registered.
     */
    @Nullable public GridRunningQueryInfo unregisterRunningQuery(@Nullable GridRunningQueryInfo runningQryInfo) {
        return (runningQryInfo != null) ? unregisterRunningQuery(runningQryInfo.id()) : null;
    }

    /**
     * Return long running user queries.
     *
     * @param duration Duration of long query.
     * @return List of queries which running longer than given duration.
     */
    public Collection<GridRunningQueryInfo> longRunningUserQueries(long duration, boolean includeSysQrys) {
        Collection<GridRunningQueryInfo> res = new ArrayList<>();

        long curTime = U.currentTimeMillis();

        for (GridRunningQueryInfo runningQryInfo : userQueriesRuns.values()) {
            if (runningQryInfo.longQuery(curTime, duration))
                res.add(runningQryInfo);
        }

        if (includeSysQrys) {
            for (GridRunningQueryInfo runningQryInfo : sysQueriesRuns.values()) {
                if (runningQryInfo.longQuery(curTime, duration))
                    res.add(runningQryInfo);
            }
        }

        return res;
    }

    /**
     * Return long running system queries.
     *
     * @param duration Duration of long query.
     * @return List of queries which running longer than given duration.
     */
    public Collection<GridRunningQueryInfo> longRunningSystemQueries(long duration) {
        Collection<GridRunningQueryInfo> res = new ArrayList<>();

        long curTime = U.currentTimeMillis();

        for (GridRunningQueryInfo runningQryInfo : sysQueriesRuns.values()) {
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
        GridRunningQueryInfo run = userQueriesRuns.getOrDefault(qryId, sysQueriesRuns.get(qryId));

        if (run != null)
            run.cancel();
    }
}
