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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Query cache.
 */
public class QueryCache {
    /** Default size for update plan cache. */
    private static final int PLAN_CACHE_SIZE = 1024;

    /** Cached value of {@code IgniteSystemProperties.IGNITE_ALLOW_DML_INSIDE_TRANSACTION}. */
    private static final boolean DML_ALLOWED =
        Boolean.getBoolean(IgniteSystemProperties.IGNITE_ALLOW_DML_INSIDE_TRANSACTION);

    /** Indexing. */
    private final IgniteH2Indexing idx;

    /** */
    private volatile GridBoundedConcurrentLinkedHashMap<H2TwoStepCachedQueryKey, H2TwoStepCachedQuery> twoStepCache =
        new GridBoundedConcurrentLinkedHashMap<>(PLAN_CACHE_SIZE);

    /** Update plans cache. */
    private final ConcurrentMap<H2CachedStatementKey, UpdatePlan> dmlCache =
        new GridBoundedConcurrentLinkedHashMap<>(PLAN_CACHE_SIZE);

    /**
     * Constructor.
     *
     * @param idx Indexing.
     */
    public QueryCache(IgniteH2Indexing idx) {
        this.idx = idx;
    }

    /**
     * Get query plan.
     *
     * @param key Key.
     * @return Plan.
     */
    public H2TwoStepCachedQuery planForSelect(H2TwoStepCachedQueryKey key) {
        return twoStepCache.get(key);
    }

    /**
     * Save plan if it doesn't exist.
     *
     * @param key Key.
     * @param plan Plan.
     */
    public void planForSelect(H2TwoStepCachedQueryKey key, H2TwoStepCachedQuery plan) {
        twoStepCache.putIfAbsent(key, plan);
    }

    /**
     * Generate SELECT statements to retrieve data for modifications from and find fast UPDATE or DELETE args,
     * if available.
     *
     * @param schema Schema.
     * @param conn Connection.
     * @param p Prepared statement.
     * @param fieldsQry Original fields query.
     * @param loc Local query flag.
     * @return Update plan.
     */
    public UpdatePlan planForDml(String schema, Connection conn, Prepared p, SqlFieldsQuery fieldsQry,
        boolean loc, @Nullable Integer errKeysPos) throws IgniteCheckedException {
        isDmlOnSchemaSupported(schema);

        H2CachedStatementKey planKey = H2CachedStatementKey.forDmlStatement(schema, p.getSQL(), fieldsQry, loc);

        UpdatePlan res = (errKeysPos == null ? dmlCache.get(planKey) : null);

        if (res != null)
            return res;

        res = UpdatePlanBuilder.planForStatement(p, loc, idx, conn, fieldsQry, errKeysPos, DML_ALLOWED);

        // Don't cache re-runs
        if (errKeysPos == null)
            return U.firstNotNull(dmlCache.putIfAbsent(planKey, res), res);
        else
            return res;
    }

    /**
     * Handle cache stop.
     *
     * @param cacheName Cache name.
     */
    public void onCacheStop(String cacheName) {
        // Clear SQL cache.
        int cacheId = CU.cacheId(cacheName);

        for (Iterator<Map.Entry<H2TwoStepCachedQueryKey, H2TwoStepCachedQuery>> it =
             twoStepCache.entrySet().iterator(); it.hasNext();) {
            Map.Entry<H2TwoStepCachedQueryKey, H2TwoStepCachedQuery> e = it.next();

            GridCacheTwoStepQuery qry = e.getValue().query();

            if (!F.isEmpty(qry.cacheIds()) && qry.cacheIds().contains(cacheId))
                it.remove();
        }

        // Clear DML cache.
        Iterator<Map.Entry<H2CachedStatementKey, UpdatePlan>> iter = dmlCache.entrySet().iterator();

        while (iter.hasNext()) {
            UpdatePlan plan = iter.next().getValue();

            if (F.eq(cacheName, plan.cacheContext().name()))
                iter.remove();
        }
    }

    /**
     * Remove all cached queries from cached two-steps queries.
     */
    public void clear() {
        twoStepCache = new GridBoundedConcurrentLinkedHashMap<>(PLAN_CACHE_SIZE);

        // TODO: Clear DML.
    }

    /**
     * Check if schema supports DDL statement.
     *
     * @param schemaName Schema name.
     */
    private static void isDmlOnSchemaSupported(String schemaName) {
        if (F.eq(QueryUtils.SCHEMA_SYS, schemaName))
            throw new IgniteSQLException("DML statements are not supported on " + schemaName + " schema",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }
}
