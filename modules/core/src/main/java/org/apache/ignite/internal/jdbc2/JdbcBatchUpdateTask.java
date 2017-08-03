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

package org.apache.ignite.internal.jdbc2;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.IgniteSQLBatchUpdateException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.sql.Statement.SUCCESS_NO_INFO;

/**
 * Task for SQL batched update statements execution through {@link IgniteJdbcDriver}.
 */
class JdbcBatchUpdateTask implements IgniteCallable<long[]> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Ignite. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Cache name. */
    private final String cacheName;

    /** Schema name. */
    private final String schemaName;

    /** Sql batch. */
    private final String[] sqlBatch;

    /** Fetch size. */
    private final int fetchSize;

    /** Local execution flag. */
    private final boolean loc;

    /** Local query flag. */
    private final boolean locQry;

    /** Collocated query flag. */
    private final boolean collocatedQry;

    /** Distributed joins flag. */
    private final boolean distributedJoins;

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param sqlBatch Collection of sql queries.
     * @param loc Local execution flag.
     * @param fetchSize Fetch size.
     * @param locQry Local query flag.
     * @param collocatedQry Collocated query flag.
     * @param distributedJoins Distributed joins flag.
     */
    public JdbcBatchUpdateTask(Ignite ignite, String cacheName, String schemaName, String[] sqlBatch,
        boolean loc, int fetchSize, boolean locQry, boolean collocatedQry,
        boolean distributedJoins) {
        this.ignite = ignite;
        this.cacheName = cacheName;
        this.schemaName = schemaName;
        this.sqlBatch = sqlBatch;
        this.fetchSize = fetchSize;
        this.loc = loc;
        this.locQry = locQry;
        this.collocatedQry = collocatedQry;
        this.distributedJoins = distributedJoins;
    }

    /** {@inheritDoc} */
    @Override public long[] call() throws Exception {
        IgniteCache<?, ? > cache = getCache();

        long[] updCntrs = new long[sqlBatch.length];

        int idx = 0;

        try {
            for (; idx < sqlBatch.length; idx++)
                updCntrs[idx] = doSingleUpdate(cache, sqlBatch[idx]);
        }
        catch (SQLException ex) {
            throw new IgniteSQLBatchUpdateException(ex, Arrays.copyOf(updCntrs, idx));
        }
        catch (Exception ex) {
            throw new IgniteSQLBatchUpdateException(new SQLException(ex), Arrays.copyOf(updCntrs, idx));
        }

        return updCntrs;
    }

    /** */
    private Long doSingleUpdate(IgniteCache<?, ?> cache, String s) throws Exception {
        SqlFieldsQuery qry = new JdbcSqlFieldsQuery(s, false);

        qry.setPageSize(fetchSize);
        qry.setLocal(locQry);
        qry.setCollocated(collocatedQry);
        qry.setDistributedJoins(distributedJoins);
        qry.setSchema(schemaName);

        QueryCursorImpl<List<?>> qryCursor = (QueryCursorImpl<List<?>>)cache.withKeepBinary().query(qry);

        if (qryCursor.isQuery())
            throw new SQLException("Query produced result set [qry=" + s + ']');

        List<List<?>> rows = qryCursor.getAll();

        if (F.isEmpty(rows))
            return (long)SUCCESS_NO_INFO; // consider returning null instead

        if (rows.size() != 1)
            throw new SQLException("Expected number of rows for update operation");

        List<?> row = rows.get(0);

        if (F.isEmpty(row) || row.size() != 1)
            throw new SQLException("Expected row size for update operation");

        Object objRes = row.get(0);

        if (!(objRes instanceof Long))
            throw new SQLException("Unexpected update result type");

        return (Long) objRes;
    }

    /** */
    private IgniteCache<?, ?> getCache() throws Exception {
        IgniteCache<?, ?> cache = ignite.cache(cacheName);

        // Don't create caches on server nodes in order to avoid of data rebalancing.
        boolean start = ignite.configuration().isClientMode();

        if (cache == null && cacheName == null)
            cache = ((IgniteKernal)ignite).context().cache().getOrStartPublicCache(start, !loc && locQry);

        if (cache == null) {
            if (cacheName == null)
                throw new SQLException("Failed to execute query. No suitable caches found.");
            else
                throw new SQLException("Cache not found [cacheName=" + cacheName + ']');
        }

        return cache;
    }
}
