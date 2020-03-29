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

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.sql.Statement.SUCCESS_NO_INFO;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.createJdbcSqlException;

/**
 * Task for SQL batched update statements execution through {@link IgniteJdbcDriver}.
 */
class JdbcBatchUpdateTask implements IgniteCallable<int[]> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Ignite. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Cache name. */
    private final String cacheName;

    /** Schema name. */
    private final String schemaName;

    /** SQL command for argument batching. */
    private final String sql;

    /** Batch of statements. */
    private final List<String> sqlBatch;

    /** Batch of arguments. */
    private final List<List<Object>> batchArgs;

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
     * @param sql SQL query. {@code null} in case of statement batching.
     * @param sqlBatch Batch of SQL statements. {@code null} in case of parameter batching.
     * @param batchArgs Batch of SQL parameters. {@code null} in case of statement batching.
     * @param loc Local execution flag.
     * @param fetchSize Fetch size.
     * @param locQry Local query flag.
     * @param collocatedQry Collocated query flag.
     * @param distributedJoins Distributed joins flag.
     */
    public JdbcBatchUpdateTask(Ignite ignite, String cacheName, String schemaName, String sql,
        List<String> sqlBatch, List<List<Object>> batchArgs, boolean loc, int fetchSize,
        boolean locQry, boolean collocatedQry, boolean distributedJoins) {
        this.ignite = ignite;
        this.cacheName = cacheName;
        this.schemaName = schemaName;
        this.sql = sql;
        this.sqlBatch = sqlBatch;
        this.batchArgs = batchArgs;
        this.fetchSize = fetchSize;
        this.loc = loc;
        this.locQry = locQry;
        this.collocatedQry = collocatedQry;
        this.distributedJoins = distributedJoins;

        assert (!F.isEmpty(sql) && !F.isEmpty(batchArgs)) ^ !F.isEmpty(sqlBatch);
    }

    /** {@inheritDoc} */
    @Override public int[] call() throws Exception {
        IgniteCache<?, ?> cache = ignite.cache(cacheName);

        // Don't create caches on server nodes in order to avoid of data rebalancing.
        boolean start = ignite.configuration().isClientMode();

        if (cache == null && cacheName == null)
            cache = ((IgniteKernal)ignite).context().cache().getOrStartPublicCache(start, !loc && locQry);

        if (cache == null) {
            if (cacheName == null) {
                throw createJdbcSqlException("Failed to execute query. No suitable caches found.",
                    IgniteQueryErrorCode.CACHE_NOT_FOUND);
            }
            else {
                throw createJdbcSqlException("Cache not found [cacheName=" + cacheName + ']',
                    IgniteQueryErrorCode.CACHE_NOT_FOUND);
            }
        }

        int batchSize = F.isEmpty(sql) ? sqlBatch.size() : batchArgs.size();

        int[] updCntrs = new int[batchSize];

        int idx = 0;

        try {
            if (F.isEmpty(sql)) {
                for (; idx < batchSize; idx++)
                    updCntrs[idx] = doSingleUpdate(cache, sqlBatch.get(idx), null);
            }
            else {
                for (; idx < batchSize; idx++)
                    updCntrs[idx] = doSingleUpdate(cache, sql, batchArgs.get(idx));
            }
        }
        catch (Exception ex) {
            IgniteSQLException sqlEx = X.cause(ex, IgniteSQLException.class);

            if (sqlEx != null)
                throw new BatchUpdateException(sqlEx.getMessage(), sqlEx.sqlState(), Arrays.copyOf(updCntrs, idx), ex);
            else
                throw new BatchUpdateException(Arrays.copyOf(updCntrs, idx), ex);
        }

        return updCntrs;
    }

    /**
     * Performs update.
     *
     * @param cache Cache.
     * @param sqlText SQL text.
     * @param args Parameters.
     * @return Update counter.
     * @throws SQLException If failed.
     */
    private Integer doSingleUpdate(IgniteCache<?, ?> cache, String sqlText, List<Object> args) throws SQLException {
        SqlFieldsQuery qry = new SqlFieldsQueryEx(sqlText, false);

        qry.setPageSize(fetchSize);
        qry.setLocal(locQry);
        qry.setCollocated(collocatedQry);
        qry.setDistributedJoins(distributedJoins);
        qry.setSchema(schemaName);
        qry.setArgs(args == null ? null : args.toArray());

        QueryCursorImpl<List<?>> qryCursor = (QueryCursorImpl<List<?>>)cache.withKeepBinary().query(qry);

        if (qryCursor.isQuery()) {
            throw createJdbcSqlException(getError("Query produced result set", qry),
                IgniteQueryErrorCode.STMT_TYPE_MISMATCH);
        }

        List<List<?>> rows = qryCursor.getAll();

        if (F.isEmpty(rows))
            return SUCCESS_NO_INFO;

        if (rows.size() != 1)
            throw new SQLException(getError("Expected single row for update operation result", qry));

        List<?> row = rows.get(0);

        if (F.isEmpty(row) || row.size() != 1)
            throw new SQLException(getError("Expected row size of 1 for update operation", qry));

        Object objRes = row.get(0);

        if (!(objRes instanceof Long))
            throw new SQLException(getError("Unexpected update result type", qry));

        Long longRes = (Long)objRes;

        if (longRes > Integer.MAX_VALUE) {
            IgniteLogger log = ignite.log();

            if (log != null)
                log.warning(getError("Query updated row counter (" + longRes + ") exceeds integer range", qry));

            return Integer.MAX_VALUE;
        }

        return longRes.intValue();
    }

    /**
     * Formats error message with query details.
     *
     * @param msg Error message.
     * @param qry Query.
     * @return Result.
     */
    private String getError(String msg, SqlFieldsQuery qry) {
        return msg + " [qry='" + qry.getSql() + "', params=" + Arrays.deepToString(qry.getArgs()) + ']';
    }
}
