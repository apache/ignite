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

import java.util.List;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.jetbrains.annotations.Nullable;

/**
 * Parsing result for SELECT.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class QueryParserResultSelect {
    /** Statement. */
    private GridSqlStatement stmt;

    /** Two-step query, or {@code} null if this result is for local query. */
    private final GridCacheTwoStepQuery twoStepQry;

    /**
     * Two-step query in SELECT FOR UPDATE case, or {@code} null if this result is for local query.
     * If a query is for update, we need to save two  variants of the this query.
     * First variant {@link QueryParserResultSelect#twoStepQry} is used  when the query is executed outside
     * of transaction - it is executed as a plain query.  The second variant of the query - is actually
     * a "for update" query which is used when running within transaction. In this query an extra _key column
     * is implicitly appended to query columns. This extra column is used to lock the selected rows.
     * This column is hidden from client.
     */
    private final GridCacheTwoStepQuery forUpdateTwoStepQry;

    /** Metadata for two-step query, or {@code} null if this result is for local query. */
    private final List<GridQueryFieldMetadata> meta;

    /** Involved cache IDs. */
    private final List<Integer> cacheIds;

    /** ID of the first MVCC cache. */
    private final Integer mvccCacheId;

    /**
     * Sql query with cleared "FOR UPDATE" statement.
     * This string is used when query is executed out of transaction.
     */
    private final String forUpdateQryOutTx;

    /**
     * Sql query for update. Contains additional "_key" column.
     * This string is used when executing query within explicit transaction.
     */
    private final String forUpdateQryTx;

    /**
     * Constructor.
     *
     * @param stmt Statement.
     * @param twoStepQry Distributed query plan.
     * @param forUpdateTwoStepQry FOR UPDATE query for execution within transaction.
     * @param meta Fields metadata.
     * @param cacheIds Cache IDs.
     * @param mvccCacheId ID of the first MVCC cache.
     * @param forUpdateQryOutTx FOR UPDATE query string for execution out of transaction.
     * @param forUpdateQryTx FOR UPDATE query string for execution within transaction.
     */
    public QueryParserResultSelect(
        GridSqlStatement stmt,
        @Nullable GridCacheTwoStepQuery twoStepQry,
        @Nullable GridCacheTwoStepQuery forUpdateTwoStepQry,
        List<GridQueryFieldMetadata> meta,
        List<Integer> cacheIds,
        @Nullable Integer mvccCacheId,
        String forUpdateQryOutTx,
        String forUpdateQryTx
    ) {
        this.stmt = stmt;
        this.twoStepQry = twoStepQry;
        this.forUpdateTwoStepQry = forUpdateTwoStepQry;
        this.meta = meta;
        this.cacheIds = cacheIds;
        this.mvccCacheId = mvccCacheId;
        this.forUpdateQryOutTx = forUpdateQryOutTx;
        this.forUpdateQryTx = forUpdateQryTx;
    }

    /**
     * @return Parsed SELECT statement.
     */
    public GridSqlStatement statement() {
        return stmt;
    }

    /**
     * @return Two-step query, or {@code} null if this result is for local query.
     */
    @Nullable public GridCacheTwoStepQuery twoStepQuery() {
        return twoStepQry;
    }

    /**
     * @return Two-step query for update, or {@code} null if this result is for local query.
     */
    @Nullable public GridCacheTwoStepQuery forUpdateTwoStepQuery() {
        return forUpdateTwoStepQry;
    }

    /**
     * @return Two-step query metadata.
     */
    public List<GridQueryFieldMetadata> meta() {
        return meta;
    }

    /**
     * @return Whether split is needed for this query.
     */
    public boolean splitNeeded() {
        return twoStepQry != null;
    }

    /**
     * @return Involved cache IDs.
     */
    public List<Integer> cacheIds() {
        return cacheIds;
    }

    /**
     * @return ID of the first MVCC cache.
     */
    public Integer mvccCacheId() {
        return mvccCacheId;
    }

    /**
     * @return Whether this is a SELECT for MVCC caches.
     */
    public boolean mvccEnabled() {
        return mvccCacheId != null;
    }

    /**
     * @return Whether this is FOR UPDATE query.
     */
    public boolean forUpdate() {
        return forUpdateQryTx != null;
    }

    /**
     * @return Sql FOR UPDATE query for execution out of transaction.
     */
    public String forUpdateQueryOutTx() {
        return forUpdateQryOutTx;
    }

    /**
     * @return Sql FOR UPDATE query for execution within transaction.
     */
    public String forUpdateQueryTx() {
        return forUpdateQryTx;
    }
}
