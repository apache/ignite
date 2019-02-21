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
    /** Statmement. */
    private GridSqlStatement stmt;

    /** Two-step query, or {@code} null if this result is for local query. */
    private final GridCacheTwoStepQuery twoStepQry;

    /** Metadata for two-step query, or {@code} null if this result is for local query. */
    private final List<GridQueryFieldMetadata> meta;

    /** Number of parameters. */
    private final int paramsCnt;

    /** Involved cache IDs. */
    private final List<Integer> cacheIds;

    /** ID of the first MVCC cache. */
    private final Integer mvccCacheId;

    /** FOR UPDATE flag. */
    private final boolean forUpdate;

    /**
     * Constructor.
     *
     * @param stmt Statement.
     * @param twoStepQry Distributed query plan.
     * @param meta Fields metadata.
     * @param paramsCnt Parameters count.
     * @param cacheIds Cache IDs.
     * @param mvccCacheId ID of the first MVCC cache.
     * @param forUpdate Whether this is FOR UPDATE flag.
     */
    public QueryParserResultSelect(
        GridSqlStatement stmt,
        @Nullable GridCacheTwoStepQuery twoStepQry,
        List<GridQueryFieldMetadata> meta,
        int paramsCnt,
        List<Integer> cacheIds,
        @Nullable Integer mvccCacheId,
        boolean forUpdate
    ) {
        this.stmt = stmt;
        this.twoStepQry = twoStepQry;
        this.meta = meta;
        this.paramsCnt = paramsCnt;
        this.cacheIds = cacheIds;
        this.mvccCacheId = mvccCacheId;
        this.forUpdate = forUpdate;
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
        return forUpdate;
    }

    /**
     * @return Number of parameters.
     */
    public int parametersCount() {
        return paramsCnt;
    }
}
