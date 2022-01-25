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
package org.apache.ignite.internal.processors.query;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.jetbrains.annotations.Nullable;

/**
 * Keep information about all running queries.
 */
public interface GridRunningQueryManager {
    /**
     * Gets info about running query by their id.
     * @param qryId Query id.
     * @return Running query info or {@code null} in case no running query for given id.
     */
    @Nullable public GridRunningQueryInfo runningQueryInfo(Long qryId);

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
        @Nullable GridQueryCancel cancel, String qryInitiatorId);

    /**
     * Unregister running query.
     *
     * @param qryId id of the query, which is given by {@link #register register} method.
     * @param failReason exception that caused query execution fail, or {@code null} if query succeded.
     */
    public void unregister(Long qryId, @Nullable Throwable failReason);

    /**
     * Cancel query running on remote or local Node.
     *
     * @param queryId Query id.
     * @param nodeId Node id, if {@code null}, cancel local query.
     * @param async If {@code true}, execute asynchronously.
     */
    public void cancelQuery(long queryId, @Nullable UUID nodeId, boolean async);

    /**
     * @param reqId Request ID of query to track.
     */
    public void trackRequestId(long reqId);

    /**
     * Gets query history statistics. Size of history could be configured via {@link
     * SqlConfiguration#setSqlQueryHistorySize(int)}
     *
     * @return Queries history statistics aggregated by query text, schema and local flag.
     */
    public Map<QueryHistoryKey, QueryHistory> queryHistoryMetrics();

    /**
     * Reset query history.
     */
    public void resetQueryHistoryMetrics();
}
