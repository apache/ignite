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

import java.util.List;
import org.apache.ignite.Ignite;
import org.jetbrains.annotations.Nullable;

/**
 * Task to perform batch update v2: added data page scan hint.
 */
public class JdbcBatchUpdateTaskV2 extends JdbcBatchUpdateTask {
    /** Data page scan support. */
    private final Boolean dataPageScan;

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
    public JdbcBatchUpdateTaskV2(Ignite ignite, String cacheName, String schemaName, String sql,
        List<String> sqlBatch, List<List<Object>> batchArgs, boolean loc, int fetchSize,
        boolean locQry, boolean collocatedQry, boolean distributedJoins, @Nullable Boolean dataPageScan) {
        super(ignite, cacheName, schemaName, sql, sqlBatch, batchArgs, loc, fetchSize, locQry, collocatedQry, distributedJoins);
        this.dataPageScan = dataPageScan;
    }

    /** {@inheritDoc} */
    @Override public Boolean dataPageScan() {
        return dataPageScan;
    }

    /**
     * Creates the v1 task if data page scan is not used, otherwise - v2 task.
     */
    public static JdbcBatchUpdateTask createTask(Ignite ignite, String cacheName, String schemaName, String sql,
        List<String> sqlBatch, List<List<Object>> batchArgs, boolean loc, int fetchSize,
        boolean locQry, boolean collocatedQry, boolean distributedJoins, @Nullable Boolean dataPageScan) {

        if (dataPageScan != null)
            return new JdbcBatchUpdateTaskV2(ignite, cacheName, schemaName, sql, sqlBatch, batchArgs, loc, fetchSize,
                locQry, collocatedQry, distributedJoins, dataPageScan);

        return new JdbcBatchUpdateTask(ignite, cacheName, schemaName, sql, sqlBatch, batchArgs, loc, fetchSize,
            locQry, collocatedQry, distributedJoins);
    }
}
