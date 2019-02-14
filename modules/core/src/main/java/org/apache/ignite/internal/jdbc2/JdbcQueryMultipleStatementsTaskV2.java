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

import org.apache.ignite.Ignite;
import org.jetbrains.annotations.Nullable;

/**
 * V2 version of the task: added data page scan support.
 */
public class JdbcQueryMultipleStatementsTaskV2 extends JdbcQueryMultipleStatementsTask {
    /** Data page scan support. */
    private final Boolean dataPageScan;

    /**
     * @param ignite Ignite.
     * @param schemaName Schema name.
     * @param sql Sql query.
     * @param isQry Operation type flag - query or not - to enforce query type check.
     * @param loc Local execution flag.
     * @param args Args.
     * @param fetchSize Fetch size.
     * @param locQry Local query flag.
     * @param collocatedQry Collocated query flag.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce joins order falg.
     * @param lazy Lazy query execution flag.
     */
    public JdbcQueryMultipleStatementsTaskV2(Ignite ignite, String schemaName, String sql,
        Boolean isQry, boolean loc, Object[] args, int fetchSize, boolean locQry, boolean collocatedQry,
        boolean distributedJoins, boolean enforceJoinOrder, boolean lazy, @Nullable Boolean dataPageScan) {
        super(ignite, schemaName, sql, isQry, loc, args, fetchSize, locQry, collocatedQry, distributedJoins, enforceJoinOrder, lazy);
        this.dataPageScan = dataPageScan;
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Boolean dataPageScan() {
        return dataPageScan;
    }

    /**
     * Creates v2 version of the task if data page scan is set, earlier version - otherwise.
     */
    public static JdbcQueryMultipleStatementsTask createTask(Ignite ignite, String schemaName, String sql,
        Boolean isQry, boolean loc, Object[] args, int fetchSize, boolean locQry, boolean collocatedQry,
        boolean distributedJoins, boolean enforceJoinOrder, boolean lazy, @Nullable Boolean dataPageScan){
        if (dataPageScan != null)
            return new JdbcQueryMultipleStatementsTaskV2(ignite, schemaName, sql, isQry, loc, args, fetchSize, locQry,
                collocatedQry, distributedJoins, enforceJoinOrder, lazy, dataPageScan);

        return new JdbcQueryMultipleStatementsTask(ignite, schemaName, sql, isQry, loc, args, fetchSize, locQry,
            collocatedQry, distributedJoins, enforceJoinOrder, lazy);
    }
}
