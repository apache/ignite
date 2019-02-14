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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.jetbrains.annotations.Nullable;

public class JdbcQueryTaskV4 extends JdbcQueryTaskV3 {
    private final Boolean dataPageScan;

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param sql Sql query.
     * @param isQry Operation type flag - query or not - to enforce query type check.
     * @param loc Local execution flag.
     * @param args Args.
     * @param fetchSize Fetch size.
     * @param uuid UUID.
     * @param locQry Local query flag.
     * @param collocatedQry Collocated query flag.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce joins order flag.
     * @param lazy Lazy query execution flag.
     * @param updateMeta Update metadata on demand.
     * @param skipReducerOnUpdate Flkag to enable server side updates.
     */
    public JdbcQueryTaskV4(Ignite ignite, String cacheName, String schemaName, String sql,
        Boolean isQry, boolean loc, Object[] args, int fetchSize, UUID uuid, boolean locQry,
        boolean collocatedQry, boolean distributedJoins, boolean enforceJoinOrder, boolean lazy, boolean updateMeta,
        boolean skipReducerOnUpdate, @Nullable Boolean dataPageScan) {
        super(ignite, cacheName, schemaName, sql, isQry, loc, args, fetchSize, uuid, locQry, collocatedQry,
            distributedJoins, enforceJoinOrder, lazy, updateMeta, skipReducerOnUpdate);
        this.dataPageScan = dataPageScan;
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Boolean dataPageScan() {
        return dataPageScan;
    }

    /**
     * Creates the task of this version (v4) if data page scan is set, previous versions otherwise.
     */
    public static JdbcQueryTask createTask(Ignite ignite, String cacheName, String schemaName, String sql,
        Boolean isQry, boolean loc, Object[] args, int fetchSize, UUID uuid, boolean locQry,
        boolean collocatedQry, boolean distributedJoins, boolean enforceJoinOrder, boolean lazy, boolean updateMeta,
        boolean skipReducerOnUpdate, @Nullable Boolean dataPageScan) {

        if (dataPageScan != null)
            return new JdbcQueryTaskV4(ignite, cacheName, schemaName, sql, isQry, loc, args, fetchSize, uuid, locQry, collocatedQry,
                distributedJoins, enforceJoinOrder, lazy, updateMeta, skipReducerOnUpdate, dataPageScan);

        return JdbcQueryTaskV3.createTask(ignite, cacheName, schemaName, sql, isQry, loc, args, fetchSize, uuid, locQry, collocatedQry,
            distributedJoins, enforceJoinOrder, lazy, updateMeta, skipReducerOnUpdate);
    }


}
