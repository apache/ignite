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
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.IgniteSystemProperties;

/**
 * Task for SQL queries execution through {@link IgniteJdbcDriver}.
 * <p>
 * Not closed cursors will be removed after {@link #RMV_DELAY} milliseconds.
 * This parameter can be configured via {@link IgniteSystemProperties#IGNITE_JDBC_DRIVER_CURSOR_REMOVE_DELAY}
 * system property.
 */
class JdbcQueryTaskV2 extends JdbcQueryTask {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Enforce join order flag. */
    private final boolean enforceJoinOrder;

    /** Lazy query execution flag. */
    private final boolean lazy;

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
     * @param enforceJoinOrder Enforce joins order falg.
     * @param lazy Lazy query execution flag.
     */
    public JdbcQueryTaskV2(Ignite ignite, String cacheName, String schemaName, String sql, Boolean isQry, boolean loc,
        Object[] args, int fetchSize, UUID uuid, boolean locQry, boolean collocatedQry, boolean distributedJoins,
        boolean enforceJoinOrder, boolean lazy) {
        super(ignite, cacheName, schemaName, sql, isQry, loc, args, fetchSize, uuid, locQry,
            collocatedQry, distributedJoins);

        this.enforceJoinOrder = enforceJoinOrder;
        this.lazy = lazy;
    }

    /** {@inheritDoc} */
    @Override protected boolean enforceJoinOrder() {
        return enforceJoinOrder;
    }

    /** {@inheritDoc} */
    @Override protected boolean lazy() {
        return lazy;
    }

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
     * @param enforceJoinOrder Enforce joins order falg.
     * @param lazy Lazy query execution flag.
     * @return Appropriate task JdbcQueryTask or JdbcQueryTaskV2.
     */
    public static JdbcQueryTask createTask(Ignite ignite, String cacheName, String schemaName, String sql,
        Boolean isQry, boolean loc, Object[] args, int fetchSize, UUID uuid, boolean locQry,
        boolean collocatedQry, boolean distributedJoins,
        boolean enforceJoinOrder, boolean lazy) {

        if (enforceJoinOrder || lazy)
            return new JdbcQueryTaskV2(ignite, cacheName, schemaName, sql, isQry, loc, args, fetchSize,
                uuid, locQry, collocatedQry, distributedJoins, enforceJoinOrder, lazy);
        else
            return new JdbcQueryTask(ignite, cacheName, schemaName, sql, isQry, loc, args, fetchSize,
                uuid, locQry, collocatedQry, distributedJoins);
    }
}
