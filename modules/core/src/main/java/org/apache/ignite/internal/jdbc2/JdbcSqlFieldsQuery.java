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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.SqlFieldsQuery;

/**
 * {@link SqlFieldsQuery} with JDBC flavor - it has additional flag indicating whether JDBC driver expects
 * this query to return a result set or an update counter. This class is not intended for use outside JDBC driver.
 */
public final class JdbcSqlFieldsQuery extends SqlFieldsQuery {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag set by JDBC driver to enforce checks for correct operation type. */
    private final boolean isQry;

    /**
     * @param sql SQL query.
     * @param isQry Flag indicating whether this object denotes a query or an update operation.
     */
    JdbcSqlFieldsQuery(String sql, boolean isQry) {
        super(sql);
        this.isQry = isQry;
    }

    /**
     * @return Flag indicating whether this object denotes a query or an update operation..
     */
    public boolean isQuery() {
        return isQry;
    }

    /** {@inheritDoc} */
    @Override public JdbcSqlFieldsQuery setSql(String sql) {
        super.setSql(sql);

        return this;
    }

    /** {@inheritDoc} */
    @Override public JdbcSqlFieldsQuery setArgs(Object... args) {
        super.setArgs(args);

        return this;
    }

    /** {@inheritDoc} */
    @Override public JdbcSqlFieldsQuery setTimeout(int timeout, TimeUnit timeUnit) {
        super.setTimeout(timeout, timeUnit);

        return this;
    }

    /** {@inheritDoc} */
    @Override public JdbcSqlFieldsQuery setCollocated(boolean collocated) {
        super.setCollocated(collocated);

        return this;
    }

    /** {@inheritDoc} */
    @Override public JdbcSqlFieldsQuery setEnforceJoinOrder(boolean enforceJoinOrder) {
        super.setEnforceJoinOrder(enforceJoinOrder);

        return this;
    }

    /** {@inheritDoc} */
    @Override public JdbcSqlFieldsQuery setDistributedJoins(boolean distributedJoins) {
        super.setDistributedJoins(distributedJoins);

        return this;
    }

    /** {@inheritDoc} */
    @Override public JdbcSqlFieldsQuery setPageSize(int pageSize) {
        super.setPageSize(pageSize);

        return this;
    }

    /** {@inheritDoc} */
    @Override public JdbcSqlFieldsQuery setLocal(boolean loc) {
        super.setLocal(loc);

        return this;
    }
}
