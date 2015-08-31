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

package org.apache.ignite.cache.query;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * SQL Fields query. This query can return specific fields of data based
 * on SQL {@code 'select'} clause, as opposed to {@link SqlQuery}, which always returns
 * the whole key and value objects back.
 * <h1 class="header">Collocated Flag</h1>
 * Collocation flag is used for optimization purposes. Whenever Ignite executes
 * a distributed query, it sends sub-queries to individual cluster members.
 * If you know in advance that the elements of your query selection are collocated
 * together on the same node, usually based on some <b>affinity-key</b>, Ignite
 * can make significant performance and network optimizations.
 * <p>
 * For example, in case of Word-Count example, we know that all identical words
 * are processed on the same cluster member, because we use the {@code word} itself
 * as affinity key. This allows Ignite to execute the {@code 'limit'} clause on
 * the remote nodes and bring back only the small data set specified within the 'limit' clause,
 * instead of the whole query result as would happen in a non-collocated execution.
 *
 * @see IgniteCache#query(Query)
 */
public final class SqlFieldsQuery extends Query<List<?>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** SQL Query. */
    private String sql;

    /** Arguments. */
    @GridToStringInclude
    private Object[] args;

    /** Collocation flag. */
    private boolean collocated;

    /**
     * Constructs SQL fields query.
     *
     * @param sql SQL query.
     */
    public SqlFieldsQuery(String sql) {
        setSql(sql);
    }

    /**
     * Constructs SQL fields query.
     *
     * @param sql SQL query.
     * @param collocated Collocated flag.
     */
    public SqlFieldsQuery(String sql, boolean collocated) {
        this.sql = sql;
        this.collocated = collocated;
    }

    /**
     * Gets SQL clause.
     *
     * @return SQL clause.
     */
    public String getSql() {
        return sql;
    }

    /**
     * Sets SQL clause.
     *
     * @param sql SQL clause.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setSql(String sql) {
        A.notNull(sql, "sql");

        this.sql = sql;

        return this;
    }

    /**
     * Gets SQL arguments.
     *
     * @return SQL arguments.
     */
    public Object[] getArgs() {
        return args;
    }

    /**
     * Sets SQL arguments.
     *
     * @param args SQL arguments.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setArgs(Object... args) {
        this.args = args;

        return this;
    }

    /**
     * Checks if this query is collocated.
     *
     * @return {@code true} If the query is collocated.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * Sets flag defining if this query is collocated.
     *
     * @param collocated Flag value.
     * @return {@code this} For chaining.
     */
    public SqlFieldsQuery setCollocated(boolean collocated) {
        this.collocated = collocated;

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQuery setPageSize(int pageSize) {
        return (SqlFieldsQuery)super.setPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQuery setLocal(boolean loc) {
        return (SqlFieldsQuery)super.setLocal(loc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlFieldsQuery.class, this);
    }
}