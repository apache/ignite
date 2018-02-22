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

import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * SQL Query.
 *
 * @see IgniteCache#query(Query)
 */
public final class SqlQuery<K, V> extends Query<Cache.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String type;

    /** Table alias */
    private String alias;

    /** SQL clause. */
    private String sql;

    /** Arguments. */
    @GridToStringInclude
    private Object[] args;

    /** Timeout in millis. */
    private int timeout;

    /** */
    private boolean distributedJoins;

    /**
     * Constructs query for the given type name and SQL query.
     *
     * @param type Type.
     * @param sql SQL Query.
     */
    public SqlQuery(String type, String sql) {
        setType(type);
        setSql(sql);
    }

    /**
     * Constructs query for the given type and SQL query.
     *
     * @param type Type.
     * @param sql SQL Query.
     */
    public SqlQuery(Class<?> type, String sql) {
        setType(type);
        setSql(sql);
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
    public SqlQuery<K, V> setSql(String sql) {
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
    public SqlQuery<K, V> setArgs(Object... args) {
        this.args = args;

        return this;
    }

    /**
     * Gets type for query.
     *
     * @return Type.
     */
    public String getType() {
        return type;
    }

    /**
     * Sets type for query.
     *
     * @param type Type.
     * @return {@code this} For chaining.
     */
    public SqlQuery<K, V> setType(String type) {
        this.type = type;

        return this;
    }

    /**
     * Sets table alias for type.
     *
     * @return Table alias.
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Gets table alias for type.
     *
     * @param alias table alias for type that is used in query.
     * @return {@code this} For chaining.
     */
    public SqlQuery<K, V> setAlias(String alias) {
        this.alias = alias;

        return this;
    }

    /**
     * Gets the query execution timeout in milliseconds.
     *
     * @return Timeout value.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Sets the query execution timeout. Query will be automatically cancelled if the execution timeout is exceeded.
     *
     * @param timeout Timeout value. Zero value disables timeout.
     * @param timeUnit Time granularity.
     * @return {@code this} For chaining.
     */
    public SqlQuery<K, V> setTimeout(int timeout, TimeUnit timeUnit) {
        this.timeout = GridQueryProcessor.validateTimeout(timeout, timeUnit);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlQuery<K, V> setPageSize(int pageSize) {
        return (SqlQuery<K, V>)super.setPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override public SqlQuery<K, V> setLocal(boolean loc) {
        return (SqlQuery<K, V>)super.setLocal(loc);
    }

    /**
     * @param type Type.
     * @return {@code this} For chaining.
     */
    public SqlQuery setType(Class<?> type) {
        return setType(GridQueryProcessor.typeName(type));
    }

    /**
     * Specify if distributed joins are enabled for this query.
     *
     * When disabled, join results will only contain colocated data (joins work locally).
     * When enabled, joins work as expected, no matter how the data is distributed.
     *
     * @param distributedJoins Distributed joins enabled.
     * @return {@code this} For chaining.
     */
    public SqlQuery setDistributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;

        return this;
    }

    /**
     * Check if distributed joins are enabled for this query.
     *
     * @return {@code true} If distributed joind enabled.
     */
    public boolean isDistributedJoins() {
        return distributedJoins;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlQuery.class, this);
    }
}