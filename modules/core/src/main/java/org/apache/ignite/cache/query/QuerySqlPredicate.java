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

import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.*;

/**
 * Query SQL predicate to use with any of the {@code JCache.query(...)} and
 * {@code JCache.queryFields(...)} methods.
 *
 * @author @java.author
 * @version @java.version
 */
public final class QuerySqlPredicate<K, V> extends QueryPredicate<K, V> {
    /** SQL clause. */
    private String sql;

    /** Arguments. */
    private Object[] args;

    /**
     * Empty constructor.
     */
    public QuerySqlPredicate() {
        // No-op.
    }

    /**
     * Constructs SQL predicate with given SQL clause and arguments.
     *
     * @param sql SQL clause.
     * @param args Arguments.
     */
    public QuerySqlPredicate(String sql, Object... args) {
        this.sql = sql;
        this.args = args;
    }

    /**
     * Constructs SQL predicate with given SQL clause, page size, and arguments.
     *
     * @param sql SQL clause.
     * @param pageSize Optional page size, if {@code 0}, then {@link QueryConfiguration#getPageSize()} is used.
     * @param args Arguments.
     */
    public QuerySqlPredicate(String sql, int pageSize, Object[] args) {
        super(pageSize);

        this.sql = sql;
        this.args = args;
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
     */
    public void setSql(String sql) {
        this.sql = sql;
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
     */
    public void setArgs(Object... args) {
        this.args = args;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Cache.Entry<K, V> entry) {
        return false; // Not used.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QuerySqlPredicate.class, this);
    }
}
