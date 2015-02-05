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

import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Query SQL predicate to use with any of the {@code JCache.query(...)} and
 * {@code JCache.queryFields(...)} methods.
 */
public final class QuerySqlPredicate extends QueryPredicate {
    /** */
    private String type;

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
     * Constructs SQL predicate with given type, SQL clause and arguments.
     *
     * @param type Class.
     * @param sql SQL clause.
     * @param args Arguments.
     */
    public QuerySqlPredicate(Class<?> type, String sql, Object... args) {
        setType(type);
        setSql(sql);
        setArgs(args);
    }

    /**
     * Constructs SQL predicate with given type, SQL clause and arguments.
     *
     * @param type Type to query in cache.
     * @param sql SQL clause.
     * @param args Arguments.
     */
    public QuerySqlPredicate(String type, String sql, Object... args) {
        setType(type);
        setSql(sql);
        setArgs(args);
    }

    /**
     * Constructs SQL predicate with given SQL clause and arguments.
     *
     * @param sql SQL clause.
     * @param args Arguments.
     */
    public QuerySqlPredicate(String sql, Object... args) {
        this((String)null, sql, args);
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
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * @param type Type.
     */
    public void setType(Class<?> type) {
        this.type = type == null ? null : type.getSimpleName();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QuerySqlPredicate.class, this);
    }
}
