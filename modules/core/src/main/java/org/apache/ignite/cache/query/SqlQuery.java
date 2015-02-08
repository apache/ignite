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

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * SQL Query.
 *
 * @see IgniteCache#query(Query)
 * @see IgniteCache#localQuery(Query)
 */
public final class SqlQuery extends Query<SqlQuery> {
    /** */
    private String type;

    /** SQL clause. */
    private String sql;

    /** Arguments. */
    @GridToStringInclude
    private Object[] args;

    /**
     * Constructs query for the given SQL query.
     *
     * @param sql SQL Query.
     */
    public SqlQuery(String sql) {
        setSql(sql);
    }

    /**
     * Constructs query for the given type and SQL query.
     *
     * @param type Type.
     * @param sql SQL Query.
     */
    public SqlQuery(Class<?> type, String sql) {
        this(sql);

        setType(type);
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
    public SqlQuery setSql(String sql) {
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
    public SqlQuery setArgs(Object... args) {
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
    public SqlQuery setType(String type) {
        this.type = type;

        return this;
    }

    /**
     * @param type Type.
     */
    public SqlQuery setType(Class<?> type) {
        return setType(GridQueryProcessor.typeName(type));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlQuery.class, this);
    }
}
