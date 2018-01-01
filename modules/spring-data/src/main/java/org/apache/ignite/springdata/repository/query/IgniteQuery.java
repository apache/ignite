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

package org.apache.ignite.springdata.repository.query;

/**
 * Ignite query helper class. For internal use only.
 */
public class IgniteQuery {
    /** */
    enum Option {
        /** Query will be used with Sort object. */
        SORTING,

        /** Query will be used with Pageable object. */
        PAGINATION,

        /** No advanced option. */
        NONE
    }

    /** Sql query text string. */
    private final String sql;

    /** */
    private final boolean isFieldQuery;

    /** Type of option. */
    private final Option option;

    /**
     * @param sql Sql.
     * @param isFieldQuery Is field query.
     * @param option Option.
     */
    public IgniteQuery(String sql, boolean isFieldQuery, Option option) {
        this.sql = sql;
        this.isFieldQuery = isFieldQuery;
        this.option = option;
    }

    /**
     * Text string of the query.
     *
     * @return SQL query text string.
     */
    public String sql() {
        return sql;
    }

    /**
     * Returns {@code true} if it's Ignite SQL fields query, {@code false} otherwise.
     *
     * @return {@code true} if it's Ignite SQL fields query, {@code false} otherwise.
     */
    public boolean isFieldQuery() {
        return isFieldQuery;
    }

    /**
     * Advanced querying option.
     *
     * @return querying option.
     */
    public Option options() {
        return option;
    }
}

