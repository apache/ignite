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

package org.apache.ignite.springdata.repository.impl;

/**
 * Representation of query.
 */
public class IgniteQuery {
    /** */
    public enum Dynamicity {
        /** Query will be used with Sort object. */
        SORTABLE,

        /** Query will be used with Pageable object. */
        PAGEBABLE,

        /** Not dynamic query. */
        NONE
    }

    /** Sql-query text. */
    private final String sql;

    /** Is field query. */
    private final boolean isFieldQuery;

    /** Type of dynamicity. */
    private final Dynamicity dynamicity;


    /**
     * @param sql Sql.
     * @param isFieldQuery Is field query.
     * @param dynamicity Dynamicity.
     */
    public IgniteQuery(String sql, boolean isFieldQuery, Dynamicity dynamicity) {
        this.sql = sql;
        this.isFieldQuery = isFieldQuery;
        this.dynamicity = dynamicity;
    }

    /** get sql query */
    public String sql() {
        return sql;
    }

    /** Is field query */
    public boolean isFieldQuery() {
        return isFieldQuery;
    }

    /** Type of dynamicity */
    public Dynamicity dynamicity() {
        return dynamicity;
    }
}
