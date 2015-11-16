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
package org.apache.ignite.internal.processors.odbc.request;

/**
 * ODBC query execute request.
 */
public class QueryExecuteRequest extends GridOdbcRequest {
    /** Sql query. */
    private String sqlQry;

    /** Sql query arguments. */
    private Object[] args;

    /** Cache name. */
    private String cacheName;

    /**
     * @param cacheName Cache name.
     * @param sqlQry SQL query.
     */
    public QueryExecuteRequest(String cacheName, String sqlQry) {
        this(cacheName, sqlQry, null);
    }

    /**
     * @param cacheName Cache name.
     * @param sqlQry SQL query.
     * @param args Arguments list.
     */
    public QueryExecuteRequest(String cacheName, String sqlQry, Object[] args) {
        super(EXECUTE_SQL_QUERY);
        this.cacheName = cacheName;
        this.sqlQry = sqlQry;
        this.args = args;
    }

    /**
     * @param sqlQry Sql query.
     */
    public void sqlQuery(String sqlQry) {
        this.sqlQry = sqlQry;
    }

    /**
     * @return Sql query.
     */
    public String sqlQuery() {
        return sqlQry;
    }

    /**
     * @param args Sql query arguments.
     */
    public void arguments(Object[] args) {
        this.args = args;
    }

    /**
     * @return Sql query arguments.
     */
    public Object[] arguments() {
        return args;
    }

    /**
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }
}
