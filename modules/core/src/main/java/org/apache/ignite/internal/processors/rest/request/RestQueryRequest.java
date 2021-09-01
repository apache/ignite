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

package org.apache.ignite.internal.processors.rest.request;

/**
 * Sql query request.
 */
public class RestQueryRequest extends GridRestRequest {
    /** Sql query. */
    private String sqlQry;

    /** Sql query arguments. */
    private Object[] args;

    /** Page size. */
    private Integer pageSize;

    /** Distributed joins. */
    private boolean distributedJoins;

    /** Cache name. */
    private String cacheName;

    /** Query id. */
    private Long qryId;

    /** Query type name. */
    private String typeName;

    /** Predicate class name for scan query. */
    private String className;

    /** Query type. */
    private QueryType type;

    /**  Keep binary flag. */
    private boolean keepBinary;

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
     * @param pageSize Page size.
     */
    public void pageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @return Page size.
     */
    public Integer pageSize() {
        return pageSize;
    }

    /**
     * @param distributedJoins New distributed joins.
     */
    public void distributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;
    }

    /**
     * @return Distributed joins.
     */
    public boolean distributedJoins() {
        return distributedJoins;
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

    /**
     * @param id Query id.
     */
    public void queryId(Long id) {
        this.qryId = id;
    }

    /**
     * @return Query id.
     */
    public Long queryId() {
        return qryId;
    }

    /**
     * @param typeName Query type name.
     */
    public void typeName(String typeName) {
        this.typeName = typeName;
    }

    /**
     * @return Query type name.
     */
    public String typeName() {
        return typeName;
    }

    /**
     * @return Predicate class name for scan query.
     */
    public String className() {
        return className;
    }

    /**
     * @param className Predicate class name for scan query.
     */
    public void className(String className) {
        this.className = className;
    }

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @param keepBinary Keep binary flag.
     */
    public void keepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
    }

    /**
     * @param type Query type.
     */
    public void queryType(QueryType type) {
        this.type = type;
    }

    /**
     * @return Query type.
     */
    public QueryType queryType() {
        return type;
    }

    /**
     * Supported query types.
     */
    public enum QueryType {
        /** Sql query. */
        SQL,

        /** Sql fields query. */
        SQL_FIELDS,

        /** Scan query. */
        SCAN
    }
}
