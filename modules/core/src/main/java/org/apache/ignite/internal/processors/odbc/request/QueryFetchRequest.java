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
 * ODBC query fetch request.
 */
public class QueryFetchRequest extends GridOdbcRequest {
    /** Query ID. */
    private long queryId;

    /** Page size - maximum number of rows to return. */
    private Integer pageSize;

    /**
     * @param queryId Query ID.
     * @param pageSize Page size.
     */
    public QueryFetchRequest(long queryId, int pageSize) {
        super(FETCH_SQL_QUERY);
        this.queryId = queryId;
        this.pageSize = pageSize;
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
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param queryId Query ID.
     */
    public void cacheName(long queryId) {
        this.queryId = queryId;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return queryId;
    }
}