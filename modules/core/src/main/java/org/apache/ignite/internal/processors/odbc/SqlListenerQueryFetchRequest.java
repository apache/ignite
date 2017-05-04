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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * SQL listener query fetch request.
 */
public class SqlListenerQueryFetchRequest extends SqlListenerRequest {
    /** Query ID. */
    private final long queryId;

    /** Page size - maximum number of rows to return. */
    private final int pageSize;

    /**
     * @param queryId Query ID.
     * @param pageSize Page size.
     */
    public SqlListenerQueryFetchRequest(long queryId, int pageSize) {
        super(QRY_FETCH);

        this.queryId = queryId;
        this.pageSize = pageSize;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return queryId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlListenerQueryFetchRequest.class, this);
    }
}