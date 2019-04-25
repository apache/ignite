/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * SQL listener query fetch request.
 */
public class OdbcQueryMoreResultsRequest extends OdbcRequest {
    /** Query ID. */
    private final long queryId;

    /** Page size - maximum number of rows to return. */
    private final int pageSize;

    /**
     * @param queryId Query ID.
     * @param pageSize Page size.
     */
    public OdbcQueryMoreResultsRequest(long queryId, int pageSize) {
        super(MORE_RESULTS);

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
        return S.toString(OdbcQueryMoreResultsRequest.class, this);
    }
}
