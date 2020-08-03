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

package org.apache.ignite.internal.processors.odbc.odbc;

import java.util.Collection;

/**
 * SQL listener query fetch result.
 */
public class OdbcQueryFetchResult {
    /** Query ID. */
    private final long queryId;

    /** Query result rows. */
    private final Collection<?> items;

    /** Flag indicating the query has no non-fetched results. */
    private final boolean last;

    /**
     * @param queryId Query ID.
     * @param items Query result rows.
     * @param last Flag indicating the query has no unfetched results.
     */
    public OdbcQueryFetchResult(long queryId, Collection<?> items, boolean last) {
        this.queryId = queryId;
        this.items = items;
        this.last = last;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return queryId;
    }

    /**
     * @return Query result rows.
     */
    public Collection<?> items() {
        return items;
    }

    /**
     * @return Flag indicating the query has no non-fetched results.
     */
    public boolean last() {
        return last;
    }
}
