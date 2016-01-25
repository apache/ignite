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
package org.apache.ignite.internal.processors.odbc.response;

import java.util.Collection;

/**
 * Query fetch result.
 */
public class OdbcQueryFetchResult {
    /** Query ID. */
    private long queryId;

    /** Query result rows. */
    private Collection<?> items = null;

    /** Flag indicating the query has no unfetched results. */
    private boolean last = false;

    /**
     * @param queryId Query ID.
     */
    public OdbcQueryFetchResult(long queryId){
        this.queryId = queryId;
    }

    /**
     * @return Query ID.
     */
    public long getQueryId() {
        return queryId;
    }

    /**
     * @param items Query result rows.
     */
    public void setItems(Collection<?> items) {
        this.items = items;
    }

    /**
     * @return Query result rows.
     */
    public Collection<?> getItems() {
        return items;
    }

    /**
     * @param last Flag indicating the query has no unfetched results.
     */
    public void setLast(boolean last) {
        this.last = last;
    }

    /**
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean getLast() {
        return last;
    }
}
