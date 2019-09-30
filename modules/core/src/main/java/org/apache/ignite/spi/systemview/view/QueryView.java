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

package org.apache.ignite.spi.systemview.view;

import java.util.Date;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;

/**
 * Query representation for a {@link SystemView}.
 */
public class QueryView {
    /** Query. */
    private final Query qry;

    /**
     * @param qry Query.
     */
    public QueryView(Query qry) {
        this.qry = qry;
    }

    /** @return Origin query node. */
    @Order(3)
    public String originNodeId() {
    }

    /** @return Query ID. */
    @Order
    public Long id() {
    }

    public String globalQueryId() {

    }

    /** @return Query text. */
    @Order(1)
    public String query() {

    }

    /** @return Query type. */
    @Order(2)
    public GridCacheQueryType queryType() {

    }

    /** @return Schema name. */
    public String schemaName() {

    }

    /** @return Query start time. */
    @Order(4)
    public Date startTime() {

    }

    /** @return Query duration. */
    @Order(5)
    public long duration() {

    }

    /** @return {@code True} if query is local. */
    public boolean local() {

    }

    /** @return {@code True} if query failed. */
    public boolean failed() {

    }
}
