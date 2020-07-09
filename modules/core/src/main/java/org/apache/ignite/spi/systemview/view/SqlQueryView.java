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
import java.util.UUID;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * SQL query representation for a {@link SystemView}.
 */
public class SqlQueryView {
    /** Query. */
    private final GridRunningQueryInfo qry;

    /**
     * @param qry Query.
     */
    public SqlQueryView(GridRunningQueryInfo qry) {
        this.qry = qry;
    }

    /** @return Origin query node. */
    @Order(2)
    public UUID originNodeId() {
        return qry.nodeId();
    }

    /** @return Query ID. */
    @Order
    public String queryId() {
        return qry.globalQueryId();
    }

    /** @return Query text. */
    @Order(1)
    public String sql() {
        return qry.query();
    }

    /** @return Schema name. */
    public String schemaName() {
        return qry.schemaName();
    }

    /** @return Query start time. */
    @Order(3)
    public Date startTime() {
        return new Date(qry.startTime());
    }

    /** @return Query duration. */
    @Order(4)
    public long duration() {
        return U.currentTimeMillis() - qry.startTime();
    }

    /** @return {@code True} if query is local. */
    public boolean local() {
        return qry.local();
    }
}
