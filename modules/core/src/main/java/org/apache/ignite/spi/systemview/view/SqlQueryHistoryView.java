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
import org.apache.ignite.internal.managers.systemview.walker.ViewAttribute;
import org.apache.ignite.internal.processors.query.QueryHistory;

/**
 * SQL query history representation for a {@link SystemView}.
 */
public class SqlQueryHistoryView {
    /** Query history item. */
    private final QueryHistory qry;

    /**
     * @param qry Query history item.
     */
    public SqlQueryHistoryView(QueryHistory qry) {
        this.qry = qry;
    }

    /** @return Schema name. */
    @ViewAttribute
    public String schemaName() {
        return qry.schema();
    }

    /** @return Query text. */
    @ViewAttribute(order = 1)
    public String sql() {
        return qry.query();
    }

    /** @return {@code True} if query local. */
    @ViewAttribute(order = 2)
    public boolean local() {
        return qry.local();
    }

    /** @return Number of executions of the query. */
    @ViewAttribute(order = 3)
    public long executions() {
        return qry.executions();
    }

    /** @return Number of failed execution of the query. */
    @ViewAttribute(order = 4)
    public long failures() {
        return qry.failures();
    }

    /** @return Minimal query duration. */
    @ViewAttribute(order = 5)
    public long durationMin() {
        return qry.minimumTime();
    }

    /** @return Maximum query duration. */
    @ViewAttribute(order = 6)
    public long durationMax() {
        return qry.maximumTime();
    }

    /** @return Last start time. */
    @ViewAttribute(order = 7)
    public Date lastStartTime() {
        return new Date(qry.lastStartTime());
    }
}
