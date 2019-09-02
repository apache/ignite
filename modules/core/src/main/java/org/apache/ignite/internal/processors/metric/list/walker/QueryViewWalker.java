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

package org.apache.ignite.internal.processors.metric.list.walker;

import java.util.Date;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.view.QueryView;

/** */
public class QueryViewWalker implements MonitoringRowAttributeWalker<QueryView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "id", Long.class);
        v.accept(1, "query", String.class);
        v.accept(2, "queryType", GridCacheQueryType.class);
        v.accept(3, "originNodeId", String.class);
        v.accept(4, "startTime", Date.class);
        v.acceptLong(5, "duration");
        v.acceptBoolean(6, "failed");
        v.accept(7, "globalQueryId", String.class);
        v.acceptBoolean(8, "local");
        v.accept(9, "monitoringRowId", Object.class);
        v.accept(10, "monitoringRowId", Long.class);
        v.accept(11, "schemaName", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(QueryView row, AttributeWithValueVisitor v) {
        v.accept(0, "id", Long.class, row.id());
        v.accept(1, "query", String.class, row.query());
        v.accept(2, "queryType", GridCacheQueryType.class, row.queryType());
        v.accept(3, "originNodeId", String.class, row.originNodeId());
        v.accept(4, "startTime", Date.class, row.startTime());
        v.acceptLong(5, "duration", row.duration());
        v.acceptBoolean(6, "failed", row.failed());
        v.accept(7, "globalQueryId", String.class, row.globalQueryId());
        v.acceptBoolean(8, "local", row.local());
        v.accept(9, "monitoringRowId", Object.class, row.monitoringRowId());
        v.accept(10, "monitoringRowId", Long.class, row.monitoringRowId());
        v.accept(11, "schemaName", String.class, row.schemaName());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 12;
    }
}

