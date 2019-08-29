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

import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.view.QueryView;

/** */
public class QueryViewWalker implements MonitoringRowAttributeWalker<QueryView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.acceptLong(0, "duration");
        v.acceptBoolean(1, "failed");
        v.accept(2, "globalQueryId", String.class);
        v.accept(3, "id", Long.class);
        v.acceptBoolean(4, "local");
        v.accept(5, "originNodeId", String.class);
        v.accept(6, "query", String.class);
        v.accept(7, "queryType", GridCacheQueryType.class);
        v.accept(8, "schemaName", String.class);
        v.acceptLong(9, "startTime");
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(QueryView row, AttributeWithValueVisitor v) {
        v.acceptLong(0, "duration", row.duration());
        v.acceptBoolean(1, "failed", row.failed());
        v.accept(2, "globalQueryId", String.class, row.globalQueryId());
        v.accept(3, "id", Long.class, row.id());
        v.acceptBoolean(4, "local", row.local());
        v.accept(5, "originNodeId", String.class, row.originNodeId());
        v.accept(6, "query", String.class, row.query());
        v.accept(7, "queryType", GridCacheQueryType.class, row.queryType());
        v.accept(8, "schemaName", String.class, row.schemaName());
        v.acceptLong(9, "startTime", row.startTime());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 10;
    }
}

