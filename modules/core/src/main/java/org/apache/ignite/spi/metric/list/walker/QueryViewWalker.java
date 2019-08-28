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

package org.apache.ignite.spi.metric.list.walker;

import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.spi.metric.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.QueryView;

/** */
public class QueryViewWalker implements MonitoringRowAttributeWalker<QueryView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "globalQueryId", String.class);
        v.accept(1, "queryType", GridCacheQueryType.class);
        v.accept(2, "schemaName", String.class);
        v.acceptLong(3, "startTime");
        v.acceptBoolean(4, "local");
        v.acceptBoolean(5, "failed");
        v.accept(6, "originNodeId", String.class);
        v.accept(7, "query", String.class);
        v.accept(8, "id", Long.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(QueryView row, AttributeWithValueVisitor v) {
        v.accept(0, "globalQueryId", String.class, row.globalQueryId());
        v.accept(1, "queryType", GridCacheQueryType.class, row.queryType());
        v.accept(2, "schemaName", String.class, row.schemaName());
        v.acceptLong(3, "startTime", row.startTime());
        v.acceptBoolean(4, "local", row.local());
        v.acceptBoolean(5, "failed", row.failed());
        v.accept(6, "originNodeId", String.class, row.originNodeId());
        v.accept(7, "query", String.class, row.query());
        v.accept(8, "id", Long.class, row.id());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 9;
    }
}

