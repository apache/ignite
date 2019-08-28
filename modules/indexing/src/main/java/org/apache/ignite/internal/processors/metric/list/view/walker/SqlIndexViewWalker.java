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

package org.apache.ignite.internal.processors.metric.list.view.walker;

import org.apache.ignite.internal.processors.metric.list.view.SqlIndexView;
import org.apache.ignite.internal.processors.query.h2.database.H2IndexType;
import org.apache.ignite.spi.metric.MonitoringRowAttributeWalker;

/** */
public class SqlIndexViewWalker implements MonitoringRowAttributeWalker<SqlIndexView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "cacheName", String.class);
        v.accept(1, "schemaName", String.class);
        v.acceptInt(2, "cacheGroupId");
        v.accept(3, "cacheGroupName", String.class);
        v.accept(4, "tableName", String.class);
        v.accept(5, "indexName", String.class);
        v.accept(6, "indexType", H2IndexType.class);
        v.accept(7, "columns", String.class);
        v.acceptBoolean(8, "isPk");
        v.acceptBoolean(9, "isUnique");
        v.accept(10, "inlineSize", Integer.class);
        v.acceptInt(11, "cacheId");
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(SqlIndexView row, AttributeWithValueVisitor v) {
        v.accept(0, "cacheName", String.class, row.cacheName());
        v.accept(1, "schemaName", String.class, row.schemaName());
        v.acceptInt(2, "cacheGroupId", row.cacheGroupId());
        v.accept(3, "cacheGroupName", String.class, row.cacheGroupName());
        v.accept(4, "tableName", String.class, row.tableName());
        v.accept(5, "indexName", String.class, row.indexName());
        v.accept(6, "indexType", H2IndexType.class, row.indexType());
        v.accept(7, "columns", String.class, row.columns());
        v.acceptBoolean(8, "isPk", row.isPk());
        v.acceptBoolean(9, "isUnique", row.isUnique());
        v.accept(10, "inlineSize", Integer.class, row.inlineSize());
        v.acceptInt(11, "cacheId", row.cacheId());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 12;
    }
}

