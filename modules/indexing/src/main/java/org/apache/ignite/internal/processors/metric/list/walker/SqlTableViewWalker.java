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

import org.apache.ignite.internal.processors.metric.list.view.SqlTableView;
import org.apache.ignite.spi.metric.list.MonitoringRowAttributeWalker;

/** */
public class SqlTableViewWalker implements MonitoringRowAttributeWalker<SqlTableView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "tableName", String.class);
        v.accept(1, "schemaName", String.class);
        v.accept(2, "cacheName", String.class);
        v.accept(3, "affKeyCol", String.class);
        v.acceptInt(4, "cacheGroupId");
        v.accept(5, "cacheGroupName", String.class);
        v.acceptInt(6, "cacheId");
        v.accept(7, "keyAlias", String.class);
        v.accept(8, "keyTypeName", String.class);
        v.accept(9, "valAlias", String.class);
        v.accept(10, "valTypeName", String.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(SqlTableView row, AttributeWithValueVisitor v) {
        v.accept(0, "tableName", String.class, row.tableName());
        v.accept(1, "schemaName", String.class, row.schemaName());
        v.accept(2, "cacheName", String.class, row.cacheName());
        v.accept(3, "affKeyCol", String.class, row.affKeyCol());
        v.acceptInt(4, "cacheGroupId", row.cacheGroupId());
        v.accept(5, "cacheGroupName", String.class, row.cacheGroupName());
        v.acceptInt(6, "cacheId", row.cacheId());
        v.accept(7, "keyAlias", String.class, row.keyAlias());
        v.accept(8, "keyTypeName", String.class, row.keyTypeName());
        v.accept(9, "valAlias", String.class, row.valAlias());
        v.accept(10, "valTypeName", String.class, row.valTypeName());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 11;
    }
}

