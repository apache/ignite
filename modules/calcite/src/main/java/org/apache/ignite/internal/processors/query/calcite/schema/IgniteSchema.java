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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;

/**
 *
 */
public class IgniteSchema extends AbstractSchema {
    /** */
    private final String schemaName;

    /** */
    private final Map<String, IgniteTable> tableMap = new ConcurrentHashMap<>();

    private final Map<String, IgniteTable> indexMap = new ConcurrentHashMap<>();

    public IgniteSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getName() {
        return schemaName;
    }

    @Override protected Map<String, Table> getTableMap() {
        Map<String, IgniteTable> union = new HashMap<>(tableMap.size() + indexMap.size());
        union.putAll(tableMap);
        union.putAll(indexMap);
        return Collections.unmodifiableMap(union);
    }

    /**
     * Callback method.
     *
     * @param typeDesc Query type descriptor.
     * @param cacheInfo Cache info.
     */
    public void createTable(String cacheName, String tblName, RowType rowType, RelCollation collation) {
        IgniteTable table = new IgniteTable(tblName, cacheName, rowType, collation, null);
        tableMap.put(table.name(), table);
    }

    public void createIndex(String idxName, String cacheName, String tblName, RowType rowType, RelCollation collation,
        String indexViewSql) {
        IgniteTable index = new IgniteTable(idxName, cacheName, rowType, collation, indexViewSql);
        IgniteTable table = tableMap.get(tblName);
        table.addIndex(index);

        IgniteTable old = indexMap.put(idxName, index);
        assert old == null; // TODO salt index names
    }

    /**
     * Callback method.
     *
     * @param typeDesc Query type descriptor.
     */
    public void dropTable(GridQueryTypeDescriptor typeDesc) {
        tableMap.remove(typeDesc.tableName()); // TODO drop indexes
    }
}
