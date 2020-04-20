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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Ignite schema.
 */
public class IgniteSchema extends AbstractSchema {
    /** */
    private final String schemaName;

    /** */
    private final Map<String, Table> tableMap = new ConcurrentHashMap<>();

    /**
     * Creates a Schema.
     *
     * @param schemaName Schema name.
     */
    public IgniteSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Schema name.
     */
    public String getName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override protected Map<String, Table> getTableMap() {
        return Collections.unmodifiableMap(tableMap);
    }

    /**
     * @param table Table.
     */
    public void addTable(String tableName, Table table) {
        tableMap.put(tableName, table);
    }

    /**
     * @param tableName Table name.
     */
    public void removeTable(String tableName) {
        tableMap.remove(tableName);
    }

    public IgniteIndex getIndex(String tblName, String idxName) {
        Table tbl = tableMap.get(tblName);

        if (!(tbl instanceof IgniteTable))
            return null;

        IgniteTable igniteTbl = (IgniteTable)tbl;

        return igniteTbl == null ? null : igniteTbl.getIndex(idxName);
    }
}
