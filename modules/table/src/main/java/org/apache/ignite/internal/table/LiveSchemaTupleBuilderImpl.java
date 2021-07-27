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

package org.apache.ignite.internal.table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;

/**
 * Live schema buildable tuple.
 *
 * Allows to create columns implicitly by adding previously nonexistent columns during insert.
 */
public class LiveSchemaTupleBuilderImpl extends TupleBuilderImpl {
    /** Live schema column values. */
    private Map<String, Object> extraColumnsMap;

    /** Schema registry. */
    private final SchemaRegistry schemaRegistry;

    /** Current table name. */
    private final String tblName;

    /** Table manager. */
    private final TableManager mgr;

    /**
     * Constructor.
     */
    public LiveSchemaTupleBuilderImpl(SchemaRegistry schemaRegistry, String tblName, TableManager mgr) {
        super(schemaRegistry == null ? null : schemaRegistry.schema());

        Objects.requireNonNull(schemaRegistry);
        Objects.requireNonNull(tblName);
        Objects.requireNonNull(mgr);

        this.schemaRegistry = schemaRegistry;
        this.tblName = tblName;
        this.mgr = mgr;

        extraColumnsMap = null;
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder set(String columnName, Object val) {
        Column col = schema().column(columnName);

        if (col == null) {
            if (val == null)
                return this;

            if (extraColumnsMap == null)
                extraColumnsMap = new HashMap<>();
                
            extraColumnsMap.put(columnName, val);
            
            return this;
        }
        super.set(columnName, val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Tuple build() {
        if (extraColumnsMap == null)
            return this;

        while (!extraColumnsMap.isEmpty()) {
            createColumns(extraColumnsMap);

            this.schema(schemaRegistry.schema());

            Map<String, Object> colMap = map;
            map = new HashMap<>();

            extraColumnsMap.forEach(colMap::put);
            extraColumnsMap.clear();

            colMap.forEach(this::set);
        }

        return this;
    }

    /**
     * Updates the schema, creates new columns.
     * @param extraCols - map with column names and column values.
     */
    private void createColumns(Map<String, Object> extraCols) {
        Map<String, ColumnType> colTypeMap = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : extraCols.entrySet()) {
            String colName = entry.getKey();
            Object val = entry.getValue();

            ColumnType type = SchemaConfigurationConverter.columnType(val.getClass());

            if (type == null)
                throw new UnsupportedOperationException("Live schema update for type [" + val.getClass() + "] is not supported yet.");

            colTypeMap.put(colName, type);
        }
        List<org.apache.ignite.schema.Column> newCols = colTypeMap.entrySet().stream()
            .map(entry -> SchemaBuilders.column(entry.getKey(), entry.getValue()).asNullable().build())
            .collect(Collectors.toList());

        mgr.alterTable(tblName, chng -> chng.changeColumns(cols -> {
            int colIdx = chng.columns().size();
            //TODO: avoid 'colIdx' or replace with correct last colIdx.

            for (org.apache.ignite.schema.Column column : newCols) {
                cols.create(String.valueOf(colIdx), colChg -> convert(column, colChg));
                colIdx++;
            }
        }));
    }
}
