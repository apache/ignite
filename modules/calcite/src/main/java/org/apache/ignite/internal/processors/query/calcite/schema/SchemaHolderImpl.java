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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.network.TopologyService;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.nativeTypeToClass;

/**
 * Holds actual schema and mutates it on schema change, requested by Ignite.
 */
public class SchemaHolderImpl implements SchemaHolder {
    /** */
    private final Map<String, IgniteSchema> igniteSchemas = new HashMap<>();

    private final TopologyService topSrvc;

    /** */
    private volatile SchemaPlus calciteSchema;

    public SchemaHolderImpl(
        TopologyService topSrvc
    ) {
        this.topSrvc = topSrvc;

        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));
        calciteSchema = newCalciteSchema;
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus schema() {
        return calciteSchema;
    }

    public synchronized void onSchemaCreated(String schemaName) {
        igniteSchemas.putIfAbsent(schemaName, new IgniteSchema(schemaName));
        rebuild();
    }

    public synchronized void onSchemaDropped(String schemaName) {
        igniteSchemas.remove(schemaName);
        rebuild();
    }

    public synchronized void onSqlTypeCreated(
        String schemaName,
        String tableName,
        SchemaDescriptor descriptor
    ) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        tableName = tableName.substring(schemaName.length() + 1);

        List<ColumnDescriptor> colDescriptors = descriptor.columnNames().stream()
            .map(descriptor::column)
            .sorted(Comparator.comparingInt(Column::schemaIndex))
            .map(col -> new ColumnDescriptorImpl(
                col.name(),
                descriptor.isKeyColumn(col.schemaIndex()),
                col.schemaIndex(),
                nativeTypeToClass(col.type()),
                col::defaultValue
            ))
            .collect(Collectors.toList());

        TableDescriptorImpl desc = new TableDescriptorImpl(topSrvc, colDescriptors);

        schema.addTable(tableName, new IgniteTableImpl(desc));

        rebuild();
    }

     public void onSqlTypeUpdated(
         String schemaName,
         String tableName,
         SchemaDescriptor descriptor
    ) {
        onSqlTypeCreated(schemaName, tableName, descriptor);
    }

    public synchronized void onSqlTypeDropped(
        String schemaName,
        String tableName
    ) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        schema.removeTable(tableName);

        rebuild();
    }

    private void rebuild() {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));
        igniteSchemas.forEach(newCalciteSchema::add);
        calciteSchema = newCalciteSchema;
    }
}
