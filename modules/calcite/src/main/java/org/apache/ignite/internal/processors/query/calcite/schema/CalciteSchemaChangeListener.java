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

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;

/**
 *
 */
public class CalciteSchemaChangeListener implements SchemaChangeListener {
    private final Map<String, IgniteSchema> schemas = new HashMap<>();
    private final CalciteSchemaHolder schemaHolder;

    public CalciteSchemaChangeListener(CalciteSchemaHolder schemaHolder) {
        this.schemaHolder = schemaHolder;
    }

    @Override public synchronized void onSchemaCreate(String schemaName) {
        schemas.putIfAbsent(schemaName, new IgniteSchema(schemaName));
        rebuild();
    }

    @Override public synchronized void onSchemaDrop(String schemaName) {
        schemas.remove(schemaName);
        rebuild();
    }

    @Override public synchronized void onSqlTypeCreate(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        schemas.computeIfAbsent(schemaName, IgniteSchema::new).onSqlTypeCreate(typeDescriptor, cacheInfo);
        rebuild();
    }

    @Override public synchronized void onSqlTypeDrop(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        schemas.computeIfAbsent(schemaName, IgniteSchema::new).onSqlTypeDrop(typeDescriptor, cacheInfo);
        rebuild();
    }

    public void rebuild() {
        SchemaPlus schema = Frameworks.createRootSchema(false);
        schemas.forEach(schema::add);
        schemaHolder.schema(schema);
    }
}
