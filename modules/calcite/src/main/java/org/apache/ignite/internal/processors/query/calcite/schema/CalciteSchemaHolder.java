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
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;

/**
 * Holds actual schema and mutates it on schema change, requested by Ignite.
 */
public class CalciteSchemaHolder implements SchemaChangeListener {
    /** */
    private final Map<String, IgniteSchema> schemas = new HashMap<>();

    /** */
    private volatile SchemaPlus schema;

    /**
     * Sets updated schema.
     * @param schema New schema.
     */
    public void schema(SchemaPlus schema) {
        this.schema = schema;
    }

    /**
     * @return Actual schema.
     */
    public SchemaPlus schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSchemaCreate(String schemaName) {
        schemas.putIfAbsent(schemaName, new IgniteSchema(schemaName));
        rebuild();
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSchemaDrop(String schemaName) {
        schemas.remove(schemaName);
        rebuild();
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSqlTypeCreate(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        schemas.computeIfAbsent(schemaName, IgniteSchema::new).onSqlTypeCreate(typeDescriptor, cacheInfo);
        rebuild();
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSqlTypeDrop(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        schemas.computeIfAbsent(schemaName, IgniteSchema::new).onSqlTypeDrop(typeDescriptor, cacheInfo);
        rebuild();
    }

    /** */
    private void rebuild() {
        SchemaPlus schema = Frameworks.createRootSchema(false);
        schemas.forEach(schema::add);
        schema(schema.getSubSchema(QueryUtils.DFLT_SCHEMA));
    }
}
