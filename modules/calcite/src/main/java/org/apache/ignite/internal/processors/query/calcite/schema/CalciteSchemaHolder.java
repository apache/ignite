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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;

/**
 *
 */
public class CalciteSchemaHolder implements SchemaChangeListener {
    private final Map<String, IgniteSchema> schemas = new HashMap<>();
    private SchemaPlus rootSchema;

    public CalciteSchemaHolder() {
        this.rootSchema = Frameworks.createRootSchema(false);;
    }

    public SchemaPlus rootSchema() {
        return rootSchema;
    }

    @Override public synchronized void onSchemaCreate(String schemaName) {
        IgniteSchema schema = new IgniteSchema(schemaName);
        rootSchema.add(schemaName, schema);
        schemas.putIfAbsent(schemaName, schema);
    }

    @Override public synchronized void onSchemaDrop(String schemaName) {
        throw new UnsupportedOperationException();
    }

    @Override public synchronized void onSqlTypeCreate(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        IgniteSchema schema = schemas.get(schemaName);

        assert schema != null;

        RowType type = Commons.rowType(typeDescriptor);

        String keyCol = typeDescriptor.keyFieldName();

        assert keyCol != null : "TODO";

        RelCollation collation = null;

        for (int i = 0; i < type.fields().length; i++) {
            String field = type.fields()[i];
            if (keyCol.equalsIgnoreCase(field)) {
                collation = RelCollationTraitDef.INSTANCE.canonize(RelCollations.of(new RelFieldCollation(i, RelFieldCollation.Direction.ASCENDING)));

                break;
            }
        }

        schema.createTable(cacheInfo, typeDescriptor.tableName(), type, collation);
    }

    @Override public synchronized void onSqlTypeDrop(String schemaName, GridQueryTypeDescriptor typeDescriptor, GridCacheContextInfo cacheInfo) {
        IgniteSchema schema = schemas.get(schemaName);
        assert schema != null;
        schema.dropTable(typeDescriptor);
    }

    public synchronized void onIndexCreate(String schemaName, String tblName, String idxName, String indexViewSql, GridQueryIndexDescriptor idxDesc) {
        IgniteSchema schema = schemas.get(schemaName);
        assert schema != null;
        System.out.println("INDEXES!");

        IgniteTable tbl = (IgniteTable)schema.getTable(tblName);
        String[] fieldsArr = tbl.igniteRowType().fields();
        HashMap<String, Integer> fieldsMap = new HashMap<>(fieldsArr.length);

        for (int i = 0; i < fieldsArr.length; i++)
            fieldsMap.put(fieldsArr[i].toUpperCase(), i);

        List<RelFieldCollation> collations = new ArrayList<>(fieldsArr.length);

        for (String field : idxDesc.fields()) {
            Integer idx = fieldsMap.get(field.toUpperCase());

            boolean descending = idxDesc.descending(field);

            RelFieldCollation collation = new RelFieldCollation(idx,
                descending ? RelFieldCollation.Direction.DESCENDING : RelFieldCollation.Direction.ASCENDING);

            collations.add(collation);
        }


        schema.createIndex(idxName, tbl.cacheName(), tbl.name(), tbl.igniteRowType(), RelCollations.of(collations), indexViewSql);
    }

    public synchronized void onIndexDrop() {
        // TODO: CODE: implement.
    }

    private class IgniteTableFactory implements MaterializationService.TableFactory {

        @Override public Table createTable(CalciteSchema schema, String viewSql, List<String> viewSchemaPath) {
            return null; // TODO: CODE: implement.
        }
    }
}
