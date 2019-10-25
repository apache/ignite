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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.query.ColumnInformation;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.TableInformation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_3_0;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_4_0;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext.VER_2_7_0;
import static org.apache.ignite.internal.processors.query.QueryUtils.matches;

/**
 * Facade over {@link GridKernalContext} to get information about database entities in terms of JDBC.
 */
public class JdbcMetadataInfo {
    /** Root context. Used to get all the database metadata. */
    private final GridKernalContext ctx;

    /** Comparator for {@link ColumnInformation} by schema then table name then column order. */
    private static final Comparator<ColumnInformation> bySchemaThenTabNameThenColOrder = new Comparator<ColumnInformation>() {
        @Override public int compare(ColumnInformation o1, ColumnInformation o2) {
            int schemaCmp = o1.schemaName().compareTo(o2.schemaName());

            if (schemaCmp != 0)
                return schemaCmp;

            int tblNameCmp = o1.tableName().compareTo(o2.tableName());

            if (tblNameCmp != 0)
                return tblNameCmp;

            return Integer.compare(o1.columnId(), o2.columnId());
        }
    };

    /** Comparator for {@link JdbcTableMeta} by table type then schema then table name. */
    private static final Comparator<TableInformation> byTblTypeThenSchemaThenTblName =
        new Comparator<TableInformation>() {
            @Override public int compare(TableInformation o1, TableInformation o2) {
                int tblTypeCmp = o1.tableType().compareTo(o2.tableType());

                if (tblTypeCmp != 0)
                    return tblTypeCmp;

                int schemCmp = o1.schemaName().compareTo(o2.schemaName());

                if (schemCmp != 0)
                    return schemCmp;

                return o1.tableName().compareTo(o2.tableName());
            }
        };

    /**
     * Initializes info.
     *
     * @param ctx GridKernalContext
     */
    public JdbcMetadataInfo(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * See {@link DatabaseMetaData#getPrimaryKeys(String, String, String)} for details.
     *
     * Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @return Collection of primary keys information for tables that matches specified schema and table name patterns.
     */
    public Collection<JdbcPrimaryKeyMeta> getPrimaryKeys(String schemaNamePtrn, String tblNamePtrn) {
        Collection<JdbcPrimaryKeyMeta> meta = new HashSet<>();

        for (String cacheName : ctx.cache().publicCacheNames()) {
            for (GridQueryTypeDescriptor table : ctx.query().types(cacheName)) {
                if (!matches(table.schemaName(), schemaNamePtrn))
                    continue;

                if (!matches(table.tableName(), tblNamePtrn))
                    continue;

                List<String> fields = new ArrayList<>();

                for (String field : table.fields().keySet()) {
                    if (table.property(field).key())
                        fields.add(field);
                }

                final String keyName = table.keyFieldName() == null ?
                    "PK_" + table.schemaName() + "_" + table.tableName() :
                    table.keyFieldName();

                if (fields.isEmpty()) {
                    String keyColName =
                        table.keyFieldName() == null ? QueryUtils.KEY_FIELD_NAME : table.keyFieldName();

                    meta.add(new JdbcPrimaryKeyMeta(table.schemaName(), table.tableName(), keyName,
                        Collections.singletonList(keyColName)));
                }
                else
                    meta.add(new JdbcPrimaryKeyMeta(table.schemaName(), table.tableName(), keyName, fields));
            }
        }

        return meta;
    }

    /**
     * See {@link DatabaseMetaData#getTables(String, String, String, String[])} for details.
     *
     * Ignite has only one possible value for CATALOG_NAME and has only one table type so these parameters are handled
     * on the client (driver) side.
     *
     * Result is ordered by (schema name, table name).
     *
     * @param schemaNamePtrn sql pattern for schema name.
     * @param tblNamePtrn sql pattern for table name.
     * @param tblTypes Requested table types.
     * @return List of metadatas of tables that matches.
     */
    public List<JdbcTableMeta> getTablesMeta(String schemaNamePtrn, String tblNamePtrn, String[] tblTypes) {
        Collection<TableInformation> tblsMeta = ctx.query().getIndexing()
            .tablesInformation(schemaNamePtrn, tblNamePtrn, tblTypes);

        return tblsMeta.stream()
            .sorted(byTblTypeThenSchemaThenTblName)
            .map(t -> new JdbcTableMeta(t.schemaName(), t.tableName(), t.tableType()))
            .collect(Collectors.toList());
    }

    /**
     * See {@link DatabaseMetaData#getColumns(String, String, String, String)} for details.
     *
     * Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @param protoVer for what version of protocol to generate metadata. Early versions of protocol don't support some
     * features like default values or precision/scale. If {@code null}, current version will be used.
     * @return List of metadatas about columns that match specified schema/tablename/columnname criterias.
     */
    public Collection<JdbcColumnMeta> getColumnsMeta(@Nullable ClientListenerProtocolVersion protoVer,
        String schemaNamePtrn, String tblNamePtrn, String colNamePtrn) {

        boolean useNewest = protoVer == null;

        Collection<JdbcColumnMeta> metas = new LinkedHashSet<>();

        Collection<ColumnInformation> colsInfo = ctx.query().getIndexing()
            .columnsInformation(schemaNamePtrn, tblNamePtrn, colNamePtrn);

        colsInfo.stream().sorted(bySchemaThenTabNameThenColOrder)
            .forEachOrdered(info -> {
                JdbcColumnMeta colMeta;

                if (useNewest || protoVer.compareTo(VER_2_7_0) >= 0) {
                    colMeta = new JdbcColumnMetaV4(info.schemaName(), info.tableName(), info.columnName(),
                        info.fieldClass(), info.nullable(), info.defaultValue(), info.precision(), info.scale());
                }
                else if (protoVer.compareTo(VER_2_4_0) >= 0) {
                    colMeta = new JdbcColumnMetaV3(info.schemaName(), info.tableName(), info.columnName(),
                        info.fieldClass(), info.nullable(), info.defaultValue());
                }
                else if (protoVer.compareTo(VER_2_3_0) >= 0) {
                    colMeta = new JdbcColumnMetaV2(info.schemaName(), info.tableName(), info.columnName(),
                        info.fieldClass(), info.nullable());
                }
                else
                    colMeta = new JdbcColumnMeta(info.schemaName(), info.tableName(), info.columnName(),
                        info.fieldClass());

                if (!metas.contains(colMeta))
                    metas.add(colMeta);
            });

        return metas;
    }

    /**
     * See {@link DatabaseMetaData#getSchemas(String, String)} for details.
     *
     * Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @param schemaNamePtrn sql pattern for schema name filter.
     * @return schema names that matches provided pattern.
     */
    public SortedSet<String> getSchemasMeta(String schemaNamePtrn) {
        SortedSet<String> schemas = new TreeSet<>(); // to have values sorted.

        for (String schema : ctx.query().getIndexing().schemasNames()) {
            if (matches(schema, schemaNamePtrn))
                schemas.add(schema);
        }

        return schemas;
    }

    /**
     * See {@link DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)} for details.
     *
     * Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side. Parameters {@code unique}
     * {@code approximate} are ignored.
     *
     * @return Sorted by index name collection of index info, filtered according to specified criterias.
     */
    public SortedSet<JdbcIndexMeta> getIndexesMeta(String schemaNamePtrn, String tblNamePtrn) {
        final Comparator<JdbcIndexMeta> byIndexName = new Comparator<JdbcIndexMeta>() {
            @Override public int compare(JdbcIndexMeta o1, JdbcIndexMeta o2) {
                return o1.indexName().compareTo(o2.indexName());
            }
        };

        TreeSet<JdbcIndexMeta> meta = new TreeSet<>(byIndexName);

        for (String cacheName : ctx.cache().publicCacheNames()) {
            for (GridQueryTypeDescriptor table : ctx.query().types(cacheName)) {
                if (!matches(table.schemaName(), schemaNamePtrn))
                    continue;

                if (!matches(table.tableName(), tblNamePtrn))
                    continue;

                for (GridQueryIndexDescriptor idxDesc : table.indexes().values())
                    meta.add(new JdbcIndexMeta(table.schemaName(), table.tableName(), idxDesc));
            }
        }

        return meta;
    }
}
