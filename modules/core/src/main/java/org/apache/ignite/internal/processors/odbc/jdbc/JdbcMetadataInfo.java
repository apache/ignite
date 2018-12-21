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
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
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

    /** The only one possible value of table type. */
    public static final String TABLE_TYPE = "TABLE";

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
    public Collection<JdbcPrimaryKeyMeta> getPrimaryKeys(String schemaNamePtrn, String tableNamePtrn) {
        Collection<JdbcPrimaryKeyMeta> meta = new HashSet<>();

        for (String cacheName : ctx.cache().publicCacheNames()) {
            for (GridQueryTypeDescriptor table : ctx.query().types(cacheName)) {
                if (!matches(table.schemaName(), schemaNamePtrn))
                    continue;

                if (!matches(table.tableName(), tableNamePtrn))
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
     * @param schemaPtrn sql pattern for schema name.
     * @param tabPtrn sql pattern for table name.
     * @return List of metadatas of tables that matches .
     */
    public List<JdbcTableMeta> getTablesMeta(String schemaPtrn, String tabPtrn) {
        Comparator<JdbcTableMeta> bySchemaThenTabname = new Comparator<JdbcTableMeta>() {
            @Override public int compare(JdbcTableMeta o1, JdbcTableMeta o2) {
                int schemCmp = o1.schemaName().compareTo(o2.schemaName());

                if (schemCmp != 0)
                    return schemCmp;

                return o1.tableName().compareTo(o2.tableName());
            }
        };

        TreeSet<JdbcTableMeta> tabMetas = new TreeSet<>(bySchemaThenTabname);

        for (String cacheName : ctx.cache().publicCacheNames()) {
            for (GridQueryTypeDescriptor table : ctx.query().types(cacheName)) {
                if (!matches(table.schemaName(), schemaPtrn))
                    continue;

                if (!matches(table.tableName(), tabPtrn))
                    continue;

                JdbcTableMeta tableMeta = new JdbcTableMeta(table.schemaName(), table.tableName(), TABLE_TYPE);

                tabMetas.add(tableMeta);
            }
        }

        return new ArrayList<>(tabMetas);
    }

    /**
     * See {@link DatabaseMetaData#getColumns(String, String, String, String)} for details.
     *
     * Ignite has only one possible CATALOG_NAME, it is handled on the client (driver) side.
     *
     * @param protocolVer for what version of protocol to generate metadata. Early versions of protocol don't support
     * some features like default values or precision/scale. If {@code null}, current version will be used.
     * @return List of metadatas about columns that match specified schema/tablename/columnname criterias.
     */
    public Collection<JdbcColumnMeta> getColumnsMeta(@Nullable ClientListenerProtocolVersion protocolVer,
        String schemaNamePtrn, String tableNamePtrn, String columnNamePtrn) {

        boolean useNewest = protocolVer == null;

        Collection<JdbcColumnMeta> metas = new LinkedHashSet<>();

        for (String cacheName : ctx.cache().publicCacheNames()) {
            for (GridQueryTypeDescriptor table : ctx.query().types(cacheName)) {
                if (!matches(table.schemaName(), schemaNamePtrn))
                    continue;

                if (!matches(table.tableName(), tableNamePtrn))
                    continue;

                for (Map.Entry<String, Class<?>> field : table.fields().entrySet()) {
                    String colName = field.getKey();

                    Class<?> fieldCls = field.getValue();

                    if (!matches(colName, columnNamePtrn))
                        continue;

                    JdbcColumnMeta columnMeta;

                    if (useNewest || protocolVer.compareTo(VER_2_7_0) >= 0) {
                        GridQueryProperty prop = table.property(colName);

                        columnMeta = new JdbcColumnMetaV4(table.schemaName(), table.tableName(),
                            colName, fieldCls, !prop.notNull(), prop.defaultValue(),
                            prop.precision(), prop.scale());
                    }
                    else if (protocolVer.compareTo(VER_2_4_0) >= 0) {
                        GridQueryProperty prop = table.property(colName);

                        columnMeta = new JdbcColumnMetaV3(table.schemaName(), table.tableName(),
                            colName, fieldCls, !prop.notNull(), prop.defaultValue());
                    }
                    else if (protocolVer.compareTo(VER_2_3_0) >= 0) {
                        GridQueryProperty prop = table.property(colName);

                        columnMeta = new JdbcColumnMetaV2(table.schemaName(), table.tableName(),
                            colName, fieldCls, !prop.notNull());
                    }
                    else
                        columnMeta = new JdbcColumnMeta(table.schemaName(), table.tableName(),
                            colName, fieldCls);

                    if (!metas.contains(columnMeta))
                        metas.add(columnMeta);
                }
            }
        }

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

        for (String cacheName : ctx.cache().publicCacheNames()) {
            for (GridQueryTypeDescriptor table : ctx.query().types(cacheName)) {
                if (matches(table.schemaName(), schemaNamePtrn))
                    schemas.add(table.schemaName());
            }
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
    public SortedSet<JdbcIndexMeta> getIndexesMeta(String schemaNamePtrn, String tableNamePtrn) {
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

                if (!matches(table.tableName(), tableNamePtrn))
                    continue;

                for (GridQueryIndexDescriptor idxDesc : table.indexes().values())
                    meta.add(new JdbcIndexMeta(table.schemaName(), table.tableName(), idxDesc));
            }
        }

        return meta;
    }
}