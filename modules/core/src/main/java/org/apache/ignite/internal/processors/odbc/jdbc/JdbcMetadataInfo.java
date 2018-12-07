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
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;

import static org.apache.ignite.internal.processors.query.QueryUtils.matches;

/**
 * Facade over {@link GridKernalContext} to get information about database entities in terms of JDBC.
 */
public class JdbcMetadataInfo {
    /** Root context. Used to get all the database metadata. */
    private final GridKernalContext ctx;

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
}
