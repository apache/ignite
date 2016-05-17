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

package org.apache.ignite.schema.parser.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.schema.parser.DbColumn;
import org.apache.ignite.schema.parser.DbTable;

/**
 * Metadata dialect that uses standard JDBC for reading metadata.
 */
public class JdbcMetadataDialect extends DatabaseMetadataDialect {
    /** */
    private static final String[] TABLES_ONLY = {"TABLE"};

    /** */
    private static final String[] TABLES_AND_VIEWS = {"TABLE", "VIEW"};

    /** Schema catalog index. */
    private static final int TBL_CATALOG_IDX = 1;

    /** Schema name index. */
    private static final int TBL_SCHEMA_IDX = 2;

    /** Table name index. */
    private static final int TBL_NAME_IDX = 3;

    /** Primary key column name index. */
    private static final int PK_COL_NAME_IDX = 4;

    /** Column name index. */
    private static final int COL_NAME_IDX = 4;

    /** Column data type index. */
    private static final int COL_DATA_TYPE_IDX = 5;

    /** Column nullable index. */
    private static final int COL_NULLABLE_IDX = 11;

    /** Index name index. */
    private static final int IDX_NAME_IDX = 6;

    /** Index column name index. */
    private static final int IDX_COL_NAME_IDX = 9;

    /** Index column descend index. */
    private static final int IDX_ASC_OR_DESC_IDX = 10;

    /** {@inheritDoc} */
    @Override public Collection<String> schemas(Connection conn) throws SQLException {
        Collection<String> schemas = new ArrayList<>();

        ResultSet rs = conn.getMetaData().getSchemas();

        Set<String> sys = systemSchemas();

        while(rs.next()) {
            String schema = rs.getString(1);

            // Skip system schemas.
            if (sys.contains(schema))
                continue;

            schemas.add(schema);
        }

        return schemas;
    }

    /**
     * @return If {@code true} use catalogs for table division.
     */
    protected boolean useCatalog() {
        return false;
    }

    /**
     * @return If {@code true} use schemas for table division.
     */
    protected boolean useSchema() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Collection<DbTable> tables(Connection conn, List<String> schemas, boolean tblsOnly)
        throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();

        Set<String> sys = systemSchemas();

        Collection<DbTable> tbls = new ArrayList<>();

        if (schemas.isEmpty())
            schemas.add(null);

        for (String toSchema: schemas) {
            try (ResultSet tblsRs = dbMeta.getTables(useCatalog() ? toSchema : null, useSchema() ? toSchema : null, "%",
                    tblsOnly ? TABLES_ONLY : TABLES_AND_VIEWS)) {
                while (tblsRs.next()) {
                    String tblCatalog = tblsRs.getString(TBL_CATALOG_IDX);
                    String tblSchema = tblsRs.getString(TBL_SCHEMA_IDX);
                    String tblName = tblsRs.getString(TBL_NAME_IDX);

                    // In case of MySql we should use catalog.
                    String schema = tblSchema != null ? tblSchema : tblCatalog;

                    // Skip system schemas.
                    if (sys.contains(schema))
                        continue;

                    Set<String> pkCols = new HashSet<>();

                    try (ResultSet pkRs = dbMeta.getPrimaryKeys(tblCatalog, tblSchema, tblName)) {
                        while (pkRs.next())
                            pkCols.add(pkRs.getString(PK_COL_NAME_IDX));
                    }

                    List<DbColumn> cols = new ArrayList<>();

                    try (ResultSet colsRs = dbMeta.getColumns(tblCatalog, tblSchema, tblName, null)) {
                        while (colsRs.next()) {
                            String colName = colsRs.getString(COL_NAME_IDX);

                            cols.add(new DbColumn(
                                    colName,
                                    colsRs.getInt(COL_DATA_TYPE_IDX),
                                    pkCols.contains(colName),
                                    colsRs.getInt(COL_NULLABLE_IDX) == DatabaseMetaData.columnNullable));
                        }
                    }

                    Map<String, QueryIndex> idxs = new LinkedHashMap<>();

                    try (ResultSet idxRs = dbMeta.getIndexInfo(tblCatalog, tblSchema, tblName, false, true)) {
                        while (idxRs.next()) {
                            String idxName = idxRs.getString(IDX_NAME_IDX);

                            String colName = idxRs.getString(IDX_COL_NAME_IDX);

                            if (idxName == null || colName == null)
                                continue;

                            QueryIndex idx = idxs.get(idxName);

                            if (idx == null) {
                                idx = new QueryIndex();
                                idx.setName(idxName);
                                idx.setIndexType(QueryIndexType.SORTED);
                                idx.setFields(new LinkedHashMap<String, Boolean>());

                                idxs.put(idxName, idx);
                            }

                            String askOrDesc = idxRs.getString(IDX_ASC_OR_DESC_IDX);

                            Boolean asc = askOrDesc == null || "A".equals(askOrDesc);

                            idx.getFields().put(colName, asc);
                        }
                    }

                    tbls.add(table(schema, tblName, cols, idxs.values()));
                }
            }
        }

        return tbls;
    }
}
