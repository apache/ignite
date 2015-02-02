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

import org.apache.ignite.schema.parser.*;

import java.sql.*;
import java.util.*;

/**
 * Metadata dialect that uses standard JDBC for reading metadata.
 */
public class JdbcMetadataDialect extends DatabaseMetadataDialect {
    /** */
    private static final String[] TABLES_ONLY = {"TABLE"};

    /** */
    private static final String[] TABLES_AND_VIEWS = {"TABLE", "VIEW"};

    /** {@inheritDoc} */
    @Override public Collection<DbTable> tables(Connection conn, boolean tblsOnly) throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();

        Set<String> sys = systemSchemas();

        Collection<DbTable> tbls = new ArrayList<>();

        try (ResultSet schemasRs = dbMeta.getSchemas()) {
            while (schemasRs.next()) {
                String schema = schemasRs.getString("TABLE_SCHEM");

                // Skip system schemas.
                if (sys.contains(schema))
                    continue;

                String catalog = schemasRs.getString("TABLE_CATALOG");

                try (ResultSet tblsRs = dbMeta.getTables(catalog, schema, "%",
                    tblsOnly ? TABLES_ONLY : TABLES_AND_VIEWS)) {
                    while (tblsRs.next()) {
                        String tblName = tblsRs.getString("TABLE_NAME");

                        Set<String> pkCols = new HashSet<>();

                        try (ResultSet pkRs = dbMeta.getPrimaryKeys(catalog, schema, tblName)) {
                            while (pkRs.next())
                                pkCols.add(pkRs.getString("COLUMN_NAME"));
                        }

                        List<DbColumn> cols = new ArrayList<>();

                        try (ResultSet colsRs = dbMeta.getColumns(catalog, schema, tblName, null)) {
                            while (colsRs.next()) {
                                String colName = colsRs.getString("COLUMN_NAME");

                                cols.add(new DbColumn(
                                    colName,
                                    colsRs.getInt("DATA_TYPE"),
                                    pkCols.contains(colName),
                                    colsRs.getInt("NULLABLE") == DatabaseMetaData.columnNullable));
                            }
                        }

                        Set<String> ascCols = new HashSet<>();

                        Set<String> descCols = new HashSet<>();

                        Map<String, Map<String, Boolean>> idxs = new LinkedHashMap<>();

                        try (ResultSet idxRs = dbMeta.getIndexInfo(catalog, schema, tblName, false, true)) {
                            while (idxRs.next()) {
                                String idxName = idxRs.getString("INDEX_NAME");

                                String colName = idxRs.getString("COLUMN_NAME");

                                if (idxName == null || colName == null)
                                    continue;

                                String askOrDesc = idxRs.getString("ASC_OR_DESC");

                                Boolean desc = askOrDesc != null ? "D".equals(askOrDesc) : null;

                                if (desc != null) {
                                    if (desc)
                                        descCols.add(colName);
                                    else
                                        ascCols.add(colName);
                                }
                            }
                        }

                        tbls.add(new DbTable(schema, tblName, cols, ascCols, descCols, idxs));
                    }
                }
            }
        }

        return tbls;
    }
}
