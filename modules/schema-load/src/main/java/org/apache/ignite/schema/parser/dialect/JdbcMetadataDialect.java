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

    /** Schema name index. */
    private static final int SCHEMA_NAME_IDX = 1;

    /** Schema catalog index. */
    private static final int SCHEMA_CATALOG_IDX = 2;

    /** Table name index. */
    private static final int TABLE_NAME_IDX = 3;

    /** Primary key column name index. */
    private static final int PK_COLUMN_NAME_IDX = 4;

    /** Column name index. */
    private static final int COLUMN_NAME_IDX = 4;

    /** Column data type index. */
    private static final int COLUMN_DATA_TYPE_IDX = 5;

    /** Column nullable index. */
    private static final int COLUMN_NULLABLE_IDX = 11;


    /** {@inheritDoc} */
    @Override public Collection<DbTable> tables(Connection conn, boolean tblsOnly) throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();

        Set<String> sys = systemSchemas();

        Collection<DbTable> tbls = new ArrayList<>();

        try (ResultSet schemasRs = dbMeta.getSchemas()) {
            while (schemasRs.next()) {
                String schema = schemasRs.getString(SCHEMA_NAME_IDX);

                // Skip system schemas.
                if (sys.contains(schema))
                    continue;

                String catalog = schemasRs.getString(SCHEMA_CATALOG_IDX);

                try (ResultSet tblsRs = dbMeta.getTables(catalog, schema, "%",
                    tblsOnly ? TABLES_ONLY : TABLES_AND_VIEWS)) {
                    while (tblsRs.next()) {
                        String tblName = tblsRs.getString(TABLE_NAME_IDX);

                        Set<String> pkCols = new HashSet<>();

                        try (ResultSet pkRs = dbMeta.getPrimaryKeys(catalog, schema, tblName)) {
                            while (pkRs.next())
                                pkCols.add(pkRs.getString(PK_COLUMN_NAME_IDX));
                        }

                        List<DbColumn> cols = new ArrayList<>();

                        try (ResultSet colsRs = dbMeta.getColumns(catalog, schema, tblName, null)) {
                            while (colsRs.next()) {
                                String colName = colsRs.getString(COLUMN_NAME_IDX);

                                cols.add(new DbColumn(
                                    colName,
                                    colsRs.getInt(COLUMN_DATA_TYPE_IDX),
                                    pkCols.contains(colName),
                                    colsRs.getInt(COLUMN_NULLABLE_IDX) == DatabaseMetaData.columnNullable));
                            }
                        }

                        tbls.add(new DbTable(schema, tblName, cols));
                    }
                }
            }
        }

        return tbls;
    }
}
