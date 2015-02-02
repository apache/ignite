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

import static java.sql.Types.*;

/**
 * Oracle specific metadata dialect.
 */
public class OracleMetadataDialect extends DatabaseMetadataDialect {
    /** SQL to get columns metadata. */
    private static final String SQL_COLUMNS = "SELECT a.owner, a.table_name, a.column_name, a.nullable, a.data_type" +
        " FROM all_tab_columns a" +
        " %s" +
        " WHERE a.owner = '%s'" +
        " ORDER BY a.owner, a.table_name, a.column_id";

    private static final int SQL_COLS_OWNER = 1;

    private static final int SQL_COLS_TAB_NAME = 2;

    private static final int SQL_COLS_COL_NAME = 3;

    private static final int SQL_COLS_NULLABLE = 4;

    private static final int SQL_COLS_DATA_TYPE = 5;

    /** SQL to get indexes metadata. */
    private static final String SQL_INDEXES = "select index_name, column_name, descend" +
        " FROM all_ind_columns" +
        " WHERE index_owner = ? and table_name = ?" +
        "  ORDER BY index_name, column_position";

    /**
     * @param type Column type from Oracle database.
     * @return JDBC type.
     */
    private static int decodeType(String type) {
        switch (type) {
            case "CHAR":
            case "NCHAR":
                return CHAR;

            case "VARCHAR2":
            case "NVARCHAR2":
                return VARCHAR;

            case "LONG":
                return LONGVARCHAR;

            case "LONG RAW":
                return LONGVARBINARY;

            case "FLOAT":
                return FLOAT;

            case "NUMBER":
                return NUMERIC;

            case "DATE":
                return DATE;

            case "TIMESTAMP":
                return TIMESTAMP;

            case "BFILE":
            case "BLOB":
                return BLOB;

            case "CLOB":
            case "NCLOB":
            case "XMLTYPE":
                return CLOB;
        }

        return OTHER;
    }

    /**
     * @param nullable Column nullable attribute from Oracle database.
     * @return {@code true}
     */
    private static boolean decodeNullable(String nullable) {
        return "Y".equals(nullable);
    }

    /**
     * @param descend Index column sort direction from Oracle database.
     * @return {@code true} if column sorted in descent direction.
     */
    private static Boolean decodeDescend(String descend) {
        return descend != null ? "DESC".equals(descend) : null;
    }

    private static Map<String, Map<String, Boolean>> indexes(PreparedStatement stmt, String owner, String tbl)
        throws SQLException {
        Map<String, Map<String, Boolean>> idxs = new LinkedHashMap<>();

        stmt.setString(1, owner);
        stmt.setString(2, tbl);

        try (ResultSet idxsRs = stmt.executeQuery()) {
            while (idxsRs.next()) {
                String idxName = idxsRs.getString("INDEX_NAME");
            }
        }

        return idxs;
    }

    /** {@inheritDoc} */
    @Override public Collection<DbTable> tables(Connection conn, boolean tblsOnly) throws SQLException {
        Collection<DbTable> tbls = new ArrayList<>();

        PreparedStatement stmtIdxs = conn.prepareStatement(SQL_INDEXES);

        try (Statement stmt = conn.createStatement()) {
            Collection<DbColumn> cols = new ArrayList<>();

            String sql = String.format(SQL_COLUMNS,
                tblsOnly ? "INNER JOIN all_tables b on a.table_name = b.table_name" : "", "TEST");

            try (ResultSet colsRs = stmt.executeQuery(sql)) {
                String prevSchema = "";
                String prevTbl = "";

                while (colsRs.next()) {
                    String schema = colsRs.getString("OWNER");
                    String tbl = colsRs.getString("TABLE_NAME");

                    if (prevSchema.isEmpty()) {
                        prevSchema = schema;
                        prevTbl = tbl;
                    }

                    if (!schema.equals(prevSchema) || !tbl.equals(prevTbl)) {
                        tbls.add(new DbTable(prevSchema, prevTbl, cols, Collections.<String>emptySet(),
                            Collections.<String>emptySet(), null));

                        prevSchema = schema;
                        prevTbl = tbl;

                        cols = new ArrayList<>();
                    }
                    cols.add(new DbColumn(colsRs.getString("COLUMN_NAME"),
                        decodeType(colsRs.getString("DATA_TYPE")),
                        false,
                        decodeNullable(colsRs.getString("NULLABLE"))
                    ));
                }

                if (!cols.isEmpty())
                    tbls.add(new DbTable(prevSchema, prevTbl, cols,
                        Collections.<String>emptySet(), Collections.<String>emptySet(),
                        Collections.<String, Map<String, Boolean>>emptyMap()));
            }
        }

        return tbls;
    }
}
