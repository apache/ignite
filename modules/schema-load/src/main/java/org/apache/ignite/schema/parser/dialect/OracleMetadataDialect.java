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
    private static final String SQL_COLUMNS = "SELECT a.owner, a.table_name, a.column_name, a.nullable," +
        " a.data_type, a.data_precision, a.data_scale " +
        "FROM all_tab_columns a %s" +
        " WHERE a.owner = '%s'" +
        " ORDER BY a.owner, a.table_name, a.column_id";

    /** SQL to get list of PRIMARY KEYS columns. */
    private static final String SQL_PRIMARY_KEYS = "SELECT b.column_name" +
        " FROM all_constraints a" +
        "  INNER JOIN all_cons_columns b ON a.owner = b.owner AND a.constraint_name = b.constraint_name" +
        " WHERE a.table_name = ? AND a.constraint_type = 'P'";

    /** Owner index. */
    private static final int OWNER_IDX = 1;

    /** Table name index. */
    private static final int TABLE_NAME_IDX = 2;

    /** Column name index. */
    private static final int COLUMN_NAME_IDX = 3;

    /** Nullable index. */
    private static final int NULLABLE_IDX = 4;

    /** Data type index. */
    private static final int DATA_TYPE_IDX = 5;

    /** Numeric precision index. */
    private static final int DATA_PRECISION_IDX = 6;

    /** Numeric scale index. */
    private static final int DATA_SCALE_IDX = 7;

    /**
     * @param rs Result set with column type metadata from Oracle database.
     * @return JDBC type.
     * @throws SQLException If failed to decode type.
     */
    private static int decodeType(ResultSet rs) throws SQLException {
        switch (rs.getString(DATA_TYPE_IDX)) {
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
                int precision = rs.getInt(DATA_PRECISION_IDX);
                int scale = rs.getInt(DATA_SCALE_IDX);

                if (scale > 0) {
                    if (scale < 4 && precision < 19)
                        return FLOAT;

                    if (scale > 4 || precision > 19)
                        return DOUBLE;

                    return NUMERIC;
                }
                else {
                    if (precision < 1)
                        return INTEGER;

                    if (precision < 2)
                        return BOOLEAN;

                    if (precision < 4)
                        return TINYINT;

                    if (precision < 6)
                        return SMALLINT;

                    if (precision < 11)
                        return INTEGER;

                    if (precision < 20)
                        return BIGINT;

                    return NUMERIC;
                }

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

    /** {@inheritDoc} */
    @Override public Collection<DbTable> tables(Connection conn, boolean tblsOnly) throws SQLException {
        Collection<DbTable> tbls = new ArrayList<>();

        PreparedStatement pkStmt = conn.prepareStatement(SQL_PRIMARY_KEYS);

        try (Statement colsStmt = conn.createStatement()) {
            Collection<DbColumn> cols = new ArrayList<>();

            Set<String> pkCols = new HashSet<>();

            String owner = conn.getMetaData().getUserName().toUpperCase();

            String sql = String.format(SQL_COLUMNS,
                tblsOnly ? "INNER JOIN all_tables b on a.table_name = b.table_name" : "", owner);

            try (ResultSet colsRs = colsStmt.executeQuery(sql)) {
                String prevSchema = "";
                String prevTbl = "";

                while (colsRs.next()) {
                    String schema = colsRs.getString(OWNER_IDX);
                    String tbl = colsRs.getString(TABLE_NAME_IDX);

                    if (!prevSchema.equals(schema) || !prevTbl.equals(tbl)) {
                        pkCols.clear();

                        pkStmt.setString(1, tbl);

                        try (ResultSet pkRs = pkStmt.executeQuery()) {
                            while(pkRs.next())
                                pkCols.add(pkRs.getString(1));
                        }
                    }

                    if (prevSchema.isEmpty()) {
                        prevSchema = schema;
                        prevTbl = tbl;
                    }

                    if (!schema.equals(prevSchema) || !tbl.equals(prevTbl)) {
                        tbls.add(new DbTable(prevSchema, prevTbl, cols));

                        prevSchema = schema;
                        prevTbl = tbl;

                        cols = new ArrayList<>();
                    }

                    String colName = colsRs.getString(COLUMN_NAME_IDX);

                    cols.add(new DbColumn(colName, decodeType(colsRs), pkCols.contains(colName),
                        !"N".equals(colsRs.getString(NULLABLE_IDX))));
                }

                if (!cols.isEmpty())
                    tbls.add(new DbTable(prevSchema, prevTbl, cols));
            }
        }

        return tbls;
    }
}
