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

package org.apache.ignite.console.agent.db.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.console.agent.db.DbColumn;
import org.apache.ignite.console.agent.db.DbTable;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;

/**
 * Oracle specific metadata dialect.
 */
public class OracleMetadataDialect extends DatabaseMetadataDialect {
    /** SQL to get columns metadata. */
    private static final String SQL_COLUMNS = "SELECT a.owner, a.table_name, a.column_name, a.nullable," +
        " a.data_type, a.data_precision, a.data_scale" +
        " FROM all_tab_columns a %s" +
        " %s " +
        " ORDER BY a.owner, a.table_name, a.column_id";

    /** SQL to get list of PRIMARY KEYS columns. */
    private static final String SQL_PRIMARY_KEYS = "SELECT b.column_name" +
        " FROM all_constraints a" +
        "  INNER JOIN all_cons_columns b" +
        "   ON a.owner = b.owner" +
        "  AND a.constraint_name = b.constraint_name" +
        " WHERE a.owner = ? and a.table_name = ? AND a.constraint_type = 'P'";

    /** SQL to get list of UNIQUE INDEX columns. */
    private static final String SQL_UNIQUE_INDEXES_KEYS = "SELECT a.index_name, b.column_name" +
        " FROM all_indexes a" +
        " INNER JOIN all_ind_columns b" +
        "   ON a.index_name = b.index_name" +
        "  AND a.table_owner = b.table_owner" +
        "  AND a.table_name = b.table_name" +
        "  AND a.owner = b.index_owner" +
        " WHERE a.owner = ? AND a.table_name = ? AND a.uniqueness = 'UNIQUE'" +
        " ORDER BY b.column_position";

    /** SQL to get indexes metadata. */
    private static final String SQL_INDEXES = "SELECT i.index_name, u.column_expression, i.column_name, i.descend" +
        " FROM all_ind_columns i" +
        " LEFT JOIN user_ind_expressions u" +
        "   ON u.index_name = i.index_name" +
        "  AND i.table_name = u.table_name" +
        " WHERE i.index_owner = ? and i.table_name = ?" +
        " ORDER BY i.index_name, i.column_position";

    /** Owner index. */
    private static final int OWNER_IDX = 1;

    /** Table name index. */
    private static final int TBL_NAME_IDX = 2;

    /** Column name index. */
    private static final int COL_NAME_IDX = 3;

    /** Nullable index. */
    private static final int NULLABLE_IDX = 4;

    /** Data type index. */
    private static final int DATA_TYPE_IDX = 5;

    /** Numeric precision index. */
    private static final int DATA_PRECISION_IDX = 6;

    /** Numeric scale index. */
    private static final int DATA_SCALE_IDX = 7;

    /** Unique index name index. */
    private static final int UNQ_IDX_NAME_IDX = 1;

    /** Unique index column name index. */
    private static final int UNQ_IDX_COL_NAME_IDX = 2;

    /** Index name index. */
    private static final int IDX_NAME_IDX = 1;

    /** Index name index. */
    private static final int IDX_EXPR_IDX = 2;

    /** Index column name index. */
    private static final int IDX_COL_NAME_IDX = 3;

    /** Index column sort order index. */
    private static final int IDX_COL_DESCEND_IDX = 4;

    /** {@inheritDoc} */
    @Override public Set<String> systemSchemas() {
        return new HashSet<>(Arrays.asList("ANONYMOUS", "APPQOSSYS", "CTXSYS", "DBSNMP", "EXFSYS", "LBACSYS", "MDSYS",
            "MGMT_VIEW", "OLAPSYS", "OWBSYS", "ORDPLUGINS", "ORDSYS", "OUTLN", "SI_INFORMTN_SCHEMA", "SYS", "SYSMAN",
            "SYSTEM", "TSMSYS", "WK_TEST", "WKSYS", "WKPROXY", "WMSYS", "XDB",

            "APEX_040000", "APEX_PUBLIC_USER", "DIP", "FLOWS_30000", "FLOWS_FILES", "MDDATA", "ORACLE_OCM",
            "SPATIAL_CSW_ADMIN_USR", "SPATIAL_WFS_ADMIN_USR", "XS$NULL",

            "BI", "HR", "OE", "PM", "IX", "SH"));
    }

    /** {@inheritDoc} */
    @Override public Collection<String> schemas(Connection conn) throws SQLException {
        Collection<String> schemas = new ArrayList<>();

        ResultSet rs = conn.getMetaData().getSchemas();

        Set<String> sysSchemas = systemSchemas();

        while(rs.next()) {
            String schema = rs.getString(1);

            if (!sysSchemas.contains(schema) && !schema.startsWith("FLOWS_"))
                schemas.add(schema);
        }

        return schemas;
    }

    /**
     * @param rs Result set with column type metadata from Oracle database.
     * @return JDBC type.
     * @throws SQLException If failed to decode type.
     */
    private int decodeType(ResultSet rs) throws SQLException {
        String type = rs.getString(DATA_TYPE_IDX);

        if (type.startsWith("TIMESTAMP"))
            return TIMESTAMP;
        else {
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

                case "BFILE":
                case "BLOB":
                    return BLOB;

                case "CLOB":
                case "NCLOB":
                    return CLOB;

                case "XMLTYPE":
                    return SQLXML;
            }
        }

        return OTHER;
    }

    /**
     * Retrieve primary key columns.
     *
     * @param stmt Prepared SQL statement to execute.
     * @param owner DB owner.
     * @param tbl Table name.
     * @return Primary key columns.
     * @throws SQLException If failed to retrieve primary key columns.
     */
    private Set<String> primaryKeys(PreparedStatement stmt, String owner, String tbl) throws SQLException {
        stmt.setString(1, owner);
        stmt.setString(2, tbl);

        Set<String> pkCols = new LinkedHashSet<>();

        try (ResultSet pkRs = stmt.executeQuery()) {
            while(pkRs.next())
                pkCols.add(pkRs.getString(1));
        }

        return pkCols;
    }

    /**
     * Retrieve unique indexes with columns.
     *
     * @param stmt Prepared SQL statement to execute.
     * @param owner DB owner.
     * @param tbl Table name.
     * @return Unique indexes.
     * @throws SQLException If failed to retrieve unique indexes columns.
     */
    private Map<String, Set<String>> uniqueIndexes(PreparedStatement stmt, String owner, String tbl) throws SQLException {
        stmt.setString(1, owner);
        stmt.setString(2, tbl);

        Map<String, Set<String>> uniqueIdxs = new LinkedHashMap<>();

        try (ResultSet idxsRs = stmt.executeQuery()) {
            while (idxsRs.next()) {
                String idxName = idxsRs.getString(UNQ_IDX_NAME_IDX);
                String colName = idxsRs.getString(UNQ_IDX_COL_NAME_IDX);

                Set<String> idxCols = uniqueIdxs.get(idxName);

                if (idxCols == null) {
                    idxCols = new LinkedHashSet<>();

                    uniqueIdxs.put(idxName, idxCols);
                }

                idxCols.add(colName);
            }
        }

        return uniqueIdxs;
    }

    /**
     * Retrieve index columns.
     *
     * @param stmt Prepared SQL statement to execute.
     * @param owner DB owner.
     * @param tbl Table name.
     * @param uniqueIdxAsPk Optional unique index that used as PK.
     * @return Indexes.
     * @throws SQLException If failed to retrieve indexes columns.
     */
    private Collection<QueryIndex> indexes(PreparedStatement stmt, String owner, String tbl, String uniqueIdxAsPk) throws SQLException {
        stmt.setString(1, owner);
        stmt.setString(2, tbl);

        Map<String, QueryIndex> idxs = new LinkedHashMap<>();

        try (ResultSet idxsRs = stmt.executeQuery()) {
            while (idxsRs.next()) {
                String idxName = idxsRs.getString(IDX_NAME_IDX);

                // Skip unique index used as PK.
                if (idxName.equals(uniqueIdxAsPk))
                    continue;

                QueryIndex idx = idxs.get(idxName);

                if (idx == null) {
                    idx = index(idxName);

                    idxs.put(idxName, idx);
                }

                String expr = idxsRs.getString(IDX_EXPR_IDX);

                String col = expr == null ? idxsRs.getString(IDX_COL_NAME_IDX) : expr.replaceAll("\"", "");

                idx.getFields().put(col, !"DESC".equals(idxsRs.getString(IDX_COL_DESCEND_IDX)));
            }
        }

        return idxs.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<DbTable> tables(Connection conn, List<String> schemas, boolean tblsOnly) throws SQLException {
        PreparedStatement pkStmt = conn.prepareStatement(SQL_PRIMARY_KEYS);
        PreparedStatement uniqueIdxsStmt = conn.prepareStatement(SQL_UNIQUE_INDEXES_KEYS);
        PreparedStatement idxStmt = conn.prepareStatement(SQL_INDEXES);

        if (schemas.isEmpty())
            schemas.add(null);

        Set<String> sysSchemas = systemSchemas();

        Collection<DbTable> tbls = new ArrayList<>();

        try (Statement colsStmt = conn.createStatement()) {
            for (String schema: schemas) {
                if (systemSchemas().contains(schema) || (schema != null && schema.startsWith("FLOWS_")))
                    continue;

                String sql = String.format(SQL_COLUMNS,
                        tblsOnly ? "INNER JOIN all_tables b on a.table_name = b.table_name and a.owner = b.owner" : "",
                        schema != null ? String.format(" WHERE a.owner = '%s' ", schema) : "");

                try (ResultSet colsRs = colsStmt.executeQuery(sql)) {
                    String prevSchema = "";
                    String prevTbl = "";

                    boolean first = true;

                    Set<String> pkCols = Collections.emptySet();
                    Collection<DbColumn> cols = new ArrayList<>();
                    Collection<QueryIndex> idxs = Collections.emptyList();

                    while (colsRs.next()) {
                        String owner = colsRs.getString(OWNER_IDX);
                        String tbl = colsRs.getString(TBL_NAME_IDX);

                        if (sysSchemas.contains(owner) || (schema != null && schema.startsWith("FLOWS_")))
                            continue;

                        boolean changed = !owner.equals(prevSchema) || !tbl.equals(prevTbl);

                        if (changed) {
                            if (first)
                                first = false;
                            else
                                tbls.add(table(prevSchema, prevTbl, cols, idxs));

                            prevSchema = owner;
                            prevTbl = tbl;
                            cols = new ArrayList<>();
                            pkCols = primaryKeys(pkStmt, owner, tbl);

                            Map.Entry<String, Set<String>> uniqueIdxAsPk = null;

                            if (pkCols.isEmpty()) {
                                uniqueIdxAsPk = uniqueIndexAsPk(uniqueIndexes(uniqueIdxsStmt, owner, tbl));

                                if (uniqueIdxAsPk != null)
                                    pkCols.addAll(uniqueIdxAsPk.getValue());
                            }

                            idxs = indexes(idxStmt, owner, tbl, uniqueIdxAsPk != null ? uniqueIdxAsPk.getKey() : null);
                        }

                        String colName = colsRs.getString(COL_NAME_IDX);

                        cols.add(new DbColumn(colName, decodeType(colsRs), pkCols.contains(colName),
                            !"N".equals(colsRs.getString(NULLABLE_IDX)), false));
                    }

                    if (!cols.isEmpty())
                        tbls.add(table(prevSchema, prevTbl, cols, idxs));
                }
            }
        }

        return tbls;
    }
}
