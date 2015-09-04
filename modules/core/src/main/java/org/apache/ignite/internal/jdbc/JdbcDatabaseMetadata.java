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

package org.apache.ignite.internal.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;

/**
 * JDBC database metadata implementation.
 */
@SuppressWarnings("RedundantCast")
public class JdbcDatabaseMetadata implements DatabaseMetaData {
    /** Task name. */
    private static final String TASK_NAME =
        "org.apache.ignite.internal.processors.cache.query.jdbc.GridCacheQueryJdbcMetadataTask";

    /** Connection. */
    private final JdbcConnection conn;

    /** Metadata. */
    private Map<String, Map<String, Map<String, String>>> meta;

    /** Index info. */
    private Collection<List<Object>> indexes;

    /**
     * @param conn Connection.
     */
    JdbcDatabaseMetadata(JdbcConnection conn) {
        this.conn = conn;
    }

    /** {@inheritDoc} */
    @Override public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getURL() throws SQLException {
        return conn.url();
    }

    /** {@inheritDoc} */
    @Override public String getUserName() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean isReadOnly() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedAtEnd() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getDatabaseProductName() throws SQLException {
        return "Ignite Cache";
    }

    /** {@inheritDoc} */
    @Override public String getDatabaseProductVersion() throws SQLException {
        return "4.1.0";
    }

    /** {@inheritDoc} */
    @Override public String getDriverName() throws SQLException {
        return "Ignite JDBC Driver";
    }

    /** {@inheritDoc} */
    @Override public String getDriverVersion() throws SQLException {
        return "1.0";
    }

    /** {@inheritDoc} */
    @Override public int getDriverMajorVersion() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public int getDriverMinorVersion() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesUpperCaseIdentifiers() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getIdentifierQuoteString() throws SQLException {
        return " ";
    }

    /** {@inheritDoc} */
    @Override public String getSQLKeywords() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getNumericFunctions() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getStringFunctions() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getSystemFunctions() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getSearchStringEscape() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsConvert() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getSchemaTerm() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getProcedureTerm() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getCatalogTerm() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getCatalogSeparator() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsUnion() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxConnections() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxRowSize() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxStatements() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getDefaultTransactionIsolation() throws SQLException {
        return TRANSACTION_NONE;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTransactions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getProcedures(String catalog, String schemaPtrn,
        String procedureNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
            Arrays.<String>asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), String.class.getName()),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getProcedureColumns(String catalog, String schemaPtrn, String procedureNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION",
                "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
            Arrays.<String>asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), Integer.class.getName(), String.class.getName(),
                Integer.class.getName(), Integer.class.getName(), Short.class.getName(), Short.class.getName(),
                Short.class.getName(), String.class.getName(), String.class.getName(), Integer.class.getName(),
                Integer.class.getName(), Integer.class.getName(), Integer.class.getName(), String.class.getName(),
                String.class.getName()),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTables(String catalog, String schemaPtrn, String tblNamePtrn,
        String[] tblTypes) throws SQLException {
        updateMetaData();

        List<List<Object>> rows = new LinkedList<>();

        if (tblTypes == null || Arrays.asList(tblTypes).contains("TABLE"))
            for (Map.Entry<String, Map<String, Map<String, String>>> schema : meta.entrySet())
                if (matches(schema.getKey(), schemaPtrn))
                    for (String tbl : schema.getValue().keySet())
                        if (matches(tbl, tblNamePtrn))
                            rows.add(tableRow(schema.getKey(), tbl));

        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
            Arrays.<String>asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), String.class.getName(), String.class.getName()),
            rows
        );
    }

    /**
     * @param schema Schema name.
     * @param tbl Table name.
     * @return Table metadata row.
     */
    private List<Object> tableRow(String schema, String tbl) {
        List<Object> row = new ArrayList<>(10);

        row.add((String)null);
        row.add(schema);
        row.add(tbl.toUpperCase());
        row.add("TABLE");
        row.add((String)null);
        row.add((String)null);
        row.add((String)null);
        row.add((String)null);
        row.add((String)null);
        row.add((String)null);

        return row;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, "%");
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCatalogs() throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_CAT"),
            Arrays.<String>asList(String.class.getName()),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTableTypes() throws SQLException {
        return new JdbcResultSet(conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.singletonList("TABLE_TYPE"),
            Collections.<String>singletonList(String.class.getName()),
            Collections.singletonList(Collections.<Object>singletonList("TABLE")));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumns(String catalog, String schemaPtrn, String tblNamePtrn,
        String colNamePtrn) throws SQLException {
        updateMetaData();

        List<List<Object>> rows = new LinkedList<>();

        int cnt = 0;

        for (Map.Entry<String, Map<String, Map<String, String>>> schema : meta.entrySet())
            if (matches(schema.getKey(), schemaPtrn))
                for (Map.Entry<String, Map<String, String>> tbl : schema.getValue().entrySet())
                    if (matches(tbl.getKey(), tblNamePtrn))
                        for (Map.Entry<String, String> col : tbl.getValue().entrySet())
                            rows.add(columnRow(schema.getKey(), tbl.getKey(), col.getKey(),
                                JdbcUtils.type(col.getValue()), JdbcUtils.typeName(col.getValue()),
                                JdbcUtils.nullable(col.getKey(), col.getValue()), ++cnt));

        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                "REMARKS", "COLUMN_DEF", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT"),
            Arrays.<String>asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Integer.class.getName(), String.class.getName(), Integer.class.getName(),
                Integer.class.getName(), Integer.class.getName(), Integer.class.getName(), String.class.getName(),
                String.class.getName(), Integer.class.getName(), Integer.class.getName(), String.class.getName(),
                String.class.getName(), String.class.getName(), String.class.getName(), Short.class.getName(),
                String.class.getName()),
            rows
        );
    }

    /**
     * @param schema Schema name.
     * @param tbl Table name.
     * @param col Column name.
     * @param type Type.
     * @param typeName Type name.
     * @param nullable Nullable flag.
     * @param pos Ordinal position.
     * @return Column metadata row.
     */
    private List<Object> columnRow(String schema, String tbl, String col, int type, String typeName,
        boolean nullable, int pos) {
        List<Object> row = new ArrayList<>(20);

        row.add((String)null);
        row.add(schema);
        row.add(tbl);
        row.add(col);
        row.add(type);
        row.add(typeName);
        row.add((Integer)null);
        row.add((Integer)null);
        row.add(10);
        row.add(nullable ? columnNullable : columnNoNulls);
        row.add((String)null);
        row.add((String)null);
        row.add(Integer.MAX_VALUE);
        row.add(pos);
        row.add("YES");
        row.add((String)null);
        row.add((String)null);
        row.add((String)null);
        row.add((Short)null);
        row.add("NO");

        return row;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumnPrivileges(String catalog, String schema, String tbl,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTablePrivileges(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getBestRowIdentifier(String catalog, String schema, String tbl, int scope,
        boolean nullable) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getVersionColumns(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPrimaryKeys(String catalog, String schema, String tbl) throws SQLException {
        updateMetaData();

        List<List<Object>> rows = new LinkedList<>();

        for (Map.Entry<String, Map<String, Map<String, String>>> s : meta.entrySet())
            if (schema == null || schema.toUpperCase().equals(s.getKey()))
                for (Map.Entry<String, Map<String, String>> t : s.getValue().entrySet())
                    if (tbl == null || tbl.toUpperCase().equals(t.getKey()))
                        rows.add(Arrays.<Object>asList((String)null, s.getKey().toUpperCase(),
                            t.getKey().toUpperCase(), "_KEY", 1, "_KEY"));

        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), String.class.getName()),
            rows
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getImportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getExportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTbl,
        String foreignCatalog, String foreignSchema, String foreignTbl) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTypeInfo() throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getIndexInfo(String catalog, String schema, String tbl, boolean unique,
        boolean approximate) throws SQLException {
        Collection<List<Object>> rows = new ArrayList<>(indexes.size());

        for (List<Object> idx : indexes) {
            String idxSchema = (String)idx.get(0);
            String idxTbl = (String)idx.get(1);

            if ((schema == null || schema.equals(idxSchema)) && (tbl == null || tbl.equals(idxTbl))) {
                List<Object> row = new ArrayList<>(13);

                row.add((String)null);
                row.add(idxSchema);
                row.add(idxTbl);
                row.add((Boolean)idx.get(2));
                row.add((String)null);
                row.add((String)idx.get(3));
                row.add((int)tableIndexOther);
                row.add((Integer)idx.get(4));
                row.add((String)idx.get(5));
                row.add((Boolean)idx.get(6) ? "D" : "A");
                row.add(0);
                row.add(0);
                row.add((String)null);

                rows.add(row);
            }
        }

        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
                "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY",
                "PAGES", "FILTER_CONDITION"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
                Boolean.class.getName(), String.class.getName(), String.class.getName(), Short.class.getName(),
                Short.class.getName(), String.class.getName(), String.class.getName(), Integer.class.getName(),
                Integer.class.getName(), String.class.getName()),
            rows
        );
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetType(int type) throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return concurrency == CONCUR_READ_ONLY;
    }

    /** {@inheritDoc} */
    @Override public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getUDTs(String catalog, String schemaPtrn, String typeNamePtrn,
        int[] types) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        return conn;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSuperTypes(String catalog, String schemaPtrn,
        String typeNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSuperTables(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getAttributes(String catalog, String schemaPtrn, String typeNamePtrn,
        String attributeNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public int getResultSetHoldability() throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public int getDatabaseMajorVersion() throws SQLException {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getSQLStateType() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public RowIdLifetime getRowIdLifetime() throws SQLException {
        return ROWID_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas(String catalog, String schemaPtrn) throws SQLException {
        updateMetaData();

        List<List<Object>> rows = new ArrayList<>(meta.size());

        for (String schema : meta.keySet())
            if (matches(schema, schemaPtrn))
                rows.add(Arrays.<Object>asList(schema, (String)null));

        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"),
            Arrays.<String>asList(String.class.getName(), String.class.getName()),
            rows
        );
    }

    /** {@inheritDoc} */
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getClientInfoProperties() throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctions(String catalog, String schemaPtrn,
        String functionNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
                "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"),
            Arrays.<String>asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), String.class.getName()),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctionColumns(String catalog, String schemaPtrn, String functionNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
                "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION",
                "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
            Arrays.<String>asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), Integer.class.getName(), String.class.getName(),
                Integer.class.getName(), Integer.class.getName(), Short.class.getName(), Short.class.getName(),
                Short.class.getName(), String.class.getName(), Integer.class.getName(), Integer.class.getName(),
                String.class.getName(), String.class.getName()),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Database meta data is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface == DatabaseMetaData.class;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPseudoColumns(String catalog, String schemaPtrn, String tblNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<Object>>emptyList()
        );
    }

    /** {@inheritDoc} */
    @Override public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    /**
     * Updates meta data.
     *
     * @throws SQLException In case of error.
     */
    private void updateMetaData() throws SQLException {
        if (conn.isClosed())
            throw new SQLException("Connection is closed.");

        try {
            byte[] packet = conn.client().compute().execute(TASK_NAME, conn.cacheName());

            byte status = packet[0];
            byte[] data = new byte[packet.length - 1];

            U.arrayCopy(packet, 1, data, 0, data.length);

            if (status == 1)
                throw JdbcUtils.unmarshalError(data);
            else {
                List<Object> res = JdbcUtils.unmarshal(data);

                meta = (Map<String, Map<String, Map<String, String>>>)res.get(0);
                indexes = (Collection<List<Object>>)res.get(1);
            }
        }
        catch (GridClientException e) {
            throw new SQLException("Failed to get meta data from Ignite.", e);
        }
    }

    /**
     * Checks whether string matches SQL pattern.
     *
     * @param str String.
     * @param ptrn Pattern.
     * @return Whether string matches pattern.
     */
    private boolean matches(String str, String ptrn) {
        return str != null && (ptrn == null ||
            str.toUpperCase().matches(ptrn.toUpperCase().replace("%", ".*").replace("_", ".")));
    }
}