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

package org.apache.ignite.internal.jdbc.thin;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.jdbc2.JdbcUtils;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcColumnMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcIndexMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaColumnsResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaIndexesResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaTablesResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcTableMeta;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;

/**
 * JDBC database metadata implementation.
 */
@SuppressWarnings("RedundantCast")
public class JdbcThinDatabaseMetadata implements DatabaseMetaData {
    /** Connection. */
    private final JdbcThinConnection conn;

    /**
     * @param conn Connection.
     */
    JdbcThinDatabaseMetadata(JdbcThinConnection conn) {
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
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), Arrays.asList(
            new JdbcColumnMeta(null, null, "PROCEDURE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_NAME", String.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_TYPE", String.class),
            new JdbcColumnMeta(null, null, "SPECIFIC_NAME", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getProcedureColumns(String catalog, String schemaPtrn, String procedureNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), Arrays.asList(
            new JdbcColumnMeta(null, null, "PROCEDURE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PROCEDURE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_TYPE", Short.class),
            new JdbcColumnMeta(null, null, "COLUMN_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PRECISION", Integer.class),
            new JdbcColumnMeta(null, null, "LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "SCALE", Short.class),
            new JdbcColumnMeta(null, null, "RADIX", Short.class),
            new JdbcColumnMeta(null, null, "NULLABLE", Short.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_DEF", String.class),
            new JdbcColumnMeta(null, null, "SQL_DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "SQL_DATETIME_SUB", Integer.class),
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Integer.class),
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class),
            new JdbcColumnMeta(null, null, "SPECIFIC_NAME", String.class)
            ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTables(String catalog, String schemaPtrn, String tblNamePtrn,
        String[] tblTypes) throws SQLException {
        try {
            if (conn.isClosed())
                throw new SQLException("Connection is closed.");

            JdbcMetaTablesResult res = conn.cliIo().tablesMeta(catalog, schemaPtrn, tblNamePtrn, tblTypes);

            List<List<Object>> rows = new LinkedList<>();

            for (JdbcTableMeta tblMeta : res.meta())
                rows.add(tableRow(tblMeta));

            return new JdbcThinResultSet(rows, Arrays.asList(
                new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
                new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
                new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
                new JdbcColumnMeta(null, null, "TABLE_TYPE", String.class),
                new JdbcColumnMeta(null, null, "REMARKS", String.class),
                new JdbcColumnMeta(null, null, "TYPE_CAT", String.class),
                new JdbcColumnMeta(null, null, "TYPE_SCHEM", String.class),
                new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
                new JdbcColumnMeta(null, null, "SELF_REFERENCING_COL_NAME", String.class),
                new JdbcColumnMeta(null, null, "REF_GENERATION", String.class)));
        }
        catch (IOException e) {
            conn.close();

            throw new SQLException("Failed to query Ignite.", e);
        }
        catch (IgniteCheckedException e) {
            throw new SQLException("Failed to query Ignite.", e);
        }
    }

    /**
     * @param tblMeta Table metadata.
     * @return Table metadata row.
     */
    private List<Object> tableRow(JdbcTableMeta tblMeta) {
        List<Object> row = new ArrayList<>(10);

        row.add(upperCase(tblMeta.catalog()));
        row.add(upperCase(tblMeta.schema()));
        row.add(upperCase(tblMeta.table()));
        row.add(upperCase(tblMeta.tableType()));
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);

        return row;
    }

    /**
     * @param str Source string.
     * @return Upper case string.
     */
    private String upperCase(String str) {
        return str == null ? null : str.toUpperCase();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, "%");
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCatalogs() throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), Arrays.asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class)));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTableTypes() throws SQLException {
        return new JdbcThinResultSet(Collections.singletonList(Collections.<Object>singletonList("TABLE")), Arrays.asList(
            new JdbcColumnMeta(null, null, "TABLE_TYPE", String.class)));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumns(String catalog, String schemaPtrn, String tblNamePtrn,
        String colNamePtrn) throws SQLException {
        try {
            if (conn.isClosed())
                throw new SQLException("Connection is closed.");

            JdbcMetaColumnsResult res = conn.cliIo().columnsMeta(schemaPtrn, tblNamePtrn, colNamePtrn);

            List<List<Object>> rows = new LinkedList<>();

            for (int i = 0; i < res.meta().size(); ++i)
                rows.add(columnRow(res.meta().get(i), i + 1));

            return new JdbcThinResultSet(rows, Arrays.asList(
                new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
                new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
                new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
                new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
                new JdbcColumnMeta(null, null, "DATA_TYPE", Short.class),
                new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
                new JdbcColumnMeta(null, null, "COLUMN_SIZE", Integer.class),
                new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Integer.class),
                new JdbcColumnMeta(null, null, "NUM_PREC_RADIX", Short.class),
                new JdbcColumnMeta(null, null, "NULLABLE", Short.class),
                new JdbcColumnMeta(null, null, "REMARKS", String.class),
                new JdbcColumnMeta(null, null, "COLUMN_DEF", String.class),
                new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class),
                new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Integer.class),
                new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class),
                new JdbcColumnMeta(null, null, "SCOPE_CATLOG", String.class),
                new JdbcColumnMeta(null, null, "SCOPE_SCHEMA", String.class),
                new JdbcColumnMeta(null, null, "SCOPE_TABLE", String.class),
                new JdbcColumnMeta(null, null, "SOURCE_DATA_TYPE", Short.class),
                new JdbcColumnMeta(null, null, "IS_AUTOINCREMENT", String.class)));
        }
        catch (IOException e) {
            conn.close();

            throw new SQLException("Failed to query Ignite.", e);
        }
        catch (IgniteCheckedException e) {
            throw new SQLException("Failed to query Ignite.", e);
        }
    }

    /**
     * @param colMeta Column metadata.
     * @param pos Ordinal position.
     * @return Column metadata row.
     */
    private List<Object> columnRow(JdbcColumnMeta colMeta, int pos) {
        List<Object> row = new ArrayList<>(20);

        row.add((String)null);
        row.add(upperCase(colMeta.schemaName()));
        row.add(upperCase(colMeta.tableName()));
        row.add(upperCase(colMeta.columnName()));
        row.add(colMeta.dataType());
        row.add(upperCase(colMeta.dataTypeName()));
        row.add((Integer)null);
        row.add((Integer)null);
        row.add(10);
        row.add(JdbcUtils.nullable(colMeta.columnName(), colMeta.dataTypeClass()) ? 1 : 0 );
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
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTablePrivileges(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getBestRowIdentifier(String catalog, String schema, String tbl, int scope,
        boolean nullable) throws SQLException {
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getVersionColumns(String catalog, String schema, String tbl) throws SQLException {
        return emptyResultSet();
    }

    /**
     * @return Empty result set.
     */
    private ResultSet emptyResultSet() {
        return new JdbcThinResultSet(Collections.singletonList(Collections.emptyList()),
            Collections.<JdbcColumnMeta>emptyList());
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPrimaryKeys(String catalog, String schema, String tbl) throws SQLException {
//        List<List<Object>> rows = new LinkedList<>();
//
//        for (Map.Entry<String, Map<String, Map<String, String>>> s : meta.entrySet())
//            if (schema == null || schema.toUpperCase().equals(s.getKey()))
//                for (Map.Entry<String, Map<String, String>> t : s.getValue().entrySet())
//                    if (tbl == null || tbl.toUpperCase().equals(t.getKey()))
//                        rows.add(Arrays.<Object>asList((String)null, s.getKey().toUpperCase(),
//                            t.getKey().toUpperCase(), "_KEY", 1, "_KEY"));
//
//        return new JdbcThinResultSet(
//            conn.createStatement0(),
//            Collections.<String>emptyList(),
//            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
//            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
//                String.class.getName(), Short.class.getName(), String.class.getName()),
//            rows
//        );

        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getImportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getExportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTbl,
        String foreignCatalog, String foreignSchema, String foreignTbl) throws SQLException {
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTypeInfo() throws SQLException {
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getIndexInfo(String catalog, String schema, String tbl, boolean unique,
        boolean approximate) throws SQLException {
        try {
            if (conn.isClosed())
                throw new SQLException("Connection is closed.");

            JdbcMetaIndexesResult res = conn.cliIo().indexMeta(catalog, schema, tbl, unique, approximate);

            List<List<Object>> rows = new LinkedList<>();

            for (int i = 0; i < res.meta().size(); ++i)
                rows.addAll(indexRow(schema, tbl, res.meta().get(i)));

            return new JdbcThinResultSet(rows, Arrays.asList(
                new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
                new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
                new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
                new JdbcColumnMeta(null, null, "NON_UNIQUE", Boolean.class),
                new JdbcColumnMeta(null, null, "INDEX_QUALIFIER", String.class),
                new JdbcColumnMeta(null, null, "INDEX_NAME", String.class),
                new JdbcColumnMeta(null, null, "TYPE", Short.class),
                new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Short.class),
                new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
                new JdbcColumnMeta(null, null, "ASC_OR_DESC", String.class),
                new JdbcColumnMeta(null, null, "CARDINALITY", Integer.class),
                new JdbcColumnMeta(null, null, "PAGES", Integer.class),
                new JdbcColumnMeta(null, null, "FILTER_CONDITION", String.class)));
        }
        catch (IOException e) {
            conn.close();

            throw new SQLException("Failed to query Ignite.", e);
        }
        catch (IgniteCheckedException e) {
            throw new SQLException("Failed to query Ignite.", e);
        }
    }

    /**
     * @param schema Table schema name.
     * @param tbl Table name.
     * @param idxMeta Index metadata.
     * @return List of result rows correspond to index.
     */
    private List<List<Object>> indexRow(String schema, String tbl, JdbcIndexMeta idxMeta) {
        List<List<Object>> rows = new ArrayList<>(idxMeta.fields().length);

        for (int i = 0; i < idxMeta.fields().length; ++i) {
            List<Object> row = new ArrayList<>(13);

            row.add((String)null); // table catalog
            row.add(upperCase(schema));
            row.add(upperCase(tbl));
            row.add(true); // non unique
            row.add(null); // index qualifier (index catalog)
            row.add(upperCase(idxMeta.name()));
            row.add((int)tableIndexOther); // type
            row.add((Integer)i); // field ordinal position in index
            row.add(upperCase(idxMeta.fields()[i]));
            row.add(idxMeta.fieldsAsc()[i] ? "A" : "D");
            row.add((Integer)0); // cardinality
            row.add((Integer)0); // pages
            row.add((String)null); // filer condition

            rows.add(row);
        }

        return rows;
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
        return emptyResultSet();
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
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSuperTables(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getAttributes(String catalog, String schemaPtrn, String typeNamePtrn,
        String attributeNamePtrn) throws SQLException {
        return emptyResultSet();
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
        // TODO: impl
//        List<List<Object>> rows = new ArrayList<>(meta.size());
//
//        for (String schema : meta.keySet())
//            if (matches(schema, schemaPtrn))
//                rows.add(Arrays.<Object>asList(schema, (String)null));
//
//        return new JdbcThinResultSet(
//            conn.createStatement0(),
//            Collections.<String>emptyList(),
//            Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"),
//            Arrays.<String>asList(String.class.getName(), String.class.getName()),
//            rows
//        );
        return emptyResultSet();
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
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctions(String catalog, String schemaPtrn,
        String functionNamePtrn) throws SQLException {

        // TODO: impl
//        return new JdbcThinResultSet(
//            conn.createStatement0(),
//            Collections.<String>emptyList(),
//            Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
//                "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"),
//            Arrays.<String>asList(String.class.getName(), String.class.getName(), String.class.getName(),
//                String.class.getName(), Short.class.getName(), String.class.getName()),
//            Collections.<List<Object>>emptyList()
//        );

        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctionColumns(String catalog, String schemaPtrn, String functionNamePtrn,
        String colNamePtrn) throws SQLException {

        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), Arrays.asList(
            new JdbcColumnMeta(null, null, "FUNCTION_CAT", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_TYPE", Short.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PRECISION", Integer.class),
            new JdbcColumnMeta(null, null, "LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "SCALE", Short.class),
            new JdbcColumnMeta(null, null, "RADIX", Short.class),
            new JdbcColumnMeta(null, null, "NULLABLE", Short.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Integer.class),
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class),
            new JdbcColumnMeta(null, null, "SPECIFIC_NAME", String.class)
        ));
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
        return emptyResultSet();
    }

    /** {@inheritDoc} */
    @Override public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
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