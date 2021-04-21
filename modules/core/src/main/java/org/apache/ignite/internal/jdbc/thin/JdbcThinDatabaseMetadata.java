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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.jdbc2.JdbcUtils;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcColumnMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcIndexMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaColumnsRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaColumnsResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaIndexesRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaIndexesResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaPrimaryKeysRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaPrimaryKeysResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaSchemasRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaSchemasResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaTablesRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetaTablesResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcPrimaryKeyMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcTableMeta;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.jdbc2.JdbcUtils.CATALOG_NAME;
import static org.apache.ignite.internal.jdbc2.JdbcUtils.TYPE_TABLE;
import static org.apache.ignite.internal.jdbc2.JdbcUtils.TYPE_VIEW;
import static org.apache.ignite.internal.jdbc2.JdbcUtils.columnRow;
import static org.apache.ignite.internal.jdbc2.JdbcUtils.indexRows;
import static org.apache.ignite.internal.jdbc2.JdbcUtils.primaryKeyRows;
import static org.apache.ignite.internal.jdbc2.JdbcUtils.tableRow;

/**
 * JDBC database metadata implementation.
 */
public class JdbcThinDatabaseMetadata implements DatabaseMetaData {
    /** Driver name. */
    public static final String DRIVER_NAME = "Apache Ignite Thin JDBC Driver";

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
        return false;
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
        return "Apache Ignite";
    }

    /** {@inheritDoc} */
    @Override public String getDatabaseProductVersion() throws SQLException {
        return conn.igniteVersion().toString();
    }

    /** {@inheritDoc} */
    @Override public String getDriverName() throws SQLException {
        return DRIVER_NAME;
    }

    /** {@inheritDoc} */
    @Override public String getDriverVersion() throws SQLException {
        return IgniteVersionUtils.VER.toString();
    }

    /** {@inheritDoc} */
    @Override public int getDriverMajorVersion() {
        return IgniteVersionUtils.VER.major();
    }

    /** {@inheritDoc} */
    @Override public int getDriverMinorVersion() {
        return IgniteVersionUtils.VER.minor();
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
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    /** {@inheritDoc} */
    @Override public String getSQLKeywords() throws SQLException {
        return "LIMIT,MINUS,ROWNUM,SYSDATE,SYSTIME,SYSTIMESTAMP,TODAY";
    }

    /** {@inheritDoc} */
    @Override public String getNumericFunctions() throws SQLException {
        // TODO: IGNITE-6028
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getStringFunctions() throws SQLException {
        // TODO: IGNITE-6028
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getSystemFunctions() throws SQLException {
        // TODO: IGNITE-6028
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getTimeDateFunctions() throws SQLException {
        // TODO: IGNITE-6028
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    /** {@inheritDoc} */
    @Override public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsConvert() throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return true;
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
        return true;
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
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
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
        return false;
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
        return true;
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
        return conn.igniteVersion().greaterThanEqual(2, 5, 0) ? TRANSACTION_REPEATABLE_READ :
            TRANSACTION_NONE;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTransactions() throws SQLException {
        return conn.igniteVersion().greaterThanEqual(2, 5, 0);
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return conn.igniteVersion().greaterThanEqual(2, 5, 0) &&
            TRANSACTION_REPEATABLE_READ == level;
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
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
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
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
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
    @Override public ResultSet getTables(String catalog, String schemaPtrn, String tblNamePtrn, String[] tblTypes)
        throws SQLException {
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "TABLE_TYPE", String.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "TYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "SELF_REFERENCING_COL_NAME", String.class),
            new JdbcColumnMeta(null, null, "REF_GENERATION", String.class));

        boolean tblTypeMatch = false;

        if (tblTypes == null)
            tblTypeMatch = true;
        else {
            for (String type : tblTypes) {
                if (TYPE_TABLE.equals(type) || TYPE_VIEW.equals(type)) {
                    tblTypeMatch = true;

                    break;
                }
            }
        }

        if (!isValidCatalog(catalog) || !tblTypeMatch)
            return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), meta);

        JdbcMetaTablesResult res = conn.sendRequest(new JdbcMetaTablesRequest(schemaPtrn, tblNamePtrn, tblTypes))
            .response();

        List<List<Object>> rows = new LinkedList<>();

        for (JdbcTableMeta tblMeta : res.meta())
            rows.add(tableRow(tblMeta));

        return new JdbcThinResultSet(rows, meta);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, "%");
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Override public ResultSet getCatalogs() throws SQLException {
        return new JdbcThinResultSet(singletonList(singletonList(CATALOG_NAME)),
            asList(new JdbcColumnMeta(null, null, "TABLE_CAT", String.class)));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Override public ResultSet getTableTypes() throws SQLException {
        return new JdbcThinResultSet(
            asList(singletonList(TYPE_TABLE), singletonList(TYPE_VIEW) ),
            asList(new JdbcColumnMeta(null, null, "TABLE_TYPE", String.class)));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumns(String catalog, String schemaPtrn, String tblNamePtrn, String colNamePtrn)
        throws SQLException {
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),      // 1
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),    // 2
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),     // 3
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),    // 4
            new JdbcColumnMeta(null, null, "DATA_TYPE", Short.class),       // 5
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),      // 6
            new JdbcColumnMeta(null, null, "COLUMN_SIZE", Integer.class),   // 7
            new JdbcColumnMeta(null, null, "BUFFER_LENGTH", Integer.class), // 8
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Integer.class), // 9
            new JdbcColumnMeta(null, null, "NUM_PREC_RADIX", Short.class),  // 10
            new JdbcColumnMeta(null, null, "NULLABLE", Short.class),        // 11
            new JdbcColumnMeta(null, null, "REMARKS", String.class),        // 12
            new JdbcColumnMeta(null, null, "COLUMN_DEF", String.class),     // 13
            new JdbcColumnMeta(null, null, "SQL_DATA_TYPE", Integer.class), // 14
            new JdbcColumnMeta(null, null, "SQL_DATETIME_SUB", Integer.class), // 15
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class), // 16
            new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Integer.class), // 17
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class),    // 18
            new JdbcColumnMeta(null, null, "SCOPE_CATLOG", String.class),   // 19
            new JdbcColumnMeta(null, null, "SCOPE_SCHEMA", String.class),   // 20
            new JdbcColumnMeta(null, null, "SCOPE_TABLE", String.class),    // 21
            new JdbcColumnMeta(null, null, "SOURCE_DATA_TYPE", Short.class), // 22
            new JdbcColumnMeta(null, null, "IS_AUTOINCREMENT", String.class), // 23
            new JdbcColumnMeta(null, null, "IS_GENERATEDCOLUMN", String.class) // 24
        );

        if (!isValidCatalog(catalog))
            return new JdbcThinResultSet(Collections.emptyList(), meta);

        JdbcMetaColumnsResult res = conn.sendRequest(new JdbcMetaColumnsRequest(schemaPtrn, tblNamePtrn, colNamePtrn)).response();

        List<List<Object>> rows = new LinkedList<>();

        for (int i = 0; i < res.meta().size(); ++i)
            rows.add(columnRow(res.meta().get(i), i + 1));

        return new JdbcThinResultSet(rows, meta);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumnPrivileges(String catalog, String schema, String tbl,
        String colNamePtrn) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "GRANTOR", String.class),
            new JdbcColumnMeta(null, null, "GRANTEE", String.class),
            new JdbcColumnMeta(null, null, "PRIVILEGE", String.class),
            new JdbcColumnMeta(null, null, "IS_GRANTABLE", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTablePrivileges(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "GRANTOR", String.class),
            new JdbcColumnMeta(null, null, "GRANTEE", String.class),
            new JdbcColumnMeta(null, null, "PRIVILEGE", String.class),
            new JdbcColumnMeta(null, null, "IS_GRANTABLE", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getBestRowIdentifier(String catalog, String schema, String tbl, int scope,
        boolean nullable) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "SCOPE", Short.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_SIZE", Integer.class),
            new JdbcColumnMeta(null, null, "BUFFER_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Short.class),
            new JdbcColumnMeta(null, null, "PSEUDO_COLUMN", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getVersionColumns(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "SCOPE", Short.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_SIZE", Integer.class),
            new JdbcColumnMeta(null, null, "BUFFER_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Short.class),
            new JdbcColumnMeta(null, null, "PSEUDO_COLUMN", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPrimaryKeys(String catalog, String schema, String tbl) throws SQLException {
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "KEY_SEQ", Short.class),
            new JdbcColumnMeta(null, null, "PK_NAME", String.class));

        if (!isValidCatalog(catalog))
            return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), meta);

        JdbcMetaPrimaryKeysResult res = conn.sendRequest(new JdbcMetaPrimaryKeysRequest(schema, tbl)).
            response();

        List<List<Object>> rows = new LinkedList<>();

        for (JdbcPrimaryKeyMeta pkMeta : res.meta())
            rows.addAll(primaryKeyRows(pkMeta));

        return new JdbcThinResultSet(rows, meta);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getImportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "PKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "KEY_SEQ", Short.class),
            new JdbcColumnMeta(null, null, "UPDATE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "DELETE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "FK_NAME", String.class),
            new JdbcColumnMeta(null, null, "PK_NAME", String.class),
            new JdbcColumnMeta(null, null, "DEFERRABILITY", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getExportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "PKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "KEY_SEQ", Short.class),
            new JdbcColumnMeta(null, null, "UPDATE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "DELETE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "FK_NAME", String.class),
            new JdbcColumnMeta(null, null, "PK_NAME", String.class),
            new JdbcColumnMeta(null, null, "DEFERRABILITY", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTbl,
        String foreignCatalog, String foreignSchema, String foreignTbl) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "PKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "PKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "PKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FKTABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "FKCOLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "KEY_SEQ", Short.class),
            new JdbcColumnMeta(null, null, "UPDATE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "DELETE_RULE", Short.class),
            new JdbcColumnMeta(null, null, "FK_NAME", String.class),
            new JdbcColumnMeta(null, null, "PK_NAME", String.class),
            new JdbcColumnMeta(null, null, "DEFERRABILITY", Short.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTypeInfo() throws SQLException {
        List<List<Object>> types = new ArrayList<>(21);

        types.add(Arrays.<Object>asList("BOOLEAN", Types.BOOLEAN, 1, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "BOOLEAN", 0, 0,
            Types.BOOLEAN, 0, 10));

        types.add(Arrays.<Object>asList("TINYINT", Types.TINYINT, 3, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "TINYINT", 0, 0,
            Types.TINYINT, 0, 10));

        types.add(Arrays.<Object>asList("SMALLINT", Types.SMALLINT, 5, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "SMALLINT", 0, 0,
            Types.SMALLINT, 0, 10));

        types.add(Arrays.<Object>asList("INTEGER", Types.INTEGER, 10, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "INTEGER", 0, 0,
            Types.INTEGER, 0, 10));

        types.add(Arrays.<Object>asList("BIGINT", Types.BIGINT, 19, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "BIGINT", 0, 0,
            Types.BIGINT, 0, 10));

        types.add(Arrays.<Object>asList("FLOAT", Types.FLOAT, 17, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "FLOAT", 0, 0,
            Types.FLOAT, 0, 10));

        types.add(Arrays.<Object>asList("REAL", Types.REAL, 7, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "REAL", 0, 0,
            Types.REAL, 0, 10));

        types.add(Arrays.<Object>asList("DOUBLE", Types.DOUBLE, 17, null, null, null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "DOUBLE", 0, 0,
            Types.DOUBLE, 0, 10));

        types.add(Arrays.<Object>asList("NUMERIC", Types.NUMERIC, Integer.MAX_VALUE, null, null, "PRECISION,SCALE",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "NUMERIC", 0, 0,
            Types.NUMERIC, 0, 10));

        types.add(Arrays.<Object>asList("DECIMAL", Types.DECIMAL, Integer.MAX_VALUE, null, null, "PRECISION,SCALE",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "DECIMAL", 0, 0,
            Types.DECIMAL, 0, 10));

        types.add(Arrays.<Object>asList("DATE", Types.DATE, 8, "DATE '", "'", null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "DATE", 0, 0,
            Types.DATE, 0, null));

        types.add(Arrays.<Object>asList("TIME", Types.TIME, 6, "TIME '", "'", null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "TIME", 0, 0,
            Types.TIME, 0, null));

        types.add(Arrays.<Object>asList("TIMESTAMP", Types.TIMESTAMP, 23, "TIMESTAMP '", "'", null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "TIMESTAMP", 0, 10,
            Types.TIMESTAMP, 0, null));

        types.add(Arrays.<Object>asList("CHAR", Types.CHAR, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, true, (short)typeSearchable, false, false, false, "CHAR", 0, 0,
            Types.CHAR, 0, null));

        types.add(Arrays.<Object>asList("VARCHAR", Types.VARCHAR, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, true, (short)typeSearchable, false, false, false, "VARCHAR", 0, 0,
            Types.VARCHAR, 0, null));

        types.add(Arrays.<Object>asList("LONGVARCHAR", Types.LONGVARCHAR, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, true, (short)typeSearchable, false, false, false, "LONGVARCHAR", 0, 0,
            Types.LONGVARCHAR, 0, null));

        types.add(Arrays.<Object>asList("BINARY", Types.BINARY, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "BINARY", 0, 0,
            Types.BINARY, 0, null));

        types.add(Arrays.<Object>asList("VARBINARY", Types.VARBINARY, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "VARBINARY", 0, 0,
            Types.VARBINARY, 0, null));

        types.add(Arrays.<Object>asList("LONGVARBINARY", Types.LONGVARBINARY, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "LONGVARBINARY", 0, 0,
            Types.LONGVARBINARY, 0, null));

        types.add(Arrays.<Object>asList("OTHER", Types.OTHER, Integer.MAX_VALUE, "'", "'", "LENGTH",
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "OTHER", 0, 0,
            Types.OTHER, 0, null));

        types.add(Arrays.<Object>asList("ARRAY", Types.ARRAY, 0, "(", "')", null,
            (short)typeNullable, false, (short)typeSearchable, false, false, false, "ARRAY", 0, 0,
            Types.ARRAY, 0, null));

        return new JdbcThinResultSet(types, asList(
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "PRECISION", Integer.class),
            new JdbcColumnMeta(null, null, "LITERAL_PREFIX", String.class),
            new JdbcColumnMeta(null, null, "LITERAL_SUFFIX", String.class),
            new JdbcColumnMeta(null, null, "CREATE_PARAMS", String.class),
            new JdbcColumnMeta(null, null, "NULLABLE", Short.class),
            new JdbcColumnMeta(null, null, "CASE_SENSITIVE", Boolean.class),
            new JdbcColumnMeta(null, null, "SEARCHABLE", Short.class),
            new JdbcColumnMeta(null, null, "UNSIGNED_ATTRIBUTE", Boolean.class),
            new JdbcColumnMeta(null, null, "FIXED_PREC_SCALE", Boolean.class),
            new JdbcColumnMeta(null, null, "AUTO_INCREMENT", Boolean.class),
            new JdbcColumnMeta(null, null, "LOCAL_TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "MINIMUM_SCALE", Short.class),
            new JdbcColumnMeta(null, null, "MAXIMUM_SCALE", Short.class),
            new JdbcColumnMeta(null, null, "SQL_DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "SQL_DATETIME_SUB", Integer.class),
            new JdbcColumnMeta(null, null, "NUM_PREC_RADIX", Integer.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getIndexInfo(String catalog, String schema, String tbl, boolean unique,
        boolean approximate) throws SQLException {
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
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
            new JdbcColumnMeta(null, null, "FILTER_CONDITION", String.class));

        if (!isValidCatalog(catalog))
            return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), meta);

        JdbcMetaIndexesResult res = conn.sendRequest(new JdbcMetaIndexesRequest(schema, tbl)).response();

        List<List<Object>> rows = new LinkedList<>();

        for (JdbcIndexMeta idxMeta : res.meta())
            rows.addAll(indexRows(idxMeta));

        return new JdbcThinResultSet(rows, meta);
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetType(int type) throws SQLException {
        return type == TYPE_FORWARD_ONLY;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return supportsResultSetType(type) && concurrency == CONCUR_READ_ONLY;
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
        return true;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getUDTs(String catalog, String schemaPtrn, String typeNamePtrn,
        int[] types) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "TYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "CLASS_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "BASE_TYPE", Short.class)
        ));
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
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "TYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "SUPERTYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "SUPERTYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "SUPERTYPE_NAME", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSuperTables(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "SUPERTABLE_NAME", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getAttributes(String catalog, String schemaPtrn, String typeNamePtrn,
        String attributeNamePtrn) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "TYPE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TYPE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "ATTR_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "ATTR_TYPE_NAME", String.class),
            new JdbcColumnMeta(null, null, "ATTR_SIZE", Integer.class),
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Integer.class),
            new JdbcColumnMeta(null, null, "NUM_PREC_RADIX", Integer.class),
            new JdbcColumnMeta(null, null, "NULLABLE", Integer.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "ATTR_DEF", String.class),
            new JdbcColumnMeta(null, null, "SQL_DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "SQL_DATETIME_SUB", Integer.class),
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "ORDINAL_POSITION", Integer.class),
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class),
            new JdbcColumnMeta(null, null, "SCOPE_CATALOG", String.class),
            new JdbcColumnMeta(null, null, "SCOPE_SCHEMA", String.class),
            new JdbcColumnMeta(null, null, "SCOPE_TABLE", String.class),
            new JdbcColumnMeta(null, null, "SOURCE_DATA_TYPE", Short.class)
        ));
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
        return conn.igniteVersion().major();
    }

    /** {@inheritDoc} */
    @Override public int getDatabaseMinorVersion() throws SQLException {
        return conn.igniteVersion().minor();
    }

    /** {@inheritDoc} */
    @Override public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL99;
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
        conn.ensureNotClosed();

        final List<JdbcColumnMeta> meta = asList(
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_CATALOG", String.class)
        );

        if (!isValidCatalog(catalog))
            return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), meta);

        JdbcMetaSchemasResult res = conn.sendRequest(new JdbcMetaSchemasRequest(schemaPtrn)).response();

        List<List<Object>> rows = new LinkedList<>();

        for (String schema : res.schemas()) {
            List<Object> row = new ArrayList<>(2);

            row.add(schema);
            row.add(CATALOG_NAME);

            rows.add(row);
        }

        return new JdbcThinResultSet(rows, meta);
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
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "NAME", String.class),
            new JdbcColumnMeta(null, null, "MAX_LEN", Integer.class),
            new JdbcColumnMeta(null, null, "DEFAULT_VALUE", String.class),
            new JdbcColumnMeta(null, null, "DESCRIPTION", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctions(String catalog, String schemaPtrn,
        String functionNamePtrn) throws SQLException {
        // TODO: IGNITE-6028
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "FUNCTION_CAT", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_NAME", String.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "FUNCTION_TYPE", String.class),
            new JdbcColumnMeta(null, null, "SPECIFIC_NAME", String.class)
        ));
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctionColumns(String catalog, String schemaPtrn, String functionNamePtrn,
        String colNamePtrn) throws SQLException {
        // TODO: IGNITE-6028
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
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
        return iface != null && iface.isAssignableFrom(JdbcThinDatabaseMetadata.class);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPseudoColumns(String catalog, String schemaPtrn, String tblNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcThinResultSet(Collections.<List<Object>>emptyList(), asList(
            new JdbcColumnMeta(null, null, "TABLE_CAT", String.class),
            new JdbcColumnMeta(null, null, "TABLE_SCHEM", String.class),
            new JdbcColumnMeta(null, null, "TABLE_NAME", String.class),
            new JdbcColumnMeta(null, null, "COLUMN_NAME", String.class),
            new JdbcColumnMeta(null, null, "DATA_TYPE", Integer.class),
            new JdbcColumnMeta(null, null, "COLUMN_SIZE", Integer.class),
            new JdbcColumnMeta(null, null, "DECIMAL_DIGITS", Integer.class),
            new JdbcColumnMeta(null, null, "NUM_PREC_RADIX", Integer.class),
            new JdbcColumnMeta(null, null, "COLUMN_USAGE", Integer.class),
            new JdbcColumnMeta(null, null, "REMARKS", String.class),
            new JdbcColumnMeta(null, null, "CHAR_OCTET_LENGTH", Integer.class),
            new JdbcColumnMeta(null, null, "IS_NULLABLE", String.class)
        ));
    }

    /**
     * Checks if specified catalog matches the only possible catalog value. See {@link JdbcUtils#CATALOG_NAME}.
     *
     * @param catalog Catalog name or {@code null}.
     * @return {@code true} If catalog equal ignoring case to {@link JdbcUtils#CATALOG_NAME} or null (which means any catalog).
     *  Otherwise returns {@code false}.
     */
    private static boolean isValidCatalog(String catalog) {
        return catalog == null || catalog.equalsIgnoreCase(CATALOG_NAME);
    }

    /** {@inheritDoc} */
    @Override public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}
