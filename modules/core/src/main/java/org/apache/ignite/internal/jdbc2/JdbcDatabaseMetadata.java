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

package org.apache.ignite.internal.jdbc2;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcColumnMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcIndexMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcMetadataInfo;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcPrimaryKeyMeta;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcTableMeta;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
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
public class JdbcDatabaseMetadata implements DatabaseMetaData {
    /** Driver name. */
    public static final String DRIVER_NAME = "Apache Ignite JDBC Driver";

    /** Connection. */
    private final JdbcConnection conn;

    private final JdbcMetadataInfo meta;

    /**
     * @param conn Connection.
     */
    JdbcDatabaseMetadata(JdbcConnection conn) {
        this.conn = conn;

        meta = new JdbcMetadataInfo(conn.ignite().context());
    }

    /** {@inheritDoc} */
    @Override public boolean allProceduresAreCallable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean allTablesAreSelectable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getURL() {
        return conn.url();
    }

    /** {@inheritDoc} */
    @Override public String getUserName() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean isReadOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedHigh() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedLow() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedAtStart() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean nullsAreSortedAtEnd() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getDatabaseProductName() {
        return "Apache Ignite";
    }

    /** {@inheritDoc} */
    @Override public String getDatabaseProductVersion() {
        return IgniteVersionUtils.VER.toString();
    }

    /** {@inheritDoc} */
    @Override public int getDatabaseMajorVersion() {
        return IgniteVersionUtils.VER.major();
    }

    /** {@inheritDoc} */
    @Override public int getDatabaseMinorVersion() {
        return IgniteVersionUtils.VER.minor();
    }

    /** {@inheritDoc} */
    @Override public String getDriverName() {
        return DRIVER_NAME;
    }

    /** {@inheritDoc} */
    @Override public String getDriverVersion() {
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
    @Override public boolean usesLocalFiles() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean usesLocalFilePerTable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesUpperCaseIdentifiers() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesUpperCaseQuotedIdentifiers() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getIdentifierQuoteString() {
        return "\"";
    }

    /** {@inheritDoc} */
    @Override public String getSQLKeywords() {
        return "LIMIT,MINUS,ROWNUM,SYSDATE,SYSTIME,SYSTIMESTAMP,TODAY";
    }

    /** {@inheritDoc} */
    @Override public String getNumericFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getStringFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getSystemFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getTimeDateFunctions() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getSearchStringEscape() {
        return "\\";
    }

    /** {@inheritDoc} */
    @Override public String getExtraNameCharacters() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAlterTableWithAddColumn() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAlterTableWithDropColumn() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsColumnAliasing() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean nullPlusNonNullIsNull() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsConvert() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsConvert(int fromType, int toType) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTableCorrelationNames() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsDifferentTableCorrelationNames() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOrderByUnrelated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGroupBy() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGroupByUnrelated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsLikeEscapeClause() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMultipleResultSets() {
        return conn.isMultipleStatementsAllowed();
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMultipleTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsNonNullableColumns() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMinimumSQLGrammar() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCoreSQLGrammar() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsANSI92FullSQL() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOuterJoins() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsFullOuterJoins() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsLimitedOuterJoins() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getSchemaTerm() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getProcedureTerm() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String getCatalogTerm() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean isCatalogAtStart() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getCatalogSeparator() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsPositionedDelete() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsPositionedUpdate() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSelectForUpdate() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsStoredProcedures() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInExists() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInIns() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsUnion() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsUnionAll() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getMaxBinaryLiteralLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCharLiteralLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInGroupBy() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInIndex() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInOrderBy() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInSelect() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxColumnsInTable() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxConnections() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCursorNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxIndexLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxSchemaNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxProcedureNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCatalogNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxRowSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getMaxStatementLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxStatements() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxTableNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxTablesInSelect() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getMaxUserNameLength() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getDefaultTransactionIsolation() {
        return TRANSACTION_NONE;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsTransactionIsolationLevel(int level) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getProcedures(String catalog, String schemaPtrn,
        String procedureNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
            asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), String.class.getName()),
            Collections.<List<?>>emptyList(), true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getProcedureColumns(String catalog, String schemaPtrn, String procedureNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION",
                "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
            asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), Integer.class.getName(), String.class.getName(),
                Integer.class.getName(), Integer.class.getName(), Short.class.getName(), Short.class.getName(),
                Short.class.getName(), String.class.getName(), String.class.getName(), Integer.class.getName(),
                Integer.class.getName(), Integer.class.getName(), Integer.class.getName(), String.class.getName(),
                String.class.getName()),
            Collections.<List<?>>emptyList(), true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTables(String catalog, String schemaPtrn, String tblNamePtrn,
        String[] tblTypes) throws SQLException {
        conn.ensureNotClosed();

        List<List<?>> rows = Collections.emptyList();

        boolean areTypesValid = false;

        if (tblTypes == null)
            areTypesValid = true;
        else {
            for (String type : tblTypes) {
                if (TYPE_TABLE.equals(type) || TYPE_VIEW.equals(type)) {
                    areTypesValid = true;

                    break;
                }
            }
        }

        if (isValidCatalog(catalog) && areTypesValid) {
            List<JdbcTableMeta> tabMetas = meta.getTablesMeta(schemaPtrn, tblNamePtrn, tblTypes);

            rows = new ArrayList<>(tabMetas.size());

            for (JdbcTableMeta m : tabMetas)
                rows.add(tableRow(m));
        }

        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
            asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), String.class.getName(), String.class.getName()),
            rows, true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, "%");
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCatalogs() throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            singletonList("TABLE_CAT"),
            singletonList(String.class.getName()),
            singletonList(singletonList(CATALOG_NAME)),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTableTypes() throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            singletonList("TABLE_TYPE"),
            singletonList(String.class.getName()),
            asList(singletonList(TYPE_TABLE), singletonList(TYPE_VIEW) ),
            true);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumns(String catalog, String schemaPtrn, String tblNamePtrn,
        String colNamePtrn) throws SQLException {
        conn.ensureNotClosed();

        List<List<?>> rows = Collections.emptyList();

        // FIXME: IGNITE-10745
        int cnt = 0;

        if (isValidCatalog(catalog)) {
            Collection<JdbcColumnMeta> colMetas =
                meta.getColumnsMeta(null /* latest */, schemaPtrn, tblNamePtrn, colNamePtrn);

            rows = new ArrayList<>(colMetas.size());

            for (JdbcColumnMeta col : colMetas)
                rows.add(columnRow(col, ++cnt));
        }

        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            asList(
                "TABLE_CAT",        // 1
                "TABLE_SCHEM",      // 2
                "TABLE_NAME",       // 3
                "COLUMN_NAME",      // 4
                "DATA_TYPE",        // 5
                "TYPE_NAME",        // 6
                "COLUMN_SIZE",      // 7
                "BUFFER_LENGTH",    // 8
                "DECIMAL_DIGITS",   // 9
                "NUM_PREC_RADIX",   // 10
                "NULLABLE",         // 11
                "REMARKS",          // 12
                "COLUMN_DEF",       // 13
                "SQL_DATA_TYPE",    // 14
                "SQL_DATETIME_SUB", // 15
                "CHAR_OCTET_LENGTH", // 16
                "ORDINAL_POSITION",  // 17
                "IS_NULLABLE",      // 18
                "SCOPE_CATLOG",     // 19
                "SCOPE_SCHEMA",     // 20
                "SCOPE_TABLE",      // 21
                "SOURCE_DATA_TYPE", // 22
                "IS_AUTOINCREMENT", // 23
                "IS_GENERATEDCOLUMN"), // 23
            asList(
                String.class.getName(),     // 1
                String.class.getName(),     // 2
                String.class.getName(),     // 3
                String.class.getName(),     // 4
                Integer.class.getName(),    // 5
                String.class.getName(),     // 6
                Integer.class.getName(),    // 7
                Integer.class.getName(),    // 8
                Integer.class.getName(),    // 9
                Integer.class.getName(),    // 10
                Integer.class.getName(),    // 11
                String.class.getName(),     // 12
                String.class.getName(),     // 13
                Integer.class.getName(),    // 14
                Integer.class.getName(),    // 15
                Integer.class.getName(),    // 16
                Integer.class.getName(),    // 17
                String.class.getName(),     // 18
                String.class.getName(),     // 19
                String.class.getName(),     // 20
                String.class.getName(),     // 21
                Short.class.getName(),      // 22
                String.class.getName(),     // 23
                String.class.getName()),    // 24
            rows, true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumnPrivileges(String catalog, String schema, String tbl,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTablePrivileges(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getBestRowIdentifier(String catalog, String schema, String tbl, int scope,
        boolean nullable) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getVersionColumns(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPrimaryKeys(String catalog, String schemaPtrn, String tblNamePtrn)
        throws SQLException {
        conn.ensureNotClosed();

        List<List<?>> rows;

        if (isValidCatalog(catalog)) {
            Collection<JdbcPrimaryKeyMeta> tabsKeyInfo = meta.getPrimaryKeys(schemaPtrn, tblNamePtrn);

            rows = new ArrayList<>();

            for (JdbcPrimaryKeyMeta keyInfo : tabsKeyInfo)
                rows.addAll(primaryKeyRows(keyInfo));

        }
        else
            rows = Collections.emptyList();

        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
            asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), String.class.getName()),
            rows, true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getImportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getExportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTbl,
        String foreignCatalog, String foreignSchema, String foreignTbl) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTypeInfo() throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getIndexInfo(String catalog, String schema, String tbl, boolean unique,
        boolean approximate) throws SQLException {
        conn.ensureNotClosed();

        List<List<?>> rows = Collections.emptyList();

        if (isValidCatalog(catalog)) {
            // Currently we are treating schema and tbl as sql patterns.
            SortedSet<JdbcIndexMeta> idxMetas = meta.getIndexesMeta(schema, tbl);

            rows = new ArrayList<>(idxMetas.size());

            for (JdbcIndexMeta idxMeta : idxMetas)
                rows.addAll(indexRows(idxMeta));
        }

        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
                "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY",
                "PAGES", "FILTER_CONDITION"),
            asList(String.class.getName(), String.class.getName(), String.class.getName(),
                Boolean.class.getName(), String.class.getName(), String.class.getName(), Short.class.getName(),
                Short.class.getName(), String.class.getName(), String.class.getName(), Integer.class.getName(),
                Integer.class.getName(), String.class.getName()),
            rows, true
        );
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetType(int type) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return concurrency == CONCUR_READ_ONLY;
    }

    /** {@inheritDoc} */
    @Override public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean updatesAreDetected(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean deletesAreDetected(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean insertsAreDetected(int type) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsBatchUpdates() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getUDTs(String catalog, String schemaPtrn, String typeNamePtrn,
        int[] types) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection() {
        return conn;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsSavepoints() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsNamedParameters() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsMultipleOpenResults() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsGetGeneratedKeys() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSuperTypes(String catalog, String schemaPtrn,
        String typeNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSuperTables(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getAttributes(String catalog, String schemaPtrn, String typeNamePtrn,
        String attributeNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public boolean supportsResultSetHoldability(int holdability) {
        return holdability == HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public int getResultSetHoldability() {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public int getJDBCMajorVersion() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public int getJDBCMinorVersion() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public int getSQLStateType() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean locatorsUpdateCopy() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsStatementPooling() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public RowIdLifetime getRowIdLifetime() {
        return ROWID_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas(String catalog, String schemaPtrn) throws SQLException {
        conn.ensureNotClosed();

        List<List<?>> rows = Collections.emptyList();

        if (isValidCatalog(catalog)) {
            Set<String> schemas = meta.getSchemasMeta(schemaPtrn);

            rows = new ArrayList<>(schemas.size());

            for (String schema : schemas) {
                if (matches(schema, schemaPtrn))
                    rows.add(Arrays.<Object>asList(schema, CATALOG_NAME));
            }
        }

        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            asList("TABLE_SCHEM", "TABLE_CATALOG"),
            asList(String.class.getName(), String.class.getName()),
            rows, true
        );
    }

    /** {@inheritDoc} */
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getClientInfoProperties() throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctions(String catalog, String schemaPtrn,
        String functionNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
                "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"),
            asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), String.class.getName()),
            Collections.<List<?>>emptyList(), true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctionColumns(String catalog, String schemaPtrn, String functionNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
                "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION",
                "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
            asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), Integer.class.getName(), String.class.getName(),
                Integer.class.getName(), Integer.class.getName(), Short.class.getName(), Short.class.getName(),
                Short.class.getName(), String.class.getName(), Integer.class.getName(), Integer.class.getName(),
                String.class.getName(), String.class.getName()),
            Collections.<List<?>>emptyList(), true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPseudoColumns(String catalog, String schemaPtrn, String tblNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(true, null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Database meta data is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) {
        return iface != null && iface == DatabaseMetaData.class;
    }

    /**
     * Checks whether string matches SQL pattern.
     *
     * @param str String.
     * @param ptrn Pattern.
     * @return Whether string matches pattern.
     */
    private static boolean matches(String str, String ptrn) {
        return str != null && (ptrn == null ||
            str.matches(ptrn.replace("%", ".*").replace("_", ".")));
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

    /**
     * This class is held only for compatibility purposes and shouldn't be used;
     *
     * @deprecated Use {@link JdbcMetadataInfo} instead.
     */
    private static class UpdateMetadataTask implements IgniteCallable<Collection<GridCacheSqlMetadata>> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Cache name. */
        private final String cacheName;

        /**
         * @param cacheName Cache name.
         * @param ignite Ignite.
         */
        public UpdateMetadataTask(String cacheName, Ignite ignite) {
            this.cacheName = cacheName;
            this.ignite = ignite;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Collection<GridCacheSqlMetadata> call() throws Exception {
            IgniteCache cache = ignite.cache(cacheName);

            return ((IgniteCacheProxy)cache).context().queries().sqlMetadataV2();
        }
    }

    /**
     * This class is held only for compatibility purposes and shouldn't be used;
     *
     * Column info.
     *
     * @deprecated Use {@link JdbcMetadataInfo} instead.
     */
    private static class ColumnInfo {
        /** Class name. */
        private final String typeName;

        /** Not null flag. */
        private final boolean notNull;

        /**
         * @param typeName Type name.
         * @param notNull Not null flag.
         */
        private ColumnInfo(String typeName, boolean notNull) {
            this.typeName = typeName;
            this.notNull = notNull;
        }

        /**
         * @return Type name.
         */
        public String typeName() {
            return typeName;
        }

        /**
         * @return Not null flag.
         */
        public boolean isNotNull() {
            return notNull;
        }
    }
}
