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

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;

/**
 * JDBC database metadata implementation.
 *
 * See documentation of {@link org.apache.ignite.IgniteJdbcThinDriver} for details.
 */
@SuppressWarnings("RedundantCast")
public class JdbcDatabaseMetadata implements DatabaseMetaData {
    /** Connection. */
    private final JdbcConnection conn;

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
        return "";
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
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getProcedureColumns(String catalog, String schemaPtrn, String procedureNamePtrn,
        String colNamePtrn) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTables(String catalog, String schemaPtrn, String tblNamePtrn,
        String[] tblTypes) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, "%");
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCatalogs() throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTableTypes() throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumns(String catalog, String schemaPtrn, String tblNamePtrn,
        String colNamePtrn) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumnPrivileges(String catalog, String schema, String tbl,
        String colNamePtrn) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTablePrivileges(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getBestRowIdentifier(String catalog, String schema, String tbl, int scope,
        boolean nullable) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getVersionColumns(String catalog, String schema, String tbl) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getPrimaryKeys(String catalog, String schema, String tbl) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getImportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getExportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTbl,
        String foreignCatalog, String foreignSchema, String foreignTbl) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getIndexInfo(String catalog, String schema, String tbl, boolean unique,
        boolean approximate) throws SQLException {
        return null;
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
        return null;
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
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSuperTables(String catalog, String schemaPtrn,
        String tblNamePtrn) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getAttributes(String catalog, String schemaPtrn, String typeNamePtrn,
        String attributeNamePtrn) throws SQLException {
        return null;
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
        return null;
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
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctions(String catalog, String schemaPtrn,
        String functionNamePtrn) throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctionColumns(String catalog, String schemaPtrn, String functionNamePtrn,
        String colNamePtrn) throws SQLException {
        return null;
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
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}