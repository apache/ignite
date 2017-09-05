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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;

/**
 * JDBC database metadata implementation.
 */
public class JdbcDatabaseMetadata implements DatabaseMetaData {
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
        return "Ignite JDBC Driver";
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
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean supportsColumnAliasing() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean nullPlusNonNullIsNull() {
        return false;
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
        return false;
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
        return false;
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
        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), String.class.getName()),
            Collections.<List<?>>emptyList(), true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getProcedureColumns(String catalog, String schemaPtrn, String procedureNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION",
                "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
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
        updateMetaData();

        List<List<?>> rows = new LinkedList<>();

        if (validCatalogPattern(catalog) && (tblTypes == null || Arrays.asList(tblTypes).contains("TABLE"))) {
            for (Map.Entry<String, Map<String, Map<String, String>>> schema : meta.entrySet()) {
                if (matches(schema.getKey(), schemaPtrn)) {
                    for (String tbl : schema.getValue().keySet()) {
                        if (matches(tbl, tblNamePtrn))
                            rows.add(tableRow(schema.getKey(), tbl));
                    }
                }
            }
        }

        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), String.class.getName(), String.class.getName()),
            rows, true
        );
    }

    /**
     * @param schema Schema name.
     * @param tbl Table name.
     * @return Table metadata row.
     */
    private List<Object> tableRow(String schema, String tbl) {
        List<Object> row = new ArrayList<>(10);

        row.add(null);
        row.add(schema);
        row.add(tbl.toUpperCase());
        row.add("TABLE");
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);

        return row;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, "%");
    }

    /** {@inheritDoc} */
    @Override public ResultSet getCatalogs() throws SQLException {
        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.singletonList("TABLE_CAT"),
            Collections.singletonList(String.class.getName()),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getTableTypes() throws SQLException {
        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.singletonList("TABLE_TYPE"),
            Collections.singletonList(String.class.getName()),
            Collections.<List<?>>singletonList(Collections.singletonList("TABLE")),
            true);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumns(String catalog, String schemaPtrn, String tblNamePtrn,
        String colNamePtrn) throws SQLException {
        updateMetaData();

        List<List<?>> rows = new LinkedList<>();

        int cnt = 0;

        if (validCatalogPattern(catalog)) {
            for (Map.Entry<String, Map<String, Map<String, String>>> schema : meta.entrySet()) {
                if (matches(schema.getKey(), schemaPtrn)) {
                    for (Map.Entry<String, Map<String, String>> tbl : schema.getValue().entrySet()) {
                        if (matches(tbl.getKey(), tblNamePtrn)) {
                            for (Map.Entry<String, String> col : tbl.getValue().entrySet()) {
                                rows.add(columnRow(schema.getKey(), tbl.getKey(), col.getKey(),
                                    JdbcUtils.type(col.getValue()), JdbcUtils.typeName(col.getValue()),
                                    JdbcUtils.nullable(col.getKey(), col.getValue()), ++cnt));
                            }
                        }
                    }
                }
            }
        }

        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                "REMARKS", "COLUMN_DEF", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Integer.class.getName(), String.class.getName(), Integer.class.getName(),
                Integer.class.getName(), Integer.class.getName(), Integer.class.getName(), String.class.getName(),
                String.class.getName(), Integer.class.getName(), Integer.class.getName(), String.class.getName(),
                String.class.getName(), String.class.getName(), String.class.getName(), Short.class.getName(),
                String.class.getName()),
            rows, true
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

        row.add(null);
        row.add(schema);
        row.add(tbl);
        row.add(col);
        row.add(type);
        row.add(typeName);
        row.add(null);
        row.add(null);
        row.add(10);
        row.add(nullable ? columnNullable : columnNoNulls);
        row.add(null);
        row.add(null);
        row.add(Integer.MAX_VALUE);
        row.add(pos);
        row.add("YES");
        row.add(null);
        row.add(null);
        row.add(null);
        row.add(null);
        row.add("NO");

        return row;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getColumnPrivileges(String catalog, String schema, String tbl,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
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
        updateMetaData();

        List<List<?>> rows = new LinkedList<>();

        if (validCatalogPattern(catalog)) {
            for (Map.Entry<String, Map<String, Map<String, String>>> schema : meta.entrySet()) {
                if (matches(schema.getKey(), schemaPtrn)) {
                    for (Map.Entry<String, Map<String, String>> tbl : schema.getValue().entrySet()) {
                        if (matches(tbl.getKey(), tblNamePtrn))
                            rows.add(Arrays.<Object>asList(null, schema.getKey(), tbl.getKey(), "_KEY", 1, "_KEY"));
                    }
                }
            }
        }

        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), String.class.getName()),
            rows, true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getImportedKeys(String catalog, String schema, String tbl) throws SQLException {
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
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
        updateMetaData();

        Collection<List<?>> rows = new ArrayList<>(indexes.size());

        if (validCatalogPattern(catalog)) {
            for (List<Object> idx : indexes) {
                String idxSchema = (String)idx.get(0);
                String idxTbl = (String)idx.get(1);

                if ((schema == null || schema.equals(idxSchema)) && (tbl == null || tbl.equals(idxTbl))) {
                    List<Object> row = new ArrayList<>(13);

                    row.add(null);
                    row.add(idxSchema);
                    row.add(idxTbl);
                    row.add(idx.get(2));
                    row.add(null);
                    row.add(idx.get(3));
                    row.add((int)tableIndexOther);
                    row.add(idx.get(4));
                    row.add(idx.get(5));
                    row.add((Boolean)idx.get(6) ? "D" : "A");
                    row.add(0);
                    row.add(0);
                    row.add(null);

                    rows.add(row);
                }
            }
        }

        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
                "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY",
                "PAGES", "FILTER_CONDITION"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
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
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
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
        updateMetaData();

        List<List<?>> rows = new ArrayList<>(meta.size());

        if (validCatalogPattern(catalog)) {
            for (String schema : meta.keySet()) {
                if (matches(schema, schemaPtrn))
                    rows.add(Arrays.<Object>asList(schema, null));
            }
        }

        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"),
            Arrays.asList(String.class.getName(), String.class.getName()),
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
        return new JdbcResultSet(null,
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
        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
                "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
                String.class.getName(), Short.class.getName(), String.class.getName()),
            Collections.<List<?>>emptyList(), true
        );
    }

    /** {@inheritDoc} */
    @Override public ResultSet getFunctionColumns(String catalog, String schemaPtrn, String functionNamePtrn,
        String colNamePtrn) throws SQLException {
        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME",
                "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION",
                "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
            Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName(),
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
        return new JdbcResultSet(null,
            conn.createStatement0(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            Collections.<List<?>>emptyList(),
            true
        );
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
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
     * Updates meta data.
     *
     * @throws SQLException In case of error.
     */
    @SuppressWarnings("unchecked")
    private void updateMetaData() throws SQLException {
        if (conn.isClosed())
            throw new SQLException("Connection is closed.");

        try {
            Ignite ignite = conn.ignite();

            UUID nodeId = conn.nodeId();

            Collection<GridCacheSqlMetadata> metas;

            UpdateMetadataTask task = new UpdateMetadataTask(conn.cacheName(), nodeId == null ? ignite : null);

            metas = nodeId == null ? task.call() : ignite.compute(ignite.cluster().forNodeId(nodeId)).call(task);

            meta = U.newHashMap(metas.size());

            indexes = new ArrayList<>();

            for (GridCacheSqlMetadata m : metas) {
                String name = m.cacheName();

                if (name == null)
                    name = "PUBLIC";

                Collection<String> types = m.types();

                Map<String, Map<String, String>> typesMap = U.newHashMap(types.size());

                for (String type : types) {
                    typesMap.put(type.toUpperCase(), m.fields(type));

                    for (GridCacheSqlIndexMetadata idx : m.indexes(type)) {
                        int cnt = 0;

                        for (String field : idx.fields()) {
                            indexes.add(F.<Object>asList(name, type.toUpperCase(), !idx.unique(),
                                idx.name(), ++cnt, field, idx.descending(field)));
                        }
                    }
                }

                meta.put(name, typesMap);
            }
        }
        catch (Exception e) {
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
    private static boolean matches(String str, String ptrn) {
        return str != null && (ptrn == null ||
            str.matches(ptrn.replace("%", ".*").replace("_", ".")));
    }

    /**
     * Checks whether pattern matches any catalog.
     *
     * @param catalog Catalog pattern.
     * @return {@code true} If patter is valid for Ignite (null, empty, or '%' wildcard).
     *  Otherwise returns {@code false}.
     */
    private static boolean validCatalogPattern(String catalog) {
        return F.isEmpty(catalog) || "%".equals(catalog);
    }

    /**
     *
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

            return ((IgniteCacheProxy)cache).context().queries().sqlMetadata();
        }
    }
}
