/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Properties;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.tools.SimpleResultSet;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/**
 * Represents the meta data for a database.
 */
public class JdbcDatabaseMetaData extends TraceObject implements
        DatabaseMetaData, JdbcDatabaseMetaDataBackwardsCompat {

    private final JdbcConnection conn;

    JdbcDatabaseMetaData(JdbcConnection conn, Trace trace, int id) {
        setTrace(trace, TraceObject.DATABASE_META_DATA, id);
        this.conn = conn;
    }

    /**
     * Returns the major version of this driver.
     *
     * @return the major version number
     */
    @Override
    public int getDriverMajorVersion() {
        debugCodeCall("getDriverMajorVersion");
        return Constants.VERSION_MAJOR;
    }

    /**
     * Returns the minor version of this driver.
     *
     * @return the minor version number
     */
    @Override
    public int getDriverMinorVersion() {
        debugCodeCall("getDriverMinorVersion");
        return Constants.VERSION_MINOR;
    }

    /**
     * Gets the database product name.
     *
     * @return the product name ("H2")
     */
    @Override
    public String getDatabaseProductName() {
        debugCodeCall("getDatabaseProductName");
        // This value must stay like that, see
        // http://opensource.atlassian.com/projects/hibernate/browse/HHH-2682
        return "H2";
    }

    /**
     * Gets the product version of the database.
     *
     * @return the product version
     */
    @Override
    public String getDatabaseProductVersion() {
        debugCodeCall("getDatabaseProductVersion");
        return Constants.getFullVersion();
    }

    /**
     * Gets the name of the JDBC driver.
     *
     * @return the driver name ("H2 JDBC Driver")
     */
    @Override
    public String getDriverName() {
        debugCodeCall("getDriverName");
        return "H2 JDBC Driver";
    }

    /**
     * Gets the version number of the driver. The format is
     * [MajorVersion].[MinorVersion].
     *
     * @return the version number
     */
    @Override
    public String getDriverVersion() {
        debugCodeCall("getDriverVersion");
        return Constants.getFullVersion();
    }

    /**
     * Gets the list of tables in the database. The result set is sorted by
     * TABLE_TYPE, TABLE_SCHEM, and TABLE_NAME.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog </li>
     * <li>2 TABLE_SCHEM (String) table schema </li>
     * <li>3 TABLE_NAME (String) table name </li>
     * <li>4 TABLE_TYPE (String) table type </li>
     * <li>5 REMARKS (String) comment </li>
     * <li>6 TYPE_CAT (String) always null </li>
     * <li>7 TYPE_SCHEM (String) always null </li>
     * <li>8 TYPE_NAME (String) always null </li>
     * <li>9 SELF_REFERENCING_COL_NAME (String) always null </li>
     * <li>10 REF_GENERATION (String) always null </li>
     * <li>11 SQL (String) the create table statement or NULL for systems tables
     * </li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @param types null or a list of table types
     * @return the list of columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getTables(String catalogPattern, String schemaPattern,
            String tableNamePattern, String[] types) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTables(" + quote(catalogPattern) + ", " +
                        quote(schemaPattern) + ", " + quote(tableNamePattern) +
                        ", " + quoteArray(types) + ");");
            }
            checkClosed();
            String tableType;
            if (types != null && types.length > 0) {
                StatementBuilder buff = new StatementBuilder("TABLE_TYPE IN(");
                for (String ignored : types) {
                    buff.appendExceptFirst(", ");
                    buff.append('?');
                }
                tableType = buff.append(')').toString();
            } else {
                tableType = "TRUE";
            }

            String tableSelect = "SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "TABLE_TYPE, "
                    + "REMARKS, "
                    + "TYPE_NAME TYPE_CAT, "
                    + "TYPE_NAME TYPE_SCHEM, "
                    + "TYPE_NAME, "
                    + "TYPE_NAME SELF_REFERENCING_COL_NAME, "
                    + "TYPE_NAME REF_GENERATION, "
                    + "SQL "
                    + "FROM INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND TABLE_NAME LIKE ? ESCAPE ? "
                    + "AND (" + tableType + ") ";

            boolean includeSynonyms = types == null || Arrays.asList(types).contains("SYNONYM");
            String synonymSelect = "SELECT "
                    + "SYNONYM_CATALOG TABLE_CAT, "
                    + "SYNONYM_SCHEMA TABLE_SCHEM, "
                    + "SYNONYM_NAME as TABLE_NAME, "
                    + "TYPE_NAME AS TABLE_TYPE, "
                    + "REMARKS, "
                    + "TYPE_NAME TYPE_CAT, "
                    + "TYPE_NAME TYPE_SCHEM, "
                    + "TYPE_NAME AS TYPE_NAME, "
                    + "TYPE_NAME SELF_REFERENCING_COL_NAME, "
                    + "TYPE_NAME REF_GENERATION, "
                    + "NULL AS SQL "
                    + "FROM INFORMATION_SCHEMA.SYNONYMS "
                    + "WHERE SYNONYM_CATALOG LIKE ? ESCAPE ? "
                    + "AND SYNONYM_SCHEMA LIKE ? ESCAPE ? "
                    + "AND SYNONYM_NAME LIKE ? ESCAPE ? "
                    + "AND (" + includeSynonyms + ") ";

            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CAT, "
                    + "TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "TABLE_TYPE, "
                    + "REMARKS, "
                    + "TYPE_CAT, "
                    + "TYPE_SCHEM, "
                    + "TYPE_NAME, "
                    + "SELF_REFERENCING_COL_NAME, "
                    + "REF_GENERATION, "
                    + "SQL "
                    + "FROM (" + synonymSelect  + " UNION " + tableSelect + ") "
                    + "ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(schemaPattern));
            prep.setString(4, "\\");
            prep.setString(5, getPattern(tableNamePattern));
            prep.setString(6, "\\");
            prep.setString(7, getCatalogPattern(catalogPattern));
            prep.setString(8, "\\");
            prep.setString(9, getSchemaPattern(schemaPattern));
            prep.setString(10, "\\");
            prep.setString(11, getPattern(tableNamePattern));
            prep.setString(12, "\\");
            for (int i = 0; types != null && i < types.length; i++) {
                prep.setString(13 + i, types[i]);
            }
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of columns. The result set is sorted by TABLE_SCHEM,
     * TABLE_NAME, and ORDINAL_POSITION.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog </li>
     * <li>2 TABLE_SCHEM (String) table schema </li>
     * <li>3 TABLE_NAME (String) table name </li>
     * <li>4 COLUMN_NAME (String) column name </li>
     * <li>5 DATA_TYPE (short) data type (see java.sql.Types) </li>
     * <li>6 TYPE_NAME (String) data type name ("INTEGER", "VARCHAR",...) </li>
     * <li>7 COLUMN_SIZE (int) precision
     *         (values larger than 2 GB are returned as 2 GB)</li>
     * <li>8 BUFFER_LENGTH (int) unused </li>
     * <li>9 DECIMAL_DIGITS (int) scale (0 for INTEGER and VARCHAR) </li>
     * <li>10 NUM_PREC_RADIX (int) radix (always 10) </li>
     * <li>11 NULLABLE (int) columnNoNulls or columnNullable</li>
     * <li>12 REMARKS (String) comment (always empty) </li>
     * <li>13 COLUMN_DEF (String) default value </li>
     * <li>14 SQL_DATA_TYPE (int) unused </li>
     * <li>15 SQL_DATETIME_SUB (int) unused </li>
     * <li>16 CHAR_OCTET_LENGTH (int) unused </li>
     * <li>17 ORDINAL_POSITION (int) the column index (1,2,...) </li>
     * <li>18 IS_NULLABLE (String) "NO" or "YES" </li>
     * <li>19 SCOPE_CATALOG (String) always null </li>
     * <li>20 SCOPE_SCHEMA (String) always null </li>
     * <li>21 SCOPE_TABLE (String) always null </li>
     * <li>22 SOURCE_DATA_TYPE (short) null </li>
     * <li>23 IS_AUTOINCREMENT (String) "NO" or "YES" </li>
     * <li>24 SCOPE_CATLOG (String) always null (the typo is on purpose,
     * for compatibility with the JDBC specification prior to 4.1)</li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name
     *            (uppercase for unquoted names)
     * @return the list of columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getColumns(String catalogPattern, String schemaPattern,
            String tableNamePattern, String columnNamePattern)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getColumns(" + quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableNamePattern)+", "
                        +quote(columnNamePattern)+");");
            }
            checkClosed();
            String tableSql = "SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "COLUMN_NAME, "
                    + "DATA_TYPE, "
                    + "TYPE_NAME, "
                    + "CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, "
                    + "CHARACTER_MAXIMUM_LENGTH BUFFER_LENGTH, "
                    + "NUMERIC_SCALE DECIMAL_DIGITS, "
                    + "NUMERIC_PRECISION_RADIX NUM_PREC_RADIX, "
                    + "NULLABLE, "
                    + "REMARKS, "
                    + "COLUMN_DEFAULT COLUMN_DEF, "
                    + "DATA_TYPE SQL_DATA_TYPE, "
                    + "ZERO() SQL_DATETIME_SUB, "
                    + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH, "
                    + "ORDINAL_POSITION, "
                    + "IS_NULLABLE IS_NULLABLE, "
                    + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_CATALOG, "
                    + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_SCHEMA, "
                    + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_TABLE, "
                    + "SOURCE_DATA_TYPE, "
                    + "CASE WHEN SEQUENCE_NAME IS NULL THEN "
                    + "CAST(? AS VARCHAR) ELSE CAST(? AS VARCHAR) END IS_AUTOINCREMENT, "
                    + "CAST(SOURCE_DATA_TYPE AS VARCHAR) SCOPE_CATLOG "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND TABLE_NAME LIKE ? ESCAPE ? "
                    + "AND COLUMN_NAME LIKE ? ESCAPE ? "
                    + "ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";
            String synonymSql = "SELECT "
                    + "s.SYNONYM_CATALOG TABLE_CAT, "
                    + "s.SYNONYM_SCHEMA TABLE_SCHEM, "
                    + "s.SYNONYM_NAME TABLE_NAME, "
                    + "c.COLUMN_NAME, "
                    + "c.DATA_TYPE, "
                    + "c.TYPE_NAME, "
                    + "c.CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, "
                    + "c.CHARACTER_MAXIMUM_LENGTH BUFFER_LENGTH, "
                    + "c.NUMERIC_SCALE DECIMAL_DIGITS, "
                    + "c.NUMERIC_PRECISION_RADIX NUM_PREC_RADIX, "
                    + "c.NULLABLE, "
                    + "c.REMARKS, "
                    + "c.COLUMN_DEFAULT COLUMN_DEF, "
                    + "c.DATA_TYPE SQL_DATA_TYPE, "
                    + "ZERO() SQL_DATETIME_SUB, "
                    + "c.CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH, "
                    + "c.ORDINAL_POSITION, "
                    + "c.IS_NULLABLE IS_NULLABLE, "
                    + "CAST(c.SOURCE_DATA_TYPE AS VARCHAR) SCOPE_CATALOG, "
                    + "CAST(c.SOURCE_DATA_TYPE AS VARCHAR) SCOPE_SCHEMA, "
                    + "CAST(c.SOURCE_DATA_TYPE AS VARCHAR) SCOPE_TABLE, "
                    + "c.SOURCE_DATA_TYPE, "
                    + "CASE WHEN c.SEQUENCE_NAME IS NULL THEN "
                    + "CAST(? AS VARCHAR) ELSE CAST(? AS VARCHAR) END IS_AUTOINCREMENT, "
                    + "CAST(c.SOURCE_DATA_TYPE AS VARCHAR) SCOPE_CATLOG "
                    + "FROM INFORMATION_SCHEMA.COLUMNS c JOIN INFORMATION_SCHEMA.SYNONYMS s ON "
                    + "s.SYNONYM_FOR = c.TABLE_NAME "
                    + "AND s.SYNONYM_FOR_SCHEMA = c.TABLE_SCHEMA "
                    + "WHERE s.SYNONYM_CATALOG LIKE ? ESCAPE ? "
                    + "AND s.SYNONYM_SCHEMA LIKE ? ESCAPE ? "
                    + "AND s.SYNONYM_NAME LIKE ? ESCAPE ? "
                    + "AND c.COLUMN_NAME LIKE ? ESCAPE ? ";
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CAT, "
                    + "TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "COLUMN_NAME, "
                    + "DATA_TYPE, "
                    + "TYPE_NAME, "
                    + "COLUMN_SIZE, "
                    + "BUFFER_LENGTH, "
                    + "DECIMAL_DIGITS, "
                    + "NUM_PREC_RADIX, "
                    + "NULLABLE, "
                    + "REMARKS, "
                    + "COLUMN_DEF, "
                    + "SQL_DATA_TYPE, "
                    + "SQL_DATETIME_SUB, "
                    + "CHAR_OCTET_LENGTH, "
                    + "ORDINAL_POSITION, "
                    + "IS_NULLABLE, "
                    + "SCOPE_CATALOG, "
                    + "SCOPE_SCHEMA, "
                    + "SCOPE_TABLE, "
                    + "SOURCE_DATA_TYPE, "
                    + "IS_AUTOINCREMENT, "
                    + "SCOPE_CATLOG "
                    + "FROM ((" + tableSql + ") UNION (" + synonymSql
                    + ")) ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");
            prep.setString(1, "NO");
            prep.setString(2, "YES");
            prep.setString(3, getCatalogPattern(catalogPattern));
            prep.setString(4, "\\");
            prep.setString(5, getSchemaPattern(schemaPattern));
            prep.setString(6, "\\");
            prep.setString(7, getPattern(tableNamePattern));
            prep.setString(8, "\\");
            prep.setString(9, getPattern(columnNamePattern));
            prep.setString(10, "\\");
            prep.setString(11, "NO");
            prep.setString(12, "YES");
            prep.setString(13, getCatalogPattern(catalogPattern));
            prep.setString(14, "\\");
            prep.setString(15, getSchemaPattern(schemaPattern));
            prep.setString(16, "\\");
            prep.setString(17, getPattern(tableNamePattern));
            prep.setString(18, "\\");
            prep.setString(19, getPattern(columnNamePattern));
            prep.setString(20, "\\");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of indexes for this database. The primary key index (if
     * there is one) is also listed, with the name PRIMARY_KEY. The result set
     * is sorted by NON_UNIQUE ('false' first), TYPE, TABLE_SCHEM, INDEX_NAME,
     * and ORDINAL_POSITION.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog </li>
     * <li>2 TABLE_SCHEM (String) table schema </li>
     * <li>3 TABLE_NAME (String) table name </li>
     * <li>4 NON_UNIQUE (boolean) 'true' if non-unique</li>
     * <li>5 INDEX_QUALIFIER (String) index catalog </li>
     * <li>6 INDEX_NAME (String) index name </li>
     * <li>7 TYPE (short) the index type (always tableIndexOther) </li>
     * <li>8 ORDINAL_POSITION (short) column index (1, 2, ...) </li>
     * <li>9 COLUMN_NAME (String) column name </li>
     * <li>10 ASC_OR_DESC (String) ascending or descending (always 'A') </li>
     * <li>11 CARDINALITY (int) numbers of unique values </li>
     * <li>12 PAGES (int) number of pages use (always 0) </li>
     * <li>13 FILTER_CONDITION (String) filter condition (always empty) </li>
     * <li>14 SORT_TYPE (int) the sort type bit map: 1=DESCENDING,
     * 2=NULLS_FIRST, 4=NULLS_LAST </li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableName table name (must be specified)
     * @param unique only unique indexes
     * @param approximate is ignored
     * @return the list of indexes and columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getIndexInfo(String catalogPattern, String schemaPattern,
            String tableName, boolean unique, boolean approximate)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getIndexInfo(" + quote(catalogPattern) + ", " +
                        quote(schemaPattern) + ", " + quote(tableName) + ", " +
                        unique + ", " + approximate + ");");
            }
            String uniqueCondition;
            if (unique) {
                uniqueCondition = "NON_UNIQUE=FALSE";
            } else {
                uniqueCondition = "TRUE";
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "NON_UNIQUE, "
                    + "TABLE_CATALOG INDEX_QUALIFIER, "
                    + "INDEX_NAME, "
                    + "INDEX_TYPE TYPE, "
                    + "ORDINAL_POSITION, "
                    + "COLUMN_NAME, "
                    + "ASC_OR_DESC, "
                    // TODO meta data for number of unique values in an index
                    + "CARDINALITY, "
                    + "PAGES, "
                    + "FILTER_CONDITION, "
                    + "SORT_TYPE "
                    + "FROM INFORMATION_SCHEMA.INDEXES "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND (" + uniqueCondition + ") "
                    + "AND TABLE_NAME = ? "
                    + "ORDER BY NON_UNIQUE, TYPE, TABLE_SCHEM, INDEX_NAME, ORDINAL_POSITION");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(schemaPattern));
            prep.setString(4, "\\");
            prep.setString(5, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the primary key columns for a table. The result set is sorted by
     * TABLE_SCHEM, and COLUMN_NAME (and not by KEY_SEQ).
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog</li>
     * <li>2 TABLE_SCHEM (String) table schema</li>
     * <li>3 TABLE_NAME (String) table name</li>
     * <li>4 COLUMN_NAME (String) column name</li>
     * <li>5 KEY_SEQ (short) the column index of this column (1,2,...)</li>
     * <li>6 PK_NAME (String) the name of the primary key index</li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableName table name (must be specified)
     * @return the list of primary key columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getPrimaryKeys(String catalogPattern,
            String schemaPattern, String tableName) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getPrimaryKeys("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableName)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "COLUMN_NAME, "
                    + "ORDINAL_POSITION KEY_SEQ, "
                    + "IFNULL(CONSTRAINT_NAME, INDEX_NAME) PK_NAME "
                    + "FROM INFORMATION_SCHEMA.INDEXES "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND TABLE_NAME = ? "
                    + "AND PRIMARY_KEY = TRUE "
                    + "ORDER BY COLUMN_NAME");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(schemaPattern));
            prep.setString(4, "\\");
            prep.setString(5, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if all procedures callable.
     *
     * @return true
     */
    @Override
    public boolean allProceduresAreCallable() {
        debugCodeCall("allProceduresAreCallable");
        return true;
    }

    /**
     * Checks if it possible to query all tables returned by getTables.
     *
     * @return true
     */
    @Override
    public boolean allTablesAreSelectable() {
        debugCodeCall("allTablesAreSelectable");
        return true;
    }

    /**
     * Returns the database URL for this connection.
     *
     * @return the url
     */
    @Override
    public String getURL() throws SQLException {
        try {
            debugCodeCall("getURL");
            return conn.getURL();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the user name as passed to DriverManager.getConnection(url, user,
     * password).
     *
     * @return the user name
     */
    @Override
    public String getUserName() throws SQLException {
        try {
            debugCodeCall("getUserName");
            return conn.getUser();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the same as Connection.isReadOnly().
     *
     * @return if read only optimization is switched on
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        try {
            debugCodeCall("isReadOnly");
            return conn.isReadOnly();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if NULL is sorted high (bigger than anything that is not null).
     *
     * @return false by default; true if the system property h2.sortNullsHigh is
     *         set to true
     */
    @Override
    public boolean nullsAreSortedHigh() {
        debugCodeCall("nullsAreSortedHigh");
        return SysProperties.SORT_NULLS_HIGH;
    }

    /**
     * Checks if NULL is sorted low (smaller than anything that is not null).
     *
     * @return true by default; false if the system property h2.sortNullsHigh is
     *         set to true
     */
    @Override
    public boolean nullsAreSortedLow() {
        debugCodeCall("nullsAreSortedLow");
        return !SysProperties.SORT_NULLS_HIGH;
    }

    /**
     * Checks if NULL is sorted at the beginning (no matter if ASC or DESC is
     * used).
     *
     * @return false
     */
    @Override
    public boolean nullsAreSortedAtStart() {
        debugCodeCall("nullsAreSortedAtStart");
        return false;
    }

    /**
     * Checks if NULL is sorted at the end (no matter if ASC or DESC is used).
     *
     * @return false
     */
    @Override
    public boolean nullsAreSortedAtEnd() {
        debugCodeCall("nullsAreSortedAtEnd");
        return false;
    }

    /**
     * Returns the connection that created this object.
     *
     * @return the connection
     */
    @Override
    public Connection getConnection() {
        debugCodeCall("getConnection");
        return conn;
    }

    /**
     * Gets the list of procedures. The result set is sorted by PROCEDURE_SCHEM,
     * PROCEDURE_NAME, and NUM_INPUT_PARAMS. There are potentially multiple
     * procedures with the same name, each with a different number of input
     * parameters.
     *
     * <ul>
     * <li>1 PROCEDURE_CAT (String) catalog </li>
     * <li>2 PROCEDURE_SCHEM (String) schema </li>
     * <li>3 PROCEDURE_NAME (String) name </li>
     * <li>4 NUM_INPUT_PARAMS (int) the number of arguments </li>
     * <li>5 NUM_OUTPUT_PARAMS (int) for future use, always 0 </li>
     * <li>6 NUM_RESULT_SETS (int) for future use, always 0 </li>
     * <li>7 REMARKS (String) description </li>
     * <li>8 PROCEDURE_TYPE (short) if this procedure returns a result
     * (procedureNoResult or procedureReturnsResult) </li>
     * <li>9 SPECIFIC_NAME (String) name </li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param procedureNamePattern the procedure name pattern
     * @return the procedures
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getProcedures(String catalogPattern, String schemaPattern,
            String procedureNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getProcedures("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(procedureNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "ALIAS_CATALOG PROCEDURE_CAT, "
                    + "ALIAS_SCHEMA PROCEDURE_SCHEM, "
                    + "ALIAS_NAME PROCEDURE_NAME, "
                    + "COLUMN_COUNT NUM_INPUT_PARAMS, "
                    + "ZERO() NUM_OUTPUT_PARAMS, "
                    + "ZERO() NUM_RESULT_SETS, "
                    + "REMARKS, "
                    + "RETURNS_RESULT PROCEDURE_TYPE, "
                    + "ALIAS_NAME SPECIFIC_NAME "
                    + "FROM INFORMATION_SCHEMA.FUNCTION_ALIASES "
                    + "WHERE ALIAS_CATALOG LIKE ? ESCAPE ? "
                    + "AND ALIAS_SCHEMA LIKE ? ESCAPE ? "
                    + "AND ALIAS_NAME LIKE ? ESCAPE ? "
                    + "ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, NUM_INPUT_PARAMS");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(schemaPattern));
            prep.setString(4, "\\");
            prep.setString(5, getPattern(procedureNamePattern));
            prep.setString(6, "\\");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of procedure columns. The result set is sorted by
     * PROCEDURE_SCHEM, PROCEDURE_NAME, NUM_INPUT_PARAMS, and POS.
     * There are potentially multiple procedures with the same name, each with a
     * different number of input parameters.
     *
     * <ul>
     * <li>1 PROCEDURE_CAT (String) catalog </li>
     * <li>2 PROCEDURE_SCHEM (String) schema </li>
     * <li>3 PROCEDURE_NAME (String) name </li>
     * <li>4 COLUMN_NAME (String) column name </li>
     * <li>5 COLUMN_TYPE (short) column type
     * (always DatabaseMetaData.procedureColumnIn)</li>
     * <li>6 DATA_TYPE (short) sql type </li>
     * <li>7 TYPE_NAME (String) type name </li>
     * <li>8 PRECISION (int) precision </li>
     * <li>9 LENGTH (int) length </li>
     * <li>10 SCALE (short) scale </li>
     * <li>11 RADIX (int) always 10 </li>
     * <li>12 NULLABLE (short) nullable
     * (DatabaseMetaData.columnNoNulls for primitive data types,
     * DatabaseMetaData.columnNullable otherwise)</li>
     * <li>13 REMARKS (String) description </li>
     * <li>14 COLUMN_DEF (String) always null </li>
     * <li>15 SQL_DATA_TYPE (int) for future use, always 0 </li>
     * <li>16 SQL_DATETIME_SUB (int) for future use, always 0 </li>
     * <li>17 CHAR_OCTET_LENGTH (int) always null </li>
     * <li>18 ORDINAL_POSITION (int) the parameter index
     * starting from 1 (0 is the return value) </li>
     * <li>19 IS_NULLABLE (String) always "YES" </li>
     * <li>20 SPECIFIC_NAME (String) name </li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param procedureNamePattern the procedure name pattern
     * @param columnNamePattern the procedure name pattern
     * @return the procedure columns
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getProcedureColumns(String catalogPattern,
            String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getProcedureColumns("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(procedureNamePattern)+", "
                        +quote(columnNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "ALIAS_CATALOG PROCEDURE_CAT, "
                    + "ALIAS_SCHEMA PROCEDURE_SCHEM, "
                    + "ALIAS_NAME PROCEDURE_NAME, "
                    + "COLUMN_NAME, "
                    + "COLUMN_TYPE, "
                    + "DATA_TYPE, "
                    + "TYPE_NAME, "
                    + "PRECISION, "
                    + "PRECISION LENGTH, "
                    + "SCALE, "
                    + "RADIX, "
                    + "NULLABLE, "
                    + "REMARKS, "
                    + "COLUMN_DEFAULT COLUMN_DEF, "
                    + "ZERO() SQL_DATA_TYPE, "
                    + "ZERO() SQL_DATETIME_SUB, "
                    + "ZERO() CHAR_OCTET_LENGTH, "
                    + "POS ORDINAL_POSITION, "
                    + "? IS_NULLABLE, "
                    + "ALIAS_NAME SPECIFIC_NAME "
                    + "FROM INFORMATION_SCHEMA.FUNCTION_COLUMNS "
                    + "WHERE ALIAS_CATALOG LIKE ? ESCAPE ? "
                    + "AND ALIAS_SCHEMA LIKE ? ESCAPE ? "
                    + "AND ALIAS_NAME LIKE ? ESCAPE ? "
                    + "AND COLUMN_NAME LIKE ? ESCAPE ? "
                    + "ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, ORDINAL_POSITION");
            prep.setString(1, "YES");
            prep.setString(2, getCatalogPattern(catalogPattern));
            prep.setString(3, "\\");
            prep.setString(4, getSchemaPattern(schemaPattern));
            prep.setString(5, "\\");
            prep.setString(6, getPattern(procedureNamePattern));
            prep.setString(7, "\\");
            prep.setString(8, getPattern(columnNamePattern));
            prep.setString(9, "\\");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of schemas.
     * The result set is sorted by TABLE_SCHEM.
     *
     * <ul>
     * <li>1 TABLE_SCHEM (String) schema name
     * </li><li>2 TABLE_CATALOG (String) catalog name
     * </li><li>3 IS_DEFAULT (boolean) if this is the default schema
     * </li></ul>
     *
     * @return the schema list
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getSchemas() throws SQLException {
        try {
            debugCodeCall("getSchemas");
            checkClosed();
            PreparedStatement prep = conn
                    .prepareAutoCloseStatement("SELECT "
                            + "SCHEMA_NAME TABLE_SCHEM, "
                            + "CATALOG_NAME TABLE_CATALOG, "
                            +" IS_DEFAULT "
                            + "FROM INFORMATION_SCHEMA.SCHEMATA "
                            + "ORDER BY SCHEMA_NAME");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of catalogs.
     * The result set is sorted by TABLE_CAT.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) catalog name
     * </li></ul>
     *
     * @return the catalog list
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getCatalogs() throws SQLException {
        try {
            debugCodeCall("getCatalogs");
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement(
                    "SELECT CATALOG_NAME TABLE_CAT "
                    + "FROM INFORMATION_SCHEMA.CATALOGS");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of table types. This call returns a result set with five
     * records: "SYSTEM TABLE", "TABLE", "VIEW", "TABLE LINK" and "EXTERNAL".
     * <ul>
     * <li>1 TABLE_TYPE (String) table type
     * </li></ul>
     *
     * @return the table types
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getTableTypes() throws SQLException {
        try {
            debugCodeCall("getTableTypes");
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TYPE TABLE_TYPE "
                    + "FROM INFORMATION_SCHEMA.TABLE_TYPES "
                    + "ORDER BY TABLE_TYPE");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of column privileges. The result set is sorted by
     * COLUMN_NAME and PRIVILEGE
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog</li>
     * <li>2 TABLE_SCHEM (String) table schema</li>
     * <li>3 TABLE_NAME (String) table name</li>
     * <li>4 COLUMN_NAME (String) column name</li>
     * <li>5 GRANTOR (String) grantor of access</li>
     * <li>6 GRANTEE (String) grantee of access</li>
     * <li>7 PRIVILEGE (String) SELECT, INSERT, UPDATE, DELETE or REFERENCES
     * (only one per row)</li>
     * <li>8 IS_GRANTABLE (String) YES means the grantee can grant access to
     * others</li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param table a table name (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name
     *            (uppercase for unquoted names)
     * @return the list of privileges
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getColumnPrivileges(String catalogPattern,
            String schemaPattern, String table, String columnNamePattern)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getColumnPrivileges("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(table)+", "
                        +quote(columnNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "COLUMN_NAME, "
                    + "GRANTOR, "
                    + "GRANTEE, "
                    + "PRIVILEGE_TYPE PRIVILEGE, "
                    + "IS_GRANTABLE "
                    + "FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND TABLE_NAME = ? "
                    + "AND COLUMN_NAME LIKE ? ESCAPE ? "
                    + "ORDER BY COLUMN_NAME, PRIVILEGE");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(schemaPattern));
            prep.setString(4, "\\");
            prep.setString(5, table);
            prep.setString(6, getPattern(columnNamePattern));
            prep.setString(7, "\\");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of table privileges. The result set is sorted by
     * TABLE_SCHEM, TABLE_NAME, and PRIVILEGE.
     *
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog </li>
     * <li>2 TABLE_SCHEM (String) table schema </li>
     * <li>3 TABLE_NAME (String) table name </li>
     * <li>4 GRANTOR (String) grantor of access </li>
     * <li>5 GRANTEE (String) grantee of access </li>
     * <li>6 PRIVILEGE (String) SELECT, INSERT, UPDATE, DELETE or REFERENCES
     * (only one per row) </li>
     * <li>7 IS_GRANTABLE (String) YES means the grantee can grant access to
     * others </li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @return the list of privileges
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getTablePrivileges(String catalogPattern,
            String schemaPattern, String tableNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTablePrivileges("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TABLE_CATALOG TABLE_CAT, "
                    + "TABLE_SCHEMA TABLE_SCHEM, "
                    + "TABLE_NAME, "
                    + "GRANTOR, "
                    + "GRANTEE, "
                    + "PRIVILEGE_TYPE PRIVILEGE, "
                    + "IS_GRANTABLE "
                    + "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES "
                    + "WHERE TABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND TABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND TABLE_NAME LIKE ? ESCAPE ? "
                    + "ORDER BY TABLE_SCHEM, TABLE_NAME, PRIVILEGE");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(schemaPattern));
            prep.setString(4, "\\");
            prep.setString(5, getPattern(tableNamePattern));
            prep.setString(6, "\\");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of columns that best identifier a row in a table.
     * The list is ordered by SCOPE.
     *
     * <ul>
     * <li>1 SCOPE (short) scope of result (always bestRowSession)
     * </li><li>2 COLUMN_NAME (String) column name
     * </li><li>3 DATA_TYPE (short) SQL data type, see also java.sql.Types
     * </li><li>4 TYPE_NAME (String) type name
     * </li><li>5 COLUMN_SIZE (int) precision
     *         (values larger than 2 GB are returned as 2 GB)
     * </li><li>6 BUFFER_LENGTH (int) unused
     * </li><li>7 DECIMAL_DIGITS (short) scale
     * </li><li>8 PSEUDO_COLUMN (short) (always bestRowNotPseudo)
     * </li></ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableName table name (must be specified)
     * @param scope ignored
     * @param nullable ignored
     * @return the primary key index
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getBestRowIdentifier(String catalogPattern,
            String schemaPattern, String tableName, int scope, boolean nullable)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBestRowIdentifier("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableName)+", "
                        +scope+", "+nullable+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "CAST(? AS SMALLINT) SCOPE, "
                    + "C.COLUMN_NAME, "
                    + "C.DATA_TYPE, "
                    + "C.TYPE_NAME, "
                    + "C.CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, "
                    + "C.CHARACTER_MAXIMUM_LENGTH BUFFER_LENGTH, "
                    + "CAST(C.NUMERIC_SCALE AS SMALLINT) DECIMAL_DIGITS, "
                    + "CAST(? AS SMALLINT) PSEUDO_COLUMN "
                    + "FROM INFORMATION_SCHEMA.INDEXES I, "
                    +" INFORMATION_SCHEMA.COLUMNS C "
                    + "WHERE C.TABLE_NAME = I.TABLE_NAME "
                    + "AND C.COLUMN_NAME = I.COLUMN_NAME "
                    + "AND C.TABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND C.TABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND C.TABLE_NAME = ? "
                    + "AND I.PRIMARY_KEY = TRUE "
                    + "ORDER BY SCOPE");
            // SCOPE
            prep.setInt(1, DatabaseMetaData.bestRowSession);
            // PSEUDO_COLUMN
            prep.setInt(2, DatabaseMetaData.bestRowNotPseudo);
            prep.setString(3, getCatalogPattern(catalogPattern));
            prep.setString(4, "\\");
            prep.setString(5, getSchemaPattern(schemaPattern));
            prep.setString(6, "\\");
            prep.setString(7, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the list of columns that are update when any value is updated.
     * The result set is always empty.
     *
     * <ul>
     * <li>1 SCOPE (int) not used
     * </li><li>2 COLUMN_NAME (String) column name
     * </li><li>3 DATA_TYPE (int) SQL data type - see also java.sql.Types
     * </li><li>4 TYPE_NAME (String) data type name
     * </li><li>5 COLUMN_SIZE (int) precision
     *         (values larger than 2 GB are returned as 2 GB)
     * </li><li>6 BUFFER_LENGTH (int) length (bytes)
     * </li><li>7 DECIMAL_DIGITS (int) scale
     * </li><li>8 PSEUDO_COLUMN (int) is this column a pseudo column
     * </li></ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schema null (to get all objects) or a schema name
     * @param tableName table name (must be specified)
     * @return an empty result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getVersionColumns(String catalog, String schema,
            String tableName) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getVersionColumns("
                        +quote(catalog)+", "
                        +quote(schema)+", "
                        +quote(tableName)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "ZERO() SCOPE, "
                    + "COLUMN_NAME, "
                    + "CAST(DATA_TYPE AS INT) DATA_TYPE, "
                    + "TYPE_NAME, "
                    + "NUMERIC_PRECISION COLUMN_SIZE, "
                    + "NUMERIC_PRECISION BUFFER_LENGTH, "
                    + "NUMERIC_PRECISION DECIMAL_DIGITS, "
                    + "ZERO() PSEUDO_COLUMN "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE FALSE");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of primary key columns that are referenced by a table. The
     * result set is sorted by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME,
     * FK_NAME, KEY_SEQ.
     *
     * <ul>
     * <li>1 PKTABLE_CAT (String) primary catalog </li>
     * <li>2 PKTABLE_SCHEM (String) primary schema </li>
     * <li>3 PKTABLE_NAME (String) primary table </li>
     * <li>4 PKCOLUMN_NAME (String) primary column </li>
     * <li>5 FKTABLE_CAT (String) foreign catalog </li>
     * <li>6 FKTABLE_SCHEM (String) foreign schema </li>
     * <li>7 FKTABLE_NAME (String) foreign table </li>
     * <li>8 FKCOLUMN_NAME (String) foreign column </li>
     * <li>9 KEY_SEQ (short) sequence number (1, 2, ...) </li>
     * <li>10 UPDATE_RULE (short) action on update (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>11 DELETE_RULE (short) action on delete (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>12 FK_NAME (String) foreign key name </li>
     * <li>13 PK_NAME (String) primary key name </li>
     * <li>14 DEFERRABILITY (short) deferrable or not (always
     * importedKeyNotDeferrable) </li>
     * </ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern the schema name of the foreign table
     * @param tableName the name of the foreign table
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getImportedKeys(String catalogPattern,
            String schemaPattern, String tableName) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getImportedKeys("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableName)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "PKTABLE_CATALOG PKTABLE_CAT, "
                    + "PKTABLE_SCHEMA PKTABLE_SCHEM, "
                    + "PKTABLE_NAME PKTABLE_NAME, "
                    + "PKCOLUMN_NAME, "
                    + "FKTABLE_CATALOG FKTABLE_CAT, "
                    + "FKTABLE_SCHEMA FKTABLE_SCHEM, "
                    + "FKTABLE_NAME, "
                    + "FKCOLUMN_NAME, "
                    + "ORDINAL_POSITION KEY_SEQ, "
                    + "UPDATE_RULE, "
                    + "DELETE_RULE, "
                    + "FK_NAME, "
                    + "PK_NAME, "
                    + "DEFERRABILITY "
                    + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES "
                    + "WHERE FKTABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND FKTABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND FKTABLE_NAME = ? "
                    + "ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, FK_NAME, KEY_SEQ");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(schemaPattern));
            prep.setString(4, "\\");
            prep.setString(5, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of foreign key columns that reference a table. The result
     * set is sorted by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME,
     * KEY_SEQ.
     *
     * <ul>
     * <li>1 PKTABLE_CAT (String) primary catalog </li>
     * <li>2 PKTABLE_SCHEM (String) primary schema </li>
     * <li>3 PKTABLE_NAME (String) primary table </li>
     * <li>4 PKCOLUMN_NAME (String) primary column </li>
     * <li>5 FKTABLE_CAT (String) foreign catalog </li>
     * <li>6 FKTABLE_SCHEM (String) foreign schema </li>
     * <li>7 FKTABLE_NAME (String) foreign table </li>
     * <li>8 FKCOLUMN_NAME (String) foreign column </li>
     * <li>9 KEY_SEQ (short) sequence number (1,2,...) </li>
     * <li>10 UPDATE_RULE (short) action on update (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>11 DELETE_RULE (short) action on delete (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>12 FK_NAME (String) foreign key name </li>
     * <li>13 PK_NAME (String) primary key name </li>
     * <li>14 DEFERRABILITY (short) deferrable or not (always
     * importedKeyNotDeferrable) </li>
     * </ul>
     *
     * @param catalogPattern null or the catalog name
     * @param schemaPattern the schema name of the primary table
     * @param tableName the name of the primary table
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getExportedKeys(String catalogPattern,
            String schemaPattern, String tableName) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getExportedKeys("
                        +quote(catalogPattern)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableName)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "PKTABLE_CATALOG PKTABLE_CAT, "
                    + "PKTABLE_SCHEMA PKTABLE_SCHEM, "
                    + "PKTABLE_NAME PKTABLE_NAME, "
                    + "PKCOLUMN_NAME, "
                    + "FKTABLE_CATALOG FKTABLE_CAT, "
                    + "FKTABLE_SCHEMA FKTABLE_SCHEM, "
                    + "FKTABLE_NAME, "
                    + "FKCOLUMN_NAME, "
                    + "ORDINAL_POSITION KEY_SEQ, "
                    + "UPDATE_RULE, "
                    + "DELETE_RULE, "
                    + "FK_NAME, "
                    + "PK_NAME, "
                    + "DEFERRABILITY "
                    + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES "
                    + "WHERE PKTABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND PKTABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND PKTABLE_NAME = ? "
                    + "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(schemaPattern));
            prep.setString(4, "\\");
            prep.setString(5, tableName);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of foreign key columns that references a table, as well as
     * the list of primary key columns that are references by a table. The
     * result set is sorted by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME,
     * FK_NAME, KEY_SEQ.
     *
     * <ul>
     * <li>1 PKTABLE_CAT (String) primary catalog </li>
     * <li>2 PKTABLE_SCHEM (String) primary schema </li>
     * <li>3 PKTABLE_NAME (String) primary table </li>
     * <li>4 PKCOLUMN_NAME (String) primary column </li>
     * <li>5 FKTABLE_CAT (String) foreign catalog </li>
     * <li>6 FKTABLE_SCHEM (String) foreign schema </li>
     * <li>7 FKTABLE_NAME (String) foreign table </li>
     * <li>8 FKCOLUMN_NAME (String) foreign column </li>
     * <li>9 KEY_SEQ (short) sequence number (1,2,...) </li>
     * <li>10 UPDATE_RULE (short) action on update (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>11 DELETE_RULE (short) action on delete (see
     * DatabaseMetaData.importedKey...) </li>
     * <li>12 FK_NAME (String) foreign key name </li>
     * <li>13 PK_NAME (String) primary key name </li>
     * <li>14 DEFERRABILITY (short) deferrable or not (always
     * importedKeyNotDeferrable) </li>
     * </ul>
     *
     * @param primaryCatalogPattern null or the catalog name
     * @param primarySchemaPattern the schema name of the primary table
     *          (optional)
     * @param primaryTable the name of the primary table (must be specified)
     * @param foreignCatalogPattern null or the catalog name
     * @param foreignSchemaPattern the schema name of the foreign table
     *          (optional)
     * @param foreignTable the name of the foreign table (must be specified)
     * @return the result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getCrossReference(String primaryCatalogPattern,
            String primarySchemaPattern, String primaryTable, String foreignCatalogPattern,
            String foreignSchemaPattern, String foreignTable) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getCrossReference("
                        +quote(primaryCatalogPattern)+", "
                        +quote(primarySchemaPattern)+", "
                        +quote(primaryTable)+", "
                        +quote(foreignCatalogPattern)+", "
                        +quote(foreignSchemaPattern)+", "
                        +quote(foreignTable)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "PKTABLE_CATALOG PKTABLE_CAT, "
                    + "PKTABLE_SCHEMA PKTABLE_SCHEM, "
                    + "PKTABLE_NAME PKTABLE_NAME, "
                    + "PKCOLUMN_NAME, "
                    + "FKTABLE_CATALOG FKTABLE_CAT, "
                    + "FKTABLE_SCHEMA FKTABLE_SCHEM, "
                    + "FKTABLE_NAME, "
                    + "FKCOLUMN_NAME, "
                    + "ORDINAL_POSITION KEY_SEQ, "
                    + "UPDATE_RULE, "
                    + "DELETE_RULE, "
                    + "FK_NAME, "
                    + "PK_NAME, "
                    + "DEFERRABILITY "
                    + "FROM INFORMATION_SCHEMA.CROSS_REFERENCES "
                    + "WHERE PKTABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND PKTABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND PKTABLE_NAME = ? "
                    + "AND FKTABLE_CATALOG LIKE ? ESCAPE ? "
                    + "AND FKTABLE_SCHEMA LIKE ? ESCAPE ? "
                    + "AND FKTABLE_NAME = ? "
                    + "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, KEY_SEQ");
            prep.setString(1, getCatalogPattern(primaryCatalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(primarySchemaPattern));
            prep.setString(4, "\\");
            prep.setString(5, primaryTable);
            prep.setString(6, getCatalogPattern(foreignCatalogPattern));
            prep.setString(7, "\\");
            prep.setString(8, getSchemaPattern(foreignSchemaPattern));
            prep.setString(9, "\\");
            prep.setString(10, foreignTable);
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of user-defined data types.
     * This call returns an empty result set.
     *
     * <ul>
     * <li>1 TYPE_CAT (String) catalog
     * </li><li>2 TYPE_SCHEM (String) schema
     * </li><li>3 TYPE_NAME (String) type name
     * </li><li>4 CLASS_NAME (String) Java class
     * </li><li>5 DATA_TYPE (short) SQL Type - see also java.sql.Types
     * </li><li>6 REMARKS (String) description
     * </li><li>7 BASE_TYPE (short) base type - see also java.sql.Types
     * </li></ul>
     *
     * @param catalog ignored
     * @param schemaPattern ignored
     * @param typeNamePattern ignored
     * @param types ignored
     * @return an empty result set
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern,
            String typeNamePattern, int[] types) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getUDTs("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(typeNamePattern)+", "
                        +quoteIntArray(types)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "CAST(NULL AS VARCHAR) TYPE_CAT, "
                    + "CAST(NULL AS VARCHAR) TYPE_SCHEM, "
                    + "CAST(NULL AS VARCHAR) TYPE_NAME, "
                    + "CAST(NULL AS VARCHAR) CLASS_NAME, "
                    + "CAST(NULL AS SMALLINT) DATA_TYPE, "
                    + "CAST(NULL AS VARCHAR) REMARKS, "
                    + "CAST(NULL AS SMALLINT) BASE_TYPE "
                    + "FROM DUAL WHERE FALSE");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the list of data types. The result set is sorted by DATA_TYPE and
     * afterwards by how closely the data type maps to the corresponding JDBC
     * SQL type (best match first).
     *
     * <ul>
     * <li>1 TYPE_NAME (String) type name </li>
     * <li>2 DATA_TYPE (short) SQL data type - see also java.sql.Types </li>
     * <li>3 PRECISION (int) maximum precision </li>
     * <li>4 LITERAL_PREFIX (String) prefix used to quote a literal </li>
     * <li>5 LITERAL_SUFFIX (String) suffix used to quote a literal </li>
     * <li>6 CREATE_PARAMS (String) parameters used (may be null) </li>
     * <li>7 NULLABLE (short) typeNoNulls (NULL not allowed) or typeNullable
     * </li>
     * <li>8 CASE_SENSITIVE (boolean) case sensitive </li>
     * <li>9 SEARCHABLE (short) typeSearchable </li>
     * <li>10 UNSIGNED_ATTRIBUTE (boolean) unsigned </li>
     * <li>11 FIXED_PREC_SCALE (boolean) fixed precision </li>
     * <li>12 AUTO_INCREMENT (boolean) auto increment </li>
     * <li>13 LOCAL_TYPE_NAME (String) localized version of the data type </li>
     * <li>14 MINIMUM_SCALE (short) minimum scale </li>
     * <li>15 MAXIMUM_SCALE (short) maximum scale </li>
     * <li>16 SQL_DATA_TYPE (int) unused </li>
     * <li>17 SQL_DATETIME_SUB (int) unused </li>
     * <li>18 NUM_PREC_RADIX (int) 2 for binary, 10 for decimal </li>
     * </ul>
     *
     * @return the list of data types
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        try {
            debugCodeCall("getTypeInfo");
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "TYPE_NAME, "
                    + "DATA_TYPE, "
                    + "PRECISION, "
                    + "PREFIX LITERAL_PREFIX, "
                    + "SUFFIX LITERAL_SUFFIX, "
                    + "PARAMS CREATE_PARAMS, "
                    + "NULLABLE, "
                    + "CASE_SENSITIVE, "
                    + "SEARCHABLE, "
                    + "FALSE UNSIGNED_ATTRIBUTE, "
                    + "FALSE FIXED_PREC_SCALE, "
                    + "AUTO_INCREMENT, "
                    + "TYPE_NAME LOCAL_TYPE_NAME, "
                    + "MINIMUM_SCALE, "
                    + "MAXIMUM_SCALE, "
                    + "DATA_TYPE SQL_DATA_TYPE, "
                    + "ZERO() SQL_DATETIME_SUB, "
                    + "RADIX NUM_PREC_RADIX "
                    + "FROM INFORMATION_SCHEMA.TYPE_INFO "
                    + "ORDER BY DATA_TYPE, POS");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this database store data in local files.
     *
     * @return true
     */
    @Override
    public boolean usesLocalFiles() {
        debugCodeCall("usesLocalFiles");
        return true;
    }

    /**
     * Checks if this database use one file per table.
     *
     * @return false
     */
    @Override
    public boolean usesLocalFilePerTable() {
        debugCodeCall("usesLocalFilePerTable");
        return false;
    }

    /**
     * Returns the string used to quote identifiers.
     *
     * @return a double quote
     */
    @Override
    public String getIdentifierQuoteString() {
        debugCodeCall("getIdentifierQuoteString");
        return "\"";
    }

    /**
     * Gets the comma-separated list of all SQL keywords that are not supported
     * as table/column/index name, in addition to the SQL-2003 keywords. The list
     * returned is:
     * <pre>
     * LIMIT,MINUS,OFFSET,ROWNUM,SYSDATE,SYSTIME,SYSTIMESTAMP,TODAY
     * </pre>
     * The complete list of keywords (including SQL-2003 keywords) is:
     * <pre>
     * ALL, CHECK, CONSTRAINT, CROSS, CURRENT_DATE, CURRENT_TIME,
     * CURRENT_TIMESTAMP, DISTINCT, EXCEPT, EXISTS, FALSE, FETCH, FOR, FOREIGN,
     * FROM, FULL, GROUP, HAVING, INNER, INTERSECT, IS, JOIN, LIKE, LIMIT,
     * MINUS, NATURAL, NOT, NULL, OFFSET, ON, ORDER, PRIMARY, ROWNUM, SELECT,
     * SYSDATE, SYSTIME, SYSTIMESTAMP, TODAY, TRUE, UNION, UNIQUE, WHERE, WITH
     * </pre>
     *
     * @return a list of additional the keywords
     */
    @Override
    public String getSQLKeywords() {
        debugCodeCall("getSQLKeywords");
        return "LIMIT,MINUS,OFFSET,ROWNUM,SYSDATE,SYSTIME,SYSTIMESTAMP,TODAY";
    }

    /**
     * Returns the list of numeric functions supported by this database.
     *
     * @return the list
     */
    @Override
    public String getNumericFunctions() throws SQLException {
        debugCodeCall("getNumericFunctions");
        return getFunctions("Functions (Numeric)");
    }

    /**
     * Returns the list of string functions supported by this database.
     *
     * @return the list
     */
    @Override
    public String getStringFunctions() throws SQLException {
        debugCodeCall("getStringFunctions");
        return getFunctions("Functions (String)");
    }

    /**
     * Returns the list of system functions supported by this database.
     *
     * @return the list
     */
    @Override
    public String getSystemFunctions() throws SQLException {
        debugCodeCall("getSystemFunctions");
        return getFunctions("Functions (System)");
    }

    /**
     * Returns the list of date and time functions supported by this database.
     *
     * @return the list
     */
    @Override
    public String getTimeDateFunctions() throws SQLException {
        debugCodeCall("getTimeDateFunctions");
        return getFunctions("Functions (Time and Date)");
    }

    private String getFunctions(String section) throws SQLException {
        try {
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT TOPIC "
                    + "FROM INFORMATION_SCHEMA.HELP WHERE SECTION = ?");
            prep.setString(1, section);
            ResultSet rs = prep.executeQuery();
            StatementBuilder buff = new StatementBuilder();
            while (rs.next()) {
                String s = rs.getString(1).trim();
                String[] array = StringUtils.arraySplit(s, ',', true);
                for (String a : array) {
                    buff.appendExceptFirst(",");
                    String f = a.trim();
                    if (f.indexOf(' ') >= 0) {
                        // remove 'Function' from 'INSERT Function'
                        f = f.substring(0, f.indexOf(' ')).trim();
                    }
                    buff.append(f);
                }
            }
            rs.close();
            prep.close();
            return buff.toString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the default escape character for DatabaseMetaData search
     * patterns.
     *
     * @return the default escape character (always '\', independent on the
     *         mode)
     */
    @Override
    public String getSearchStringEscape() {
        debugCodeCall("getSearchStringEscape");
        return "\\";
    }

    /**
     * Returns the characters that are allowed for identifiers in addiction to
     * A-Z, a-z, 0-9 and '_'.
     *
     * @return an empty String ("")
     */
    @Override
    public String getExtraNameCharacters() {
        debugCodeCall("getExtraNameCharacters");
        return "";
    }

    /**
     * Returns whether alter table with add column is supported.
     * @return true
     */
    @Override
    public boolean supportsAlterTableWithAddColumn() {
        debugCodeCall("supportsAlterTableWithAddColumn");
        return true;
    }

    /**
     * Returns whether alter table with drop column is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsAlterTableWithDropColumn() {
        debugCodeCall("supportsAlterTableWithDropColumn");
        return true;
    }

    /**
     * Returns whether column aliasing is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsColumnAliasing() {
        debugCodeCall("supportsColumnAliasing");
        return true;
    }

    /**
     * Returns whether NULL+1 is NULL or not.
     *
     * @return true
     */
    @Override
    public boolean nullPlusNonNullIsNull() {
        debugCodeCall("nullPlusNonNullIsNull");
        return true;
    }

    /**
     * Returns whether CONVERT is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsConvert() {
        debugCodeCall("supportsConvert");
        return true;
    }

    /**
     * Returns whether CONVERT is supported for one datatype to another.
     *
     * @param fromType the source SQL type
     * @param toType the target SQL type
     * @return true
     */
    @Override
    public boolean supportsConvert(int fromType, int toType) {
        if (isDebugEnabled()) {
            debugCode("supportsConvert("+fromType+", "+fromType+");");
        }
        return true;
    }

    /**
     * Returns whether table correlation names (table alias) are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsTableCorrelationNames() {
        debugCodeCall("supportsTableCorrelationNames");
        return true;
    }

    /**
     * Returns whether table correlation names (table alias) are restricted to
     * be different than table names.
     *
     * @return false
     */
    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        debugCodeCall("supportsDifferentTableCorrelationNames");
        return false;
    }

    /**
     * Returns whether expression in ORDER BY are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsExpressionsInOrderBy() {
        debugCodeCall("supportsExpressionsInOrderBy");
        return true;
    }

    /**
     * Returns whether ORDER BY is supported if the column is not in the SELECT
     * list.
     *
     * @return true
     */
    @Override
    public boolean supportsOrderByUnrelated() {
        debugCodeCall("supportsOrderByUnrelated");
        return true;
    }

    /**
     * Returns whether GROUP BY is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsGroupBy() {
        debugCodeCall("supportsGroupBy");
        return true;
    }

    /**
     * Returns whether GROUP BY is supported if the column is not in the SELECT
     * list.
     *
     * @return true
     */
    @Override
    public boolean supportsGroupByUnrelated() {
        debugCodeCall("supportsGroupByUnrelated");
        return true;
    }

    /**
     * Checks whether a GROUP BY clause can use columns that are not in the
     * SELECT clause, provided that it specifies all the columns in the SELECT
     * clause.
     *
     * @return true
     */
    @Override
    public boolean supportsGroupByBeyondSelect() {
        debugCodeCall("supportsGroupByBeyondSelect");
        return true;
    }

    /**
     * Returns whether LIKE... ESCAPE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsLikeEscapeClause() {
        debugCodeCall("supportsLikeEscapeClause");
        return true;
    }

    /**
     * Returns whether multiple result sets are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsMultipleResultSets() {
        debugCodeCall("supportsMultipleResultSets");
        return false;
    }

    /**
     * Returns whether multiple transactions (on different connections) are
     * supported.
     *
     * @return true
     */
    @Override
    public boolean supportsMultipleTransactions() {
        debugCodeCall("supportsMultipleTransactions");
        return true;
    }

    /**
     * Returns whether columns with NOT NULL are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsNonNullableColumns() {
        debugCodeCall("supportsNonNullableColumns");
        return true;
    }

    /**
     * Returns whether ODBC Minimum SQL grammar is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsMinimumSQLGrammar() {
        debugCodeCall("supportsMinimumSQLGrammar");
        return true;
    }

    /**
     * Returns whether ODBC Core SQL grammar is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCoreSQLGrammar() {
        debugCodeCall("supportsCoreSQLGrammar");
        return true;
    }

    /**
     * Returns whether ODBC Extended SQL grammar is supported.
     *
     * @return false
     */
    @Override
    public boolean supportsExtendedSQLGrammar() {
        debugCodeCall("supportsExtendedSQLGrammar");
        return false;
    }

    /**
     * Returns whether SQL-92 entry level grammar is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        debugCodeCall("supportsANSI92EntryLevelSQL");
        return true;
    }

    /**
     * Returns whether SQL-92 intermediate level grammar is supported.
     *
     * @return false
     */
    @Override
    public boolean supportsANSI92IntermediateSQL() {
        debugCodeCall("supportsANSI92IntermediateSQL");
        return false;
    }

    /**
     * Returns whether SQL-92 full level grammar is supported.
     *
     * @return false
     */
    @Override
    public boolean supportsANSI92FullSQL() {
        debugCodeCall("supportsANSI92FullSQL");
        return false;
    }

    /**
     * Returns whether referential integrity is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        debugCodeCall("supportsIntegrityEnhancementFacility");
        return true;
    }

    /**
     * Returns whether outer joins are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsOuterJoins() {
        debugCodeCall("supportsOuterJoins");
        return true;
    }

    /**
     * Returns whether full outer joins are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsFullOuterJoins() {
        debugCodeCall("supportsFullOuterJoins");
        return false;
    }

    /**
     * Returns whether limited outer joins are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsLimitedOuterJoins() {
        debugCodeCall("supportsLimitedOuterJoins");
        return true;
    }

    /**
     * Returns the term for "schema".
     *
     * @return "schema"
     */
    @Override
    public String getSchemaTerm() {
        debugCodeCall("getSchemaTerm");
        return "schema";
    }

    /**
     * Returns the term for "procedure".
     *
     * @return "procedure"
     */
    @Override
    public String getProcedureTerm() {
        debugCodeCall("getProcedureTerm");
        return "procedure";
    }

    /**
     * Returns the term for "catalog".
     *
     * @return "catalog"
     */
    @Override
    public String getCatalogTerm() {
        debugCodeCall("getCatalogTerm");
        return "catalog";
    }

    /**
     * Returns whether the catalog is at the beginning.
     *
     * @return true
     */
    @Override
    public boolean isCatalogAtStart() {
        debugCodeCall("isCatalogAtStart");
        return true;
    }

    /**
     * Returns the catalog separator.
     *
     * @return "."
     */
    @Override
    public String getCatalogSeparator() {
        debugCodeCall("getCatalogSeparator");
        return ".";
    }

    /**
     * Returns whether the schema name in INSERT, UPDATE, DELETE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInDataManipulation() {
        debugCodeCall("supportsSchemasInDataManipulation");
        return true;
    }

    /**
     * Returns whether the schema name in procedure calls is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInProcedureCalls() {
        debugCodeCall("supportsSchemasInProcedureCalls");
        return true;
    }

    /**
     * Returns whether the schema name in CREATE TABLE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInTableDefinitions() {
        debugCodeCall("supportsSchemasInTableDefinitions");
        return true;
    }

    /**
     * Returns whether the schema name in CREATE INDEX is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        debugCodeCall("supportsSchemasInIndexDefinitions");
        return true;
    }

    /**
     * Returns whether the schema name in GRANT is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        debugCodeCall("supportsSchemasInPrivilegeDefinitions");
        return true;
    }

    /**
     * Returns whether the catalog name in INSERT, UPDATE, DELETE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCatalogsInDataManipulation() {
        debugCodeCall("supportsCatalogsInDataManipulation");
        return true;
    }

    /**
     * Returns whether the catalog name in procedure calls is supported.
     *
     * @return false
     */
    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        debugCodeCall("supportsCatalogsInProcedureCalls");
        return false;
    }

    /**
     * Returns whether the catalog name in CREATE TABLE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        debugCodeCall("supportsCatalogsInTableDefinitions");
        return true;
    }

    /**
     * Returns whether the catalog name in CREATE INDEX is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        debugCodeCall("supportsCatalogsInIndexDefinitions");
        return true;
    }

    /**
     * Returns whether the catalog name in GRANT is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        debugCodeCall("supportsCatalogsInPrivilegeDefinitions");
        return true;
    }

    /**
     * Returns whether positioned deletes are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsPositionedDelete() {
        debugCodeCall("supportsPositionedDelete");
        return true;
    }

    /**
     * Returns whether positioned updates are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsPositionedUpdate() {
        debugCodeCall("supportsPositionedUpdate");
        return true;
    }

    /**
     * Returns whether SELECT ... FOR UPDATE is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSelectForUpdate() {
        debugCodeCall("supportsSelectForUpdate");
        return true;
    }

    /**
     * Returns whether stored procedures are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsStoredProcedures() {
        debugCodeCall("supportsStoredProcedures");
        return false;
    }

    /**
     * Returns whether subqueries (SELECT) in comparisons are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSubqueriesInComparisons() {
        debugCodeCall("supportsSubqueriesInComparisons");
        return true;
    }

    /**
     * Returns whether SELECT in EXISTS is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSubqueriesInExists() {
        debugCodeCall("supportsSubqueriesInExists");
        return true;
    }

    /**
     * Returns whether IN(SELECT...) is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSubqueriesInIns() {
        debugCodeCall("supportsSubqueriesInIns");
        return true;
    }

    /**
     * Returns whether subqueries in quantified expression are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        debugCodeCall("supportsSubqueriesInQuantifieds");
        return true;
    }

    /**
     * Returns whether correlated subqueries are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsCorrelatedSubqueries() {
        debugCodeCall("supportsCorrelatedSubqueries");
        return true;
    }

    /**
     * Returns whether UNION SELECT is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsUnion() {
        debugCodeCall("supportsUnion");
        return true;
    }

    /**
     * Returns whether UNION ALL SELECT is supported.
     *
     * @return true
     */
    @Override
    public boolean supportsUnionAll() {
        debugCodeCall("supportsUnionAll");
        return true;
    }

    /**
     * Returns whether open result sets across commits are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        debugCodeCall("supportsOpenCursorsAcrossCommit");
        return false;
    }

    /**
     * Returns whether open result sets across rollback are supported.
     *
     * @return false
     */
    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        debugCodeCall("supportsOpenCursorsAcrossRollback");
        return false;
    }

    /**
     * Returns whether open statements across commit are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        debugCodeCall("supportsOpenStatementsAcrossCommit");
        return true;
    }

    /**
     * Returns whether open statements across rollback are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        debugCodeCall("supportsOpenStatementsAcrossRollback");
        return true;
    }

    /**
     * Returns whether transactions are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsTransactions() {
        debugCodeCall("supportsTransactions");
        return true;
    }

    /**
     * Returns whether a specific transaction isolation level is supported.
     *
     * @param level the transaction isolation level (Connection.TRANSACTION_*)
     * @return true
     */
    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        debugCodeCall("supportsTransactionIsolationLevel");
        if (level == Connection.TRANSACTION_READ_UNCOMMITTED) {
            // currently the combination of LOCK_MODE=0 and MULTI_THREADED
            // is not supported, also see code in Database#setLockMode(int)
            PreparedStatement prep = conn.prepareAutoCloseStatement(
                    "SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME=?");
            prep.setString(1, "MULTI_THREADED");
            ResultSet rs = prep.executeQuery();
            return !rs.next() || !rs.getString(1).equals("1");
        }
        return true;
    }

    /**
     * Returns whether data manipulation and CREATE/DROP is supported in
     * transactions.
     *
     * @return false
     */
    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        debugCodeCall("supportsDataDefinitionAndDataManipulationTransactions");
        return false;
    }

    /**
     * Returns whether only data manipulations are supported in transactions.
     *
     * @return true
     */
    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        debugCodeCall("supportsDataManipulationTransactionsOnly");
        return true;
    }

    /**
     * Returns whether CREATE/DROP commit an open transaction.
     *
     * @return true
     */
    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        debugCodeCall("dataDefinitionCausesTransactionCommit");
        return true;
    }

    /**
     * Returns whether CREATE/DROP do not affect transactions.
     *
     * @return false
     */
    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        debugCodeCall("dataDefinitionIgnoredInTransactions");
        return false;
    }

    /**
     * Returns whether a specific result set type is supported.
     * ResultSet.TYPE_SCROLL_SENSITIVE is not supported.
     *
     * @param type the result set type
     * @return true for all types except ResultSet.TYPE_FORWARD_ONLY
     */
    @Override
    public boolean supportsResultSetType(int type) {
        debugCodeCall("supportsResultSetType", type);
        return type != ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    /**
     * Returns whether a specific result set concurrency is supported.
     * ResultSet.TYPE_SCROLL_SENSITIVE is not supported.
     *
     * @param type the result set type
     * @param concurrency the result set concurrency
     * @return true if the type is not ResultSet.TYPE_SCROLL_SENSITIVE
     */
    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        if (isDebugEnabled()) {
            debugCode("supportsResultSetConcurrency("+type+", "+concurrency+");");
        }
        return type != ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    /**
     * Returns whether own updates are visible.
     *
     * @param type the result set type
     * @return true
     */
    @Override
    public boolean ownUpdatesAreVisible(int type) {
        debugCodeCall("ownUpdatesAreVisible", type);
        return true;
    }

    /**
     * Returns whether own deletes are visible.
     *
     * @param type the result set type
     * @return false
     */
    @Override
    public boolean ownDeletesAreVisible(int type) {
        debugCodeCall("ownDeletesAreVisible", type);
        return false;
    }

    /**
     * Returns whether own inserts are visible.
     *
     * @param type the result set type
     * @return false
     */
    @Override
    public boolean ownInsertsAreVisible(int type) {
        debugCodeCall("ownInsertsAreVisible", type);
        return false;
    }

    /**
     * Returns whether other updates are visible.
     *
     * @param type the result set type
     * @return false
     */
    @Override
    public boolean othersUpdatesAreVisible(int type) {
        debugCodeCall("othersUpdatesAreVisible", type);
        return false;
    }

    /**
     * Returns whether other deletes are visible.
     *
     * @param type the result set type
     * @return false
     */
    @Override
    public boolean othersDeletesAreVisible(int type) {
        debugCodeCall("othersDeletesAreVisible", type);
        return false;
    }

    /**
     * Returns whether other inserts are visible.
     *
     * @param type the result set type
     * @return false
     */
    @Override
    public boolean othersInsertsAreVisible(int type) {
        debugCodeCall("othersInsertsAreVisible", type);
        return false;
    }

    /**
     * Returns whether updates are detected.
     *
     * @param type the result set type
     * @return false
     */
    @Override
    public boolean updatesAreDetected(int type) {
        debugCodeCall("updatesAreDetected", type);
        return false;
    }

    /**
     * Returns whether deletes are detected.
     *
     * @param type the result set type
     * @return false
     */
    @Override
    public boolean deletesAreDetected(int type) {
        debugCodeCall("deletesAreDetected", type);
        return false;
    }

    /**
     * Returns whether inserts are detected.
     *
     * @param type the result set type
     * @return false
     */
    @Override
    public boolean insertsAreDetected(int type) {
        debugCodeCall("insertsAreDetected", type);
        return false;
    }

    /**
     * Returns whether batch updates are supported.
     *
     * @return true
     */
    @Override
    public boolean supportsBatchUpdates() {
        debugCodeCall("supportsBatchUpdates");
        return true;
    }

    /**
     * Returns whether the maximum row size includes blobs.
     *
     * @return false
     */
    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        debugCodeCall("doesMaxRowSizeIncludeBlobs");
        return false;
    }

    /**
     * Returns the default transaction isolation level.
     *
     * @return Connection.TRANSACTION_READ_COMMITTED
     */
    @Override
    public int getDefaultTransactionIsolation() {
        debugCodeCall("getDefaultTransactionIsolation");
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
     * table name.
     *
     * @return false
     */
    @Override
    public boolean supportsMixedCaseIdentifiers() {
        debugCodeCall("supportsMixedCaseIdentifiers");
        return false;
    }

    /**
     * Checks if a table created with CREATE TABLE "Test"(ID INT) is a different
     * table than a table created with CREATE TABLE TEST(ID INT).
     *
     * @return true usually, and false in MySQL mode
     */
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        debugCodeCall("supportsMixedCaseQuotedIdentifiers");
        String m = conn.getMode();
        return !m.equals("MySQL");
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns TEST as the
     * table name.
     *
     * @return true usually, and false in MySQL mode
     */
    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        debugCodeCall("storesUpperCaseIdentifiers");
        String m = conn.getMode();
        return !m.equals("MySQL");
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns test as the
     * table name.
     *
     * @return false usually, and true in MySQL mode
     */
    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        debugCodeCall("storesLowerCaseIdentifiers");
        String m = conn.getMode();
        return m.equals("MySQL");
    }

    /**
     * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
     * table name.
     *
     * @return false
     */
    @Override
    public boolean storesMixedCaseIdentifiers() {
        debugCodeCall("storesMixedCaseIdentifiers");
        return false;
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns TEST as the
     * table name.
     *
     * @return false usually, and true in MySQL mode
     */
    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        debugCodeCall("storesUpperCaseQuotedIdentifiers");
        String m = conn.getMode();
        return m.equals("MySQL");
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns test as the
     * table name.
     *
     * @return false usually, and true in MySQL mode
     */
    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        debugCodeCall("storesLowerCaseQuotedIdentifiers");
        String m = conn.getMode();
        return m.equals("MySQL");
    }

    /**
     * Checks if for CREATE TABLE "Test"(ID INT), getTables returns Test as the
     * table name.
     *
     * @return true usually, and false in MySQL mode
     */
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        debugCodeCall("storesMixedCaseQuotedIdentifiers");
        String m = conn.getMode();
        return !m.equals("MySQL");
    }

    /**
     * Returns the maximum length for hex values (characters).
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxBinaryLiteralLength() {
        debugCodeCall("getMaxBinaryLiteralLength");
        return 0;
    }

    /**
     * Returns the maximum length for literals.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxCharLiteralLength() {
        debugCodeCall("getMaxCharLiteralLength");
        return 0;
    }

    /**
     * Returns the maximum length for column names.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnNameLength() {
        debugCodeCall("getMaxColumnNameLength");
        return 0;
    }

    /**
     * Returns the maximum number of columns in GROUP BY.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInGroupBy() {
        debugCodeCall("getMaxColumnsInGroupBy");
        return 0;
    }

    /**
     * Returns the maximum number of columns in CREATE INDEX.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInIndex() {
        debugCodeCall("getMaxColumnsInIndex");
        return 0;
    }

    /**
     * Returns the maximum number of columns in ORDER BY.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInOrderBy() {
        debugCodeCall("getMaxColumnsInOrderBy");
        return 0;
    }

    /**
     * Returns the maximum number of columns in SELECT.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInSelect() {
        debugCodeCall("getMaxColumnsInSelect");
        return 0;
    }

    /**
     * Returns the maximum number of columns in CREATE TABLE.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxColumnsInTable() {
        debugCodeCall("getMaxColumnsInTable");
        return 0;
    }

    /**
     * Returns the maximum number of open connection.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxConnections() {
        debugCodeCall("getMaxConnections");
        return 0;
    }

    /**
     * Returns the maximum length for a cursor name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxCursorNameLength() {
        debugCodeCall("getMaxCursorNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for an index (in bytes).
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxIndexLength() {
        debugCodeCall("getMaxIndexLength");
        return 0;
    }

    /**
     * Returns the maximum length for a schema name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxSchemaNameLength() {
        debugCodeCall("getMaxSchemaNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for a procedure name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxProcedureNameLength() {
        debugCodeCall("getMaxProcedureNameLength");
        return 0;
    }

    /**
     * Returns the maximum length for a catalog name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxCatalogNameLength() {
        debugCodeCall("getMaxCatalogNameLength");
        return 0;
    }

    /**
     * Returns the maximum size of a row (in bytes).
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxRowSize() {
        debugCodeCall("getMaxRowSize");
        return 0;
    }

    /**
     * Returns the maximum length of a statement.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxStatementLength() {
        debugCodeCall("getMaxStatementLength");
        return 0;
    }

    /**
     * Returns the maximum number of open statements.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxStatements() {
        debugCodeCall("getMaxStatements");
        return 0;
    }

    /**
     * Returns the maximum length for a table name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxTableNameLength() {
        debugCodeCall("getMaxTableNameLength");
        return 0;
    }

    /**
     * Returns the maximum number of tables in a SELECT.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxTablesInSelect() {
        debugCodeCall("getMaxTablesInSelect");
        return 0;
    }

    /**
     * Returns the maximum length for a user name.
     *
     * @return 0 for limit is unknown
     */
    @Override
    public int getMaxUserNameLength() {
        debugCodeCall("getMaxUserNameLength");
        return 0;
    }

    /**
     * Does the database support savepoints.
     *
     * @return true
     */
    @Override
    public boolean supportsSavepoints() {
        debugCodeCall("supportsSavepoints");
        return true;
    }

    /**
     * Does the database support named parameters.
     *
     * @return false
     */
    @Override
    public boolean supportsNamedParameters() {
        debugCodeCall("supportsNamedParameters");
        return false;
    }

    /**
     * Does the database support multiple open result sets.
     *
     * @return true
     */
    @Override
    public boolean supportsMultipleOpenResults() {
        debugCodeCall("supportsMultipleOpenResults");
        return true;
    }

    /**
     * Does the database support getGeneratedKeys.
     *
     * @return true
     */
    @Override
    public boolean supportsGetGeneratedKeys() {
        debugCodeCall("supportsGetGeneratedKeys");
        return true;
    }

    /**
     * [Not supported]
     */
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern,
            String typeNamePattern) throws SQLException {
        throw unsupported("superTypes");
    }

    /**
     * Get the list of super tables of a table. This method currently returns an
     * empty result set.
     * <ul>
     * <li>1 TABLE_CAT (String) table catalog</li>
     * <li>2 TABLE_SCHEM (String) table schema</li>
     * <li>3 TABLE_NAME (String) table name</li>
     * <li>4 SUPERTABLE_NAME (String) the name of the super table</li>
     * </ul>
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name pattern
     *            (uppercase for unquoted names)
     * @return an empty result set
     */
    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getSuperTables("
                        +quote(catalog)+", "
                        +quote(schemaPattern)+", "
                        +quote(tableNamePattern)+");");
            }
            checkClosed();
            PreparedStatement prep = conn.prepareAutoCloseStatement("SELECT "
                    + "CATALOG_NAME TABLE_CAT, "
                    + "CATALOG_NAME TABLE_SCHEM, "
                    + "CATALOG_NAME TABLE_NAME, "
                    + "CATALOG_NAME SUPERTABLE_NAME "
                    + "FROM INFORMATION_SCHEMA.CATALOGS "
                    + "WHERE FALSE");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern,
            String typeNamePattern, String attributeNamePattern)
            throws SQLException {
        throw unsupported("attributes");
    }

    /**
     * Does this database supports a result set holdability.
     *
     * @param holdability ResultSet.HOLD_CURSORS_OVER_COMMIT or
     *            CLOSE_CURSORS_AT_COMMIT
     * @return true if the holdability is ResultSet.CLOSE_CURSORS_AT_COMMIT
     */
    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        debugCodeCall("supportsResultSetHoldability", holdability);
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * Gets the result set holdability.
     *
     * @return ResultSet.CLOSE_CURSORS_AT_COMMIT
     */
    @Override
    public int getResultSetHoldability() {
        debugCodeCall("getResultSetHoldability");
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * Gets the major version of the database.
     *
     * @return the major version
     */
    @Override
    public int getDatabaseMajorVersion() {
        debugCodeCall("getDatabaseMajorVersion");
        return Constants.VERSION_MAJOR;
    }

    /**
     * Gets the minor version of the database.
     *
     * @return the minor version
     */
    @Override
    public int getDatabaseMinorVersion() {
        debugCodeCall("getDatabaseMinorVersion");
        return Constants.VERSION_MINOR;
    }

    /**
     * Gets the major version of the supported JDBC API.
     *
     * @return the major version (4)
     */
    @Override
    public int getJDBCMajorVersion() {
        debugCodeCall("getJDBCMajorVersion");
        return 4;
    }

    /**
     * Gets the minor version of the supported JDBC API.
     *
     * @return the minor version (0)
     */
    @Override
    public int getJDBCMinorVersion() {
        debugCodeCall("getJDBCMinorVersion");
        return 0;
    }

    /**
     * Gets the SQL State type.
     *
     * @return DatabaseMetaData.sqlStateSQL99
     */
    @Override
    public int getSQLStateType() {
        debugCodeCall("getSQLStateType");
        return DatabaseMetaData.sqlStateSQL99;
    }

    /**
     * Does the database make a copy before updating.
     *
     * @return false
     */
    @Override
    public boolean locatorsUpdateCopy() {
        debugCodeCall("locatorsUpdateCopy");
        return false;
    }

    /**
     * Does the database support statement pooling.
     *
     * @return false
     */
    @Override
    public boolean supportsStatementPooling() {
        debugCodeCall("supportsStatementPooling");
        return false;
    }

    // =============================================================

    private void checkClosed() {
        conn.checkClosed();
    }

    private static String getPattern(String pattern) {
        return pattern == null ? "%" : pattern;
    }

    private static String getSchemaPattern(String pattern) {
        return pattern == null ? "%" : pattern.length() == 0 ?
                Constants.SCHEMA_MAIN : pattern;
    }

    private static String getCatalogPattern(String catalogPattern) {
        // Workaround for OpenOffice: getColumns is called with "" as the
        // catalog
        return catalogPattern == null || catalogPattern.length() == 0 ?
                "%" : catalogPattern;
    }

    /**
     * Get the lifetime of a rowid.
     *
     * @return ROWID_UNSUPPORTED
     */
    @Override
    public RowIdLifetime getRowIdLifetime() {
        debugCodeCall("getRowIdLifetime");
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    /**
     * Gets the list of schemas in the database.
     * The result set is sorted by TABLE_SCHEM.
     *
     * <ul>
     * <li>1 TABLE_SCHEM (String) schema name
     * </li><li>2 TABLE_CATALOG (String) catalog name
     * </li><li>3 IS_DEFAULT (boolean) if this is the default schema
     * </li></ul>
     *
     * @param catalogPattern null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @return the schema list
     * @throws SQLException if the connection is closed
     */
    @Override
    public ResultSet getSchemas(String catalogPattern, String schemaPattern)
            throws SQLException {
        try {
            debugCodeCall("getSchemas(String,String)");
            checkClosed();
            PreparedStatement prep = conn
                    .prepareAutoCloseStatement("SELECT "
                            + "SCHEMA_NAME TABLE_SCHEM, "
                            + "CATALOG_NAME TABLE_CATALOG, "
                            +" IS_DEFAULT "
                            + "FROM INFORMATION_SCHEMA.SCHEMATA "
                            + "WHERE CATALOG_NAME LIKE ? ESCAPE ? "
                            + "AND SCHEMA_NAME LIKE ? ESCAPE ? "
                            + "ORDER BY SCHEMA_NAME");
            prep.setString(1, getCatalogPattern(catalogPattern));
            prep.setString(2, "\\");
            prep.setString(3, getSchemaPattern(schemaPattern));
            prep.setString(4, "\\");
            return prep.executeQuery();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether the database supports calling functions using the call
     * syntax.
     *
     * @return true
     */
    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        debugCodeCall("supportsStoredFunctionsUsingCallSyntax");
        return true;
    }

    /**
     * Returns whether an exception while auto commit is on closes all result
     * sets.
     *
     * @return false
     */
    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        debugCodeCall("autoCommitFailureClosesAllResultSets");
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        Properties clientInfo = conn.getClientInfo();
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Name", Types.VARCHAR, 0, 0);
        result.addColumn("Value", Types.VARCHAR, 0, 0);
        for (Object key : clientInfo.keySet()) {
            result.addRow(key, clientInfo.get(key));
        }
        return result;
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return (T) this;
            }
            throw DbException.getInvalidValueException("iface", iface);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    /**
     * [Not supported] Gets the list of function columns.
     */
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
            String functionNamePattern, String columnNamePattern)
            throws SQLException {
        throw unsupported("getFunctionColumns");
    }

    /**
     * [Not supported] Gets the list of functions.
     */
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern,
            String functionNamePattern) throws SQLException {
        throw unsupported("getFunctions");
    }

    /**
     * [Not supported]
     */
    @Override
    public boolean generatedKeyAlwaysReturned() {
        return true;
    }

    /**
     * [Not supported]
     *
     * @param catalog null (to get all objects) or the catalog name
     * @param schemaPattern null (to get all objects) or a schema name
     *            (uppercase for unquoted names)
     * @param tableNamePattern null (to get all objects) or a table name
     *            (uppercase for unquoted names)
     * @param columnNamePattern null (to get all objects) or a column name
     *            (uppercase for unquoted names)
     */
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern) {
        return null;
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": " + conn;
    }

}
