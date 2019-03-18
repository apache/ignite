/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;

import org.h2.message.DbException;
import org.h2.util.Utils;

/**
 * This class contains various database-level settings. To override the
 * documented default value for a database, append the setting in the database
 * URL: "jdbc:h2:test;ALIAS_COLUMN_NAME=TRUE" when opening the first connection
 * to the database. The settings can not be changed once the database is open.
 * <p>
 * Some settings are a last resort and temporary solution to work around a
 * problem in the application or database engine. Also, there are system
 * properties to enable features that are not yet fully tested or that are not
 * backward compatible.
 * </p>
 */
public class DbSettings extends SettingsBase {

    private static DbSettings defaultSettings;

    /**
     * Database setting <code>ALIAS_COLUMN_NAME</code> (default: false).<br />
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     * <br />
     * This setting only affects the default and the MySQL mode. When using
     * any other mode, this feature is enabled for compatibility, even if this
     * database setting is not enabled explicitly.
     */
    public final boolean aliasColumnName = get("ALIAS_COLUMN_NAME", false);

    /**
     * Database setting <code>ANALYZE_AUTO</code> (default: 2000).<br />
     * After changing this many rows, ANALYZE is automatically run for a table.
     * Automatically running ANALYZE is disabled if set to 0. If set to 1000,
     * then ANALYZE will run against each user table after about 1000 changes to
     * that table. The time between running ANALYZE doubles each time since
     * starting the database. It is not run on local temporary tables, and
     * tables that have a trigger on SELECT.
     */
    public final int analyzeAuto = get("ANALYZE_AUTO", 2000);

    /**
     * Database setting <code>ANALYZE_SAMPLE</code> (default: 10000).<br />
     * The default sample size when analyzing a table.
     */
    public final int analyzeSample = get("ANALYZE_SAMPLE", 10_000);

    /**
     * Database setting <code>DATABASE_TO_UPPER</code> (default: true).<br />
     * Database short names are converted to uppercase for the DATABASE()
     * function, and in the CATALOG column of all database meta data methods.
     * Setting this to "false" is experimental. When set to false, all
     * identifier names (table names, column names) are case sensitive (except
     * aggregate, built-in functions, data types, and keywords).
     */
    public final boolean databaseToUpper = get("DATABASE_TO_UPPER", true);

    /**
     * Database setting <code>DB_CLOSE_ON_EXIT</code> (default: true).<br />
     * Close the database when the virtual machine exits normally, using a
     * shutdown hook.
     */
    public final boolean dbCloseOnExit = get("DB_CLOSE_ON_EXIT", true);

    /**
     * Database setting <code>DEFAULT_CONNECTION</code> (default: false).<br />
     * Whether Java functions can use
     * <code>DriverManager.getConnection("jdbc:default:connection")</code> to
     * get a database connection. This feature is disabled by default for
     * performance reasons. Please note the Oracle JDBC driver will try to
     * resolve this database URL if it is loaded before the H2 driver.
     */
    public final boolean defaultConnection = get("DEFAULT_CONNECTION", false);

    /**
     * Database setting <code>DEFAULT_ESCAPE</code> (default: \).<br />
     * The default escape character for LIKE comparisons. To select no escape
     * character, use an empty string.
     */
    public final String defaultEscape = get("DEFAULT_ESCAPE", "\\");

    /**
     * Database setting <code>DEFRAG_ALWAYS</code> (default: false).<br />
     * Each time the database is closed normally, it is fully defragmented (the
     * same as SHUTDOWN DEFRAG). If you execute SHUTDOWN COMPACT, then this
     * setting is ignored.
     */
    public final boolean defragAlways = get("DEFRAG_ALWAYS", false);

    /**
     * Database setting <code>DROP_RESTRICT</code> (default: true).<br />
     * Whether the default action for DROP TABLE, DROP VIEW, and DROP SCHEMA
     * is RESTRICT.
     */
    public final boolean dropRestrict = get("DROP_RESTRICT", true);

    /**
     * Database setting <code>EARLY_FILTER</code> (default: false).<br />
     * This setting allows table implementations to apply filter conditions
     * early on.
     */
    public final boolean earlyFilter = get("EARLY_FILTER", false);

    /**
     * Database setting <code>ESTIMATED_FUNCTION_TABLE_ROWS</code> (default:
     * 1000).<br />
     * The estimated number of rows in a function table (for example, CSVREAD or
     * FTL_SEARCH). This value is used by the optimizer.
     */
    public final int estimatedFunctionTableRows = get(
            "ESTIMATED_FUNCTION_TABLE_ROWS", 1000);

    /**
     * Database setting <code>FUNCTIONS_IN_SCHEMA</code>
     * (default: true).<br />
     * If set, all functions are stored in a schema. Specially, the SCRIPT
     * statement will always include the schema name in the CREATE ALIAS
     * statement. This is not backward compatible with H2 versions 1.2.134 and
     * older.
     */
    public final boolean functionsInSchema = get("FUNCTIONS_IN_SCHEMA", true);

    /**
     * Database setting <code>LARGE_TRANSACTIONS</code> (default: true).<br />
     * Support very large transactions
     */
    public final boolean largeTransactions = get("LARGE_TRANSACTIONS", true);

    /**
     * Database setting <code>LOB_TIMEOUT</code> (default: 300000,
     * which means 5 minutes).<br />
     * The number of milliseconds a temporary LOB reference is kept until it
     * times out. After the timeout, the LOB is no longer accessible using this
     * reference.
     */
    public final int lobTimeout = get("LOB_TIMEOUT", 300_000);

    /**
     * Database setting <code>MAX_COMPACT_COUNT</code>
     * (default: Integer.MAX_VALUE).<br />
     * The maximum number of pages to move when closing a database.
     */
    public final int maxCompactCount = get("MAX_COMPACT_COUNT",
            Integer.MAX_VALUE);

    /**
     * Database setting <code>MAX_COMPACT_TIME</code> (default: 200).<br />
     * The maximum time in milliseconds used to compact a database when closing.
     */
    public final int maxCompactTime = get("MAX_COMPACT_TIME", 200);

    /**
     * Database setting <code>MAX_QUERY_TIMEOUT</code> (default: 0).<br />
     * The maximum timeout of a query in milliseconds. The default is 0, meaning
     * no limit. Please note the actual query timeout may be set to a lower
     * value.
     */
    public final int maxQueryTimeout = get("MAX_QUERY_TIMEOUT", 0);

    /**
     * Database setting <code>OPTIMIZE_DISTINCT</code> (default: true).<br />
     * Improve the performance of simple DISTINCT queries if an index is
     * available for the given column. The optimization is used if:
     * <ul>
     * <li>The select is a single column query without condition </li>
     * <li>The query contains only one table, and no group by </li>
     * <li>There is only one table involved </li>
     * <li>There is an ascending index on the column </li>
     * <li>The selectivity of the column is below 20 </li>
     * </ul>
     */
    public final boolean optimizeDistinct = get("OPTIMIZE_DISTINCT", true);

    /**
     * Database setting <code>OPTIMIZE_EVALUATABLE_SUBQUERIES</code> (default:
     * true).<br />
     * Optimize subqueries that are not dependent on the outer query.
     */
    public final boolean optimizeEvaluatableSubqueries = get(
            "OPTIMIZE_EVALUATABLE_SUBQUERIES", true);

    /**
     * Database setting <code>OPTIMIZE_INSERT_FROM_SELECT</code>
     * (default: true).<br />
     * Insert into table from query directly bypassing temporary disk storage.
     * This also applies to create table as select.
     */
    public final boolean optimizeInsertFromSelect = get(
            "OPTIMIZE_INSERT_FROM_SELECT", true);

    /**
     * Database setting <code>OPTIMIZE_IN_LIST</code> (default: true).<br />
     * Optimize IN(...) and IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInList = get("OPTIMIZE_IN_LIST", true);

    /**
     * Database setting <code>OPTIMIZE_IN_SELECT</code> (default: true).<br />
     * Optimize IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInSelect = get("OPTIMIZE_IN_SELECT", true);

    /**
     * Database setting <code>OPTIMIZE_IS_NULL</code> (default: false).<br />
     * Use an index for condition of the form columnName IS NULL.
     */
    public final boolean optimizeIsNull = get("OPTIMIZE_IS_NULL", true);

    /**
     * Database setting <code>OPTIMIZE_OR</code> (default: true).<br />
     * Convert (C=? OR C=?) to (C IN(?, ?)).
     */
    public final boolean optimizeOr = get("OPTIMIZE_OR", true);

    /**
     * Database setting <code>OPTIMIZE_TWO_EQUALS</code> (default: true).<br />
     * Optimize expressions of the form A=B AND B=1. In this case, AND A=1 is
     * added so an index on A can be used.
     */
    public final boolean optimizeTwoEquals = get("OPTIMIZE_TWO_EQUALS", true);

    /**
     * Database setting <code>OPTIMIZE_UPDATE</code> (default: true).<br />
     * Speed up inserts, updates, and deletes by not reading all rows from a
     * page unless necessary.
     */
    public final boolean optimizeUpdate = get("OPTIMIZE_UPDATE", true);

    /**
     * Database setting <code>PAGE_STORE_MAX_GROWTH</code>
     * (default: 128 * 1024).<br />
     * The maximum number of pages the file grows at any time.
     */
    public final int pageStoreMaxGrowth = get("PAGE_STORE_MAX_GROWTH",
            128 * 1024);

    /**
     * Database setting <code>PAGE_STORE_INTERNAL_COUNT</code>
     * (default: false).<br />
     * Update the row counts on a node level.
     */
    public final boolean pageStoreInternalCount = get(
            "PAGE_STORE_INTERNAL_COUNT", false);

    /**
     * Database setting <code>PAGE_STORE_TRIM</code> (default: true).<br />
     * Trim the database size when closing.
     */
    public final boolean pageStoreTrim = get("PAGE_STORE_TRIM", true);

    /**
     * Database setting <code>QUERY_CACHE_SIZE</code> (default: 8).<br />
     * The size of the query cache, in number of cached statements. Each session
     * has it's own cache with the given size. The cache is only used if the SQL
     * statement and all parameters match. Only the last returned result per
     * query is cached. The following statement types are cached: SELECT
     * statements are cached (excluding UNION and FOR UPDATE statements), CALL
     * if it returns a single value, DELETE, INSERT, MERGE, UPDATE, and
     * transactional statements such as COMMIT. This works for both statements
     * and prepared statement.
     */
    public final int queryCacheSize = get("QUERY_CACHE_SIZE", 8);

    /**
     * Database setting <code>RECOMPILE_ALWAYS</code> (default: false).<br />
     * Always recompile prepared statements.
     */
    public final boolean recompileAlways = get("RECOMPILE_ALWAYS", false);

    /**
     * Database setting <code>RECONNECT_CHECK_DELAY</code> (default: 200).<br />
     * Check the .lock.db file every this many milliseconds to detect that the
     * database was changed. The process writing to the database must first
     * notify a change in the .lock.db file, then wait twice this many
     * milliseconds before updating the database.
     */
    public final int reconnectCheckDelay = get("RECONNECT_CHECK_DELAY", 200);

    /**
     * Database setting <code>REUSE_SPACE</code> (default: true).<br />
     * If disabled, all changes are appended to the database file, and existing
     * content is never overwritten. This setting has no effect if the database
     * is already open.
     */
    public final boolean reuseSpace = get("REUSE_SPACE", true);

    /**
     * Database setting <code>ROWID</code> (default: true).<br />
     * If set, each table has a pseudo-column _ROWID_.
     */
    public final boolean rowId = get("ROWID", true);

    /**
     * Database setting <code>SELECT_FOR_UPDATE_MVCC</code>
     * (default: true).<br />
     * If set, SELECT .. FOR UPDATE queries lock only the selected rows when
     * using MVCC.
     */
    public final boolean selectForUpdateMvcc = get("SELECT_FOR_UPDATE_MVCC", true);

    /**
     * Database setting <code>SHARE_LINKED_CONNECTIONS</code>
     * (default: true).<br />
     * Linked connections should be shared, that means connections to the same
     * database should be used for all linked tables that connect to the same
     * database.
     */
    public final boolean shareLinkedConnections = get(
            "SHARE_LINKED_CONNECTIONS", true);

    /**
     * Database setting <code>DEFAULT_TABLE_ENGINE</code>
     * (default: null).<br />
     * The default table engine to use for new tables.
     */
    public final String defaultTableEngine = get("DEFAULT_TABLE_ENGINE", null);

    /**
     * Database setting <code>MV_STORE</code>
     * (default: false for version 1.3, true for version 1.4).<br />
     * Use the MVStore storage engine.
     */
    public boolean mvStore = get("MV_STORE", Constants.VERSION_MINOR >= 4);

    /**
     * Database setting <code>COMPRESS</code>
     * (default: false).<br />
     * Compress data when storing.
     */
    public final boolean compressData = get("COMPRESS", false);

    /**
     * Database setting <code>MULTI_THREADED</code>
     * (default: false).<br />
     */
    public final boolean multiThreaded = get("MULTI_THREADED", false);

    /**
     * Database setting <code>STANDARD_DROP_TABLE_RESTRICT</code> (default:
     * false).<br />
     * <code>true</code> if DROP TABLE RESTRICT should fail if there's any
     * foreign key referencing the table to be dropped. <code>false</code> if
     * foreign keys referencing the table to be dropped should be silently
     * dropped as well.
     */
    public final boolean standardDropTableRestrict = get(
            "STANDARD_DROP_TABLE_RESTRICT", false);

    private DbSettings(HashMap<String, String> s) {
        super(s);
        if (s.get("NESTED_JOINS") != null || Utils.getProperty("h2.nestedJoins", null) != null) {
            throw DbException.getUnsupportedException("NESTED_JOINS setting is not available since 1.4.197");
        }
    }

    /**
     * INTERNAL.
     * Get the settings for the given properties (may not be null).
     *
     * @param s the settings
     * @return the settings
     */
    public static DbSettings getInstance(HashMap<String, String> s) {
        return new DbSettings(s);
    }

    /**
     * INTERNAL.
     * Get the default settings. Those must not be modified.
     *
     * @return the settings
     */
    public static DbSettings getDefaultSettings() {
        if (defaultSettings == null) {
            defaultSettings = new DbSettings(new HashMap<String, String>());
        }
        return defaultSettings;
    }

}
