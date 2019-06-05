/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.ResultSet;

/**
 * Constants are fixed values that are used in the whole database code.
 */
public class Constants {

    /**
     * The build date is updated for each public release.
     */
    public static final String BUILD_DATE = "2018-03-18";

    /**
     * The build date of the last stable release.
     */
    public static final String BUILD_DATE_STABLE = "2017-06-10";

    /**
     * The build id is incremented for each public release.
     */
    public static final int BUILD_ID = 197;

    /**
     * The build id of the last stable release.
     */
    public static final int BUILD_ID_STABLE = 196;

    /**
     * Whether this is a snapshot version.
     */
    public static final boolean BUILD_SNAPSHOT = false;

    /**
     * If H2 is compiled to be included in a product, this should be set to
     * a unique vendor id (to distinguish from official releases).
     * Additionally, a version number should be set to distinguish releases.
     * Example: ACME_SVN1651_BUILD3
     */
    public static final String BUILD_VENDOR_AND_VERSION = null;

    /**
     * The TCP protocol version number 8.
     * @since 1.2.143 (2010-09-18)
     */
    public static final int TCP_PROTOCOL_VERSION_8 = 8;

    /**
     * The TCP protocol version number 9.
     * @since 1.3.158 (2011-07-17)
     */
    public static final int TCP_PROTOCOL_VERSION_9 = 9;

    /**
     * The TCP protocol version number 10.
     * @since 1.3.162 (2011-11-26)
     */
    public static final int TCP_PROTOCOL_VERSION_10 = 10;

    /**
     * The TCP protocol version number 11.
     * @since 1.3.163 (2011-12-30)
     */
    public static final int TCP_PROTOCOL_VERSION_11 = 11;

    /**
     * The TCP protocol version number 12.
     * @since 1.3.168 (2012-07-13)
     */
    public static final int TCP_PROTOCOL_VERSION_12 = 12;

    /**
     * The TCP protocol version number 13.
     * @since 1.3.174 (2013-10-19)
     */
    public static final int TCP_PROTOCOL_VERSION_13 = 13;

    /**
     * The TCP protocol version number 14.
     * @since 1.3.176 (2014-04-05)
     */
    public static final int TCP_PROTOCOL_VERSION_14 = 14;

    /**
     * The TCP protocol version number 15.
     * @since 1.4.178 Beta (2014-05-02)
     */
    public static final int TCP_PROTOCOL_VERSION_15 = 15;

    /**
     * The TCP protocol version number 16.
     * @since 1.4.194 (2017-03-10)
     */
    public static final int TCP_PROTOCOL_VERSION_16 = 16;

    /**
     * The TCP protocol version number 17.
     * @since 1.4.197 (TODO)
     */
    public static final int TCP_PROTOCOL_VERSION_17 = 17;

    /**
     * Minimum supported version of TCP protocol.
     */
    public static final int TCP_PROTOCOL_VERSION_MIN_SUPPORTED = TCP_PROTOCOL_VERSION_8;

    /**
     * Maximum supported version of TCP protocol.
     */
    public static final int TCP_PROTOCOL_VERSION_MAX_SUPPORTED = TCP_PROTOCOL_VERSION_17;

    /**
     * The major version of this database.
     */
    public static final int VERSION_MAJOR = 1;

    /**
     * The minor version of this database.
     */
    public static final int VERSION_MINOR = 4;

    /**
     * The lock mode that means no locking is used at all.
     */
    public static final int LOCK_MODE_OFF = 0;

    /**
     * The lock mode that means read locks are acquired, but they are released
     * immediately after the statement is executed.
     */
    public static final int LOCK_MODE_READ_COMMITTED = 3;

    /**
     * The lock mode that means table level locking is used for reads and
     * writes.
     */
    public static final int LOCK_MODE_TABLE = 1;

    /**
     * The lock mode that means table level locking is used for reads and
     * writes. If a table is locked, System.gc is called to close forgotten
     * connections.
     */
    public static final int LOCK_MODE_TABLE_GC = 2;

    /**
     * Constant meaning both numbers and text is allowed in SQL statements.
     */
    public static final int ALLOW_LITERALS_ALL = 2;

    /**
     * Constant meaning no literals are allowed in SQL statements.
     */
    public static final int ALLOW_LITERALS_NONE = 0;

    /**
     * Constant meaning only numbers are allowed in SQL statements (but no
     * texts).
     */
    public static final int ALLOW_LITERALS_NUMBERS = 1;

    /**
     * Whether searching in Blob values should be supported.
     */
    public static final boolean BLOB_SEARCH = false;

    /**
     * The minimum number of entries to keep in the cache.
     */
    public static final int CACHE_MIN_RECORDS = 16;

    /**
     * The default cache size in KB for each GB of RAM.
     */
    public static final int CACHE_SIZE_DEFAULT = 64 * 1024;

    /**
     * The default cache type.
     */
    public static final String CACHE_TYPE_DEFAULT = "LRU";

    /**
     * The value of the cluster setting if clustering is disabled.
     */
    public static final String CLUSTERING_DISABLED = "''";

    /**
     * The value of the cluster setting if clustering is enabled (the actual
     * value is checked later).
     */
    public static final String CLUSTERING_ENABLED = "TRUE";

    /**
     * The database URL used when calling a function if only the column list
     * should be returned.
     */
    public static final String CONN_URL_COLUMNLIST = "jdbc:columnlist:connection";

    /**
     * The database URL used when calling a function if the data should be
     * returned.
     */
    public static final String CONN_URL_INTERNAL = "jdbc:default:connection";

    /**
     * The cost is calculated on rowcount + this offset,
     * to avoid using the wrong or no index if the table
     * contains no rows _currently_ (when preparing the statement)
     */
    public static final int COST_ROW_OFFSET = 1000;

    /**
     * The number of milliseconds after which to check for a deadlock if locking
     * is not successful.
     */
    public static final int DEADLOCK_CHECK = 100;

    /**
     * The default port number of the HTTP server (for the H2 Console).
     * This value is also in the documentation and in the Server javadoc.
     */
    public static final int DEFAULT_HTTP_PORT = 8082;

    /**
     * The default value for the LOCK_MODE setting.
     */
    public static final int DEFAULT_LOCK_MODE = LOCK_MODE_READ_COMMITTED;

    /**
     * The default maximum length of an LOB that is stored with the record
     * itself, and not in a separate place.
     */
    public static final int DEFAULT_MAX_LENGTH_INPLACE_LOB = 256;

    /**
     * The default value for the maximum transaction log size.
     */
    public static final long DEFAULT_MAX_LOG_SIZE = 16 * 1024 * 1024;

    /**
     * The default value for the MAX_MEMORY_UNDO setting.
     */
    public static final int DEFAULT_MAX_MEMORY_UNDO = 50_000;

    /**
     * The default for the setting MAX_OPERATION_MEMORY.
     */
    public static final int DEFAULT_MAX_OPERATION_MEMORY = 100_000;

    /**
     * The default page size to use for new databases.
     */
    public static final int DEFAULT_PAGE_SIZE = 4096;

    /**
     * The default result set concurrency for statements created with
     * Connection.createStatement() or prepareStatement(String sql).
     */
    public static final int DEFAULT_RESULT_SET_CONCURRENCY =
            ResultSet.CONCUR_READ_ONLY;

    /**
     * The default port of the TCP server.
     * This port is also used in the documentation and in the Server javadoc.
     */
    public static final int DEFAULT_TCP_PORT = 9092;

    /**
     * The default delay in milliseconds before the transaction log is written.
     */
    public static final int DEFAULT_WRITE_DELAY = 500;

    /**
     * The password is hashed this many times
     * to slow down dictionary attacks.
     */
    public static final int ENCRYPTION_KEY_HASH_ITERATIONS = 1024;

    /**
     * The block of a file. It is also the encryption block size.
     */
    public static final int FILE_BLOCK_SIZE = 16;

    /**
     * For testing, the lock timeout is smaller than for interactive use cases.
     * This value could be increased to about 5 or 10 seconds.
     */
    public static final int INITIAL_LOCK_TIMEOUT = 2000;

    /**
     * The block size for I/O operations.
     */
    public static final int IO_BUFFER_SIZE = 4 * 1024;

    /**
     * The block size used to compress data in the LZFOutputStream.
     */
    public static final int IO_BUFFER_SIZE_COMPRESS = 128 * 1024;

    /**
     * The number of milliseconds to wait between checking the .lock.db file
     * still exists once a database is locked.
     */
    public static final int LOCK_SLEEP = 1000;

    /**
     * The highest possible parameter index.
     */
    public static final int MAX_PARAMETER_INDEX = 100_000;

    /**
     * The memory needed by a object of class Data
     */
    public static final int MEMORY_DATA = 24;

    /**
     * This value is used to calculate the average memory usage.
     */
    public static final int MEMORY_FACTOR = 64;

    /**
     * The memory needed by a regular object with at least one field.
     */
    // Java 6, 64 bit: 24
    // Java 6, 32 bit: 12
    public static final int MEMORY_OBJECT = 24;

    /**
     * The memory needed by an object of class PageBtree.
     */
    public static final int MEMORY_PAGE_BTREE =
            112 + MEMORY_DATA + 2 * MEMORY_OBJECT;

    /**
     * The memory needed by an object of class PageData.
     */
    public static final int MEMORY_PAGE_DATA =
            144 + MEMORY_DATA + 3 * MEMORY_OBJECT;

    /**
     * The memory needed by an object of class PageDataOverflow.
     */
    public static final int MEMORY_PAGE_DATA_OVERFLOW = 96 + MEMORY_DATA;

    /**
     * The memory needed by a pointer.
     */
    // Java 6, 64 bit: 8
    // Java 6, 32 bit: 4
    public static final int MEMORY_POINTER = 8;

    /**
     * The memory needed by a Row.
     */
    public static final int MEMORY_ROW = 40;

    /**
     * The minimum write delay that causes commits to be delayed.
     */
    public static final int MIN_WRITE_DELAY = 5;

    /**
     * The name prefix used for indexes that are not explicitly named.
     */
    public static final String PREFIX_INDEX = "INDEX_";

    /**
     * The name prefix used for synthetic nested join tables.
     */
    public static final String PREFIX_JOIN = "SYSTEM_JOIN_";

    /**
     * The name prefix used for primary key constraints that are not explicitly
     * named.
     */
    public static final String PREFIX_PRIMARY_KEY = "PRIMARY_KEY_";

    /**
     * Every user belongs to this role.
     */
    public static final String PUBLIC_ROLE_NAME = "PUBLIC";

    /**
     * The number of bytes in random salt that is used to hash passwords.
     */
    public static final int SALT_LEN = 8;

    /**
     * The name of the default schema.
     */
    public static final String SCHEMA_MAIN = "PUBLIC";

    /**
     * The default selectivity (used if the selectivity is not calculated).
     */
    public static final int SELECTIVITY_DEFAULT = 50;

    /**
     * The number of distinct values to keep in memory when running ANALYZE.
     */
    public static final int SELECTIVITY_DISTINCT_COUNT = 10_000;

    /**
     * The default directory name of the server properties file for the H2
     * Console.
     */
    public static final String SERVER_PROPERTIES_DIR = "~";

    /**
     * The name of the server properties file for the H2 Console.
     */
    public static final String SERVER_PROPERTIES_NAME = ".h2.server.properties";

    /**
     * Queries that take longer than this number of milliseconds are written to
     * the trace file with the level info.
     */
    public static final long SLOW_QUERY_LIMIT_MS = 100;

    /**
     * The database URL prefix of this database.
     */
    public static final String START_URL = "jdbc:h2:";

    /**
     * The file name suffix of all database files.
     */
    public static final String SUFFIX_DB_FILE = ".db";

    /**
     * The file name suffix of large object files.
     */
    public static final String SUFFIX_LOB_FILE = ".lob.db";

    /**
     * The suffix of the directory name used if LOB objects are stored in a
     * directory.
     */
    public static final String SUFFIX_LOBS_DIRECTORY = ".lobs.db";

    /**
     * The file name suffix of file lock files that are used to make sure a
     * database is open by only one process at any time.
     */
    public static final String SUFFIX_LOCK_FILE = ".lock.db";

    /**
     * The file name suffix of a H2 version 1.1 database file.
     */
    public static final String SUFFIX_OLD_DATABASE_FILE = ".data.db";

    /**
     * The file name suffix of page files.
     */
    public static final String SUFFIX_PAGE_FILE = ".h2.db";
    /**
     * The file name suffix of a MVStore file.
     */
    public static final String SUFFIX_MV_FILE = ".mv.db";

    /**
     * The file name suffix of a new MVStore file, used when compacting a store.
     */
    public static final String SUFFIX_MV_STORE_NEW_FILE = ".newFile";

    /**
     * The file name suffix of a temporary MVStore file, used when compacting a
     * store.
     */
    public static final String SUFFIX_MV_STORE_TEMP_FILE = ".tempFile";

    /**
     * The file name suffix of temporary files.
     */
    public static final String SUFFIX_TEMP_FILE = ".temp.db";

    /**
     * The file name suffix of trace files.
     */
    public static final String SUFFIX_TRACE_FILE = ".trace.db";

    /**
     * How often we check to see if we need to apply a throttling delay if SET
     * THROTTLE has been used.
     */
    public static final int THROTTLE_DELAY = 50;

    /**
     * The maximum size of an undo log block.
     */
    public static final int UNDO_BLOCK_SIZE = 1024 * 1024;

    /**
     * The database URL format in simplified Backus-Naur form.
     */
    public static final String URL_FORMAT = START_URL +
            "{ {.|mem:}[name] | [file:]fileName | " +
            "{tcp|ssl}:[//]server[:port][,server2[:port]]/name }[;key=value...]";

    /**
     * The package name of user defined classes.
     */
    public static final String USER_PACKAGE = "org.h2.dynamic";

    /**
     * The maximum time in milliseconds to keep the cost of a view.
     * 10000 means 10 seconds.
     */
    public static final int VIEW_COST_CACHE_MAX_AGE = 10_000;

    /**
     * The name of the index cache that is used for temporary view (subqueries
     * used as tables).
     */
    public static final int VIEW_INDEX_CACHE_SIZE = 64;

    /**
     * The maximum number of entries in query statistics.
     */
    public static final int QUERY_STATISTICS_MAX_ENTRIES = 100;

    /**
     * Announced version for PgServer.
     */
    public static final String PG_VERSION = "8.2.23";

    private Constants() {
        // utility class
    }

    /**
     * Get the version of this product, consisting of major version, minor
     * version, and build id.
     *
     * @return the version number
     */
    public static String getVersion() {
        String version = VERSION_MAJOR + "." + VERSION_MINOR + "." + BUILD_ID;
        if (BUILD_VENDOR_AND_VERSION != null) {
            version += "_" + BUILD_VENDOR_AND_VERSION;
        }
        if (BUILD_SNAPSHOT) {
            version += "-SNAPSHOT";
        }
        return version;
    }

    /**
     * Get the last stable version name.
     *
     * @return the version number
     */
    public static Object getVersionStable() {
        return "1.4." + BUILD_ID_STABLE;
    }

    /**
     * Get the complete version number of this database, consisting of
     * the major version, the minor version, the build id, and the build date.
     *
     * @return the complete version
     */
    public static String getFullVersion() {
        return getVersion() + " (" + BUILD_DATE + ")";
    }

}
