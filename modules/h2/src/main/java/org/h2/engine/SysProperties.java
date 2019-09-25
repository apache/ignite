/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.util.MathUtils;
import org.h2.util.Utils;

/**
 * The constants defined in this class are initialized from system properties.
 * Some system properties are per machine settings, and others are as a last
 * resort and temporary solution to work around a problem in the application or
 * database engine. Also, there are system properties to enable features that
 * are not yet fully tested or that are not backward compatible.
 * <p>
 * System properties can be set when starting the virtual machine:
 * </p>
 *
 * <pre>
 * java -Dh2.baseDir=/temp
 * </pre>
 *
 * They can be set within the application, but this must be done before loading
 * any classes of this database (before loading the JDBC driver):
 *
 * <pre>
 * System.setProperty(&quot;h2.baseDir&quot;, &quot;/temp&quot;);
 * </pre>
 */
public class SysProperties {

    /**
     * INTERNAL
     */
    public static final String H2_SCRIPT_DIRECTORY = "h2.scriptDirectory";

    /**
     * INTERNAL
     */
    public static final String H2_BROWSER = "h2.browser";

    /**
     * System property <code>file.encoding</code> (default: Cp1252).<br />
     * It is usually set by the system and is the default encoding used for the
     * RunScript and CSV tool.
     */
    public static final String FILE_ENCODING =
            Utils.getProperty("file.encoding", "Cp1252");

    /**
     * System property <code>file.separator</code> (default: /).<br />
     * It is usually set by the system, and used to build absolute file names.
     */
    public static final String FILE_SEPARATOR =
            Utils.getProperty("file.separator", "/");

    /**
     * System property <code>line.separator</code> (default: \n).<br />
     * It is usually set by the system, and used by the script and trace tools.
     */
    public static final String LINE_SEPARATOR =
            Utils.getProperty("line.separator", "\n");

    /**
     * System property <code>user.home</code> (empty string if not set).<br />
     * It is usually set by the system, and used as a replacement for ~ in file
     * names.
     */
    public static final String USER_HOME =
            Utils.getProperty("user.home", "");

    /**
     * System property <code>h2.allowedClasses</code> (default: *).<br />
     * Comma separated list of class names or prefixes.
     */
    public static final String ALLOWED_CLASSES =
            Utils.getProperty("h2.allowedClasses", "*");

    /**
     * System property <code>h2.enableAnonymousTLS</code> (default: true).<br />
     * When using TLS connection, the anonymous cipher suites should be enabled.
     */
    public static final boolean ENABLE_ANONYMOUS_TLS =
            Utils.getProperty("h2.enableAnonymousTLS", true);

    /**
     * System property <code>h2.bindAddress</code> (default: null).<br />
     * The bind address to use.
     */
    public static final String BIND_ADDRESS =
            Utils.getProperty("h2.bindAddress", null);

    /**
     * System property <code>h2.check</code> (default: true).<br />
     * Assertions in the database engine.
     */
    //## CHECK ##
    public static final boolean CHECK =
            Utils.getProperty("h2.check", true);
    /*/
    public static final boolean CHECK = false;
    //*/

    /**
     * System property <code>h2.check2</code> (default: false).<br />
     * Additional assertions in the database engine.
     */
    //## CHECK ##
    public static final boolean CHECK2 =
            Utils.getProperty("h2.check2", false);
    /*/
    public static final boolean CHECK2 = false;
    //*/

    /**
     * System property <code>h2.clientTraceDirectory</code> (default:
     * trace.db/).<br />
     * Directory where the trace files of the JDBC client are stored (only for
     * client / server).
     */
    public static final String CLIENT_TRACE_DIRECTORY =
            Utils.getProperty("h2.clientTraceDirectory", "trace.db/");

    /**
     * System property <code>h2.collatorCacheSize</code> (default: 32000).<br />
     * The cache size for collation keys (in elements). Used when a collator has
     * been set for the database.
     */
    public static final int COLLATOR_CACHE_SIZE =
            Utils.getProperty("h2.collatorCacheSize", 32_000);

    /**
     * System property <code>h2.consoleTableIndexes</code>
     * (default: 100).<br />
     * Up to this many tables, the column type and indexes are listed.
     */
    public static final int CONSOLE_MAX_TABLES_LIST_INDEXES =
            Utils.getProperty("h2.consoleTableIndexes", 100);

    /**
     * System property <code>h2.consoleTableColumns</code>
     * (default: 500).<br />
     * Up to this many tables, the column names are listed.
     */
    public static final int CONSOLE_MAX_TABLES_LIST_COLUMNS =
            Utils.getProperty("h2.consoleTableColumns", 500);

    /**
     * System property <code>h2.consoleProcedureColumns</code>
     * (default: 500).<br />
     * Up to this many procedures, the column names are listed.
     */
    public static final int CONSOLE_MAX_PROCEDURES_LIST_COLUMNS =
            Utils.getProperty("h2.consoleProcedureColumns", 300);

    /**
     * System property <code>h2.consoleStream</code> (default: true).<br />
     * H2 Console: stream query results.
     */
    public static final boolean CONSOLE_STREAM =
            Utils.getProperty("h2.consoleStream", true);

    /**
     * System property <code>h2.consoleTimeout</code> (default: 1800000).<br />
     * H2 Console: session timeout in milliseconds. The default is 30 minutes.
     */
    public static final int CONSOLE_TIMEOUT =
            Utils.getProperty("h2.consoleTimeout", 30 * 60 * 1000);

    /**
     * System property <code>h2.dataSourceTraceLevel</code> (default: 1).<br />
     * The trace level of the data source implementation. Default is 1 for
     * error.
     */
    public static final int DATASOURCE_TRACE_LEVEL =
            Utils.getProperty("h2.dataSourceTraceLevel", 1);

    /**
     * System property <code>h2.delayWrongPasswordMin</code>
     * (default: 250).<br />
     * The minimum delay in milliseconds before an exception is thrown for using
     * the wrong user name or password. This slows down brute force attacks. The
     * delay is reset to this value after a successful login. Unsuccessful
     * logins will double the time until DELAY_WRONG_PASSWORD_MAX.
     * To disable the delay, set this system property to 0.
     */
    public static final int DELAY_WRONG_PASSWORD_MIN =
            Utils.getProperty("h2.delayWrongPasswordMin", 250);

    /**
     * System property <code>h2.delayWrongPasswordMax</code>
     * (default: 4000).<br />
     * The maximum delay in milliseconds before an exception is thrown for using
     * the wrong user name or password. This slows down brute force attacks. The
     * delay is reset after a successful login. The value 0 means there is no
     * maximum delay.
     */
    public static final int DELAY_WRONG_PASSWORD_MAX =
            Utils.getProperty("h2.delayWrongPasswordMax", 4000);

    /**
     * System property <code>h2.javaSystemCompiler</code> (default: true).<br />
     * Whether to use the Java system compiler
     * (ToolProvider.getSystemJavaCompiler()) if it is available to compile user
     * defined functions. If disabled or if the system compiler is not
     * available, the com.sun.tools.javac compiler is used if available, and
     * "javac" (as an external process) is used if not.
     */
    public static final boolean JAVA_SYSTEM_COMPILER =
            Utils.getProperty("h2.javaSystemCompiler", true);

    /**
     * System property <code>h2.lobCloseBetweenReads</code>
     * (default: false).<br />
     * Close LOB files between read operations.
     */
    public static boolean lobCloseBetweenReads =
            Utils.getProperty("h2.lobCloseBetweenReads", false);

    /**
     * System property <code>h2.lobFilesPerDirectory</code>
     * (default: 256).<br />
     * Maximum number of LOB files per directory.
     */
    public static final int LOB_FILES_PER_DIRECTORY =
            Utils.getProperty("h2.lobFilesPerDirectory", 256);

    /**
     * System property <code>h2.lobClientMaxSizeMemory</code> (default:
     * 1048576).<br />
     * The maximum size of a LOB object to keep in memory on the client side
     * when using the server mode.
     */
    public static final int LOB_CLIENT_MAX_SIZE_MEMORY =
            Utils.getProperty("h2.lobClientMaxSizeMemory", 1024 * 1024);

    /**
     * System property <code>h2.maxFileRetry</code> (default: 16).<br />
     * Number of times to retry file delete and rename. in Windows, files can't
     * be deleted if they are open. Waiting a bit can help (sometimes the
     * Windows Explorer opens the files for a short time) may help. Sometimes,
     * running garbage collection may close files if the user forgot to call
     * Connection.close() or InputStream.close().
     */
    public static final int MAX_FILE_RETRY =
            Math.max(1, Utils.getProperty("h2.maxFileRetry", 16));

    /**
     * System property <code>h2.maxReconnect</code> (default: 3).<br />
     * The maximum number of tries to reconnect in a row.
     */
    public static final int MAX_RECONNECT =
            Utils.getProperty("h2.maxReconnect", 3);

    /**
     * System property <code>h2.maxMemoryRows</code>
     * (default: 40000 per GB of available RAM).<br />
     * The default maximum number of rows to be kept in memory in a result set.
     */
    public static final int MAX_MEMORY_ROWS =
            getAutoScaledForMemoryProperty("h2.maxMemoryRows", 40_000);

    /**
     * System property <code>h2.maxTraceDataLength</code>
     * (default: 65535).<br />
     * The maximum size of a LOB value that is written as data to the trace
     * system.
     */
    public static final long MAX_TRACE_DATA_LENGTH =
            Utils.getProperty("h2.maxTraceDataLength", 65535);

    /**
     * System property <code>h2.modifyOnWrite</code> (default: false).<br />
     * Only modify the database file when recovery is necessary, or when writing
     * to the database. If disabled, opening the database always writes to the
     * file (except if the database is read-only). When enabled, the serialized
     * file lock is faster.
     */
    public static final boolean MODIFY_ON_WRITE =
            Utils.getProperty("h2.modifyOnWrite", false);

    /**
     * System property <code>h2.nioLoadMapped</code> (default: false).<br />
     * If the mapped buffer should be loaded when the file is opened.
     * This can improve performance.
     */
    public static final boolean NIO_LOAD_MAPPED =
            Utils.getProperty("h2.nioLoadMapped", false);

    /**
     * System property <code>h2.nioCleanerHack</code> (default: false).<br />
     * If enabled, use the reflection hack to un-map the mapped file if
     * possible. If disabled, System.gc() is called in a loop until the object
     * is garbage collected. See also
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
     */
    public static final boolean NIO_CLEANER_HACK =
            Utils.getProperty("h2.nioCleanerHack", false);

    /**
     * System property <code>h2.objectCache</code> (default: true).<br />
     * Cache commonly used values (numbers, strings). There is a shared cache
     * for all values.
     */
    public static final boolean OBJECT_CACHE =
            Utils.getProperty("h2.objectCache", true);

    /**
     * System property <code>h2.objectCacheMaxPerElementSize</code> (default:
     * 4096).<br />
     * The maximum size (precision) of an object in the cache.
     */
    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE =
            Utils.getProperty("h2.objectCacheMaxPerElementSize", 4096);

    /**
     * System property <code>h2.objectCacheSize</code> (default: 1024).<br />
     * The maximum number of objects in the cache.
     * This value must be a power of 2.
     */
    public static final int OBJECT_CACHE_SIZE;
    static {
        try {
            OBJECT_CACHE_SIZE = MathUtils.nextPowerOf2(
                    Utils.getProperty("h2.objectCacheSize", 1024));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Invalid h2.objectCacheSize", e);
        }
    }

    /**
     * System property <code>h2.oldStyleOuterJoin</code>
     * (default: true for version 1.3, false for version 1.4).<br />
     * Limited support for the old-style Oracle outer join with "(+)".
     */
    public static final boolean OLD_STYLE_OUTER_JOIN =
            Utils.getProperty("h2.oldStyleOuterJoin",
                    Constants.VERSION_MINOR < 4);

    /**
     * System property {@code h2.oldResultSetGetObject}, {@code true} by default.
     * Return {@code Byte} and {@code Short} instead of {@code Integer} from
     * {@code ResultSet#getObject(...)} for {@code TINYINT} and {@code SMALLINT}
     * values.
     */
    public static final boolean OLD_RESULT_SET_GET_OBJECT =
            Utils.getProperty("h2.oldResultSetGetObject", true);

    /**
     * System property {@code h2.bigDecimalIsDecimal}, {@code true} by default. If
     * {@code true} map {@code BigDecimal} to {@code DECIMAL} type, if {@code false}
     * map it to {@code NUMERIC} as specified in JDBC specification (see Mapping
     * from Java Object Types to JDBC Types).
     */
    public static final boolean BIG_DECIMAL_IS_DECIMAL =
            Utils.getProperty("h2.bigDecimalIsDecimal", true);


    /**
     * System property {@code h2.unlimitedTimeRange}, {@code false} by default.
     *
     * <p>
     * Controls limits of TIME data type.
     * </p>
     *
     * <table>
     * <thead>
     * <tr>
     * <th>h2.unlimitedTimeRange</th>
     * <th>Minimum TIME value</th>
     * <th>Maximum TIME value</th>
     * </tr>
     * </thead>
     * <tbody>
     * <tr>
     * <td>false</td>
     * <td>00:00:00.000000000</td>
     * <td>23:59:59.999999999</td>
     * </tr>
     * <tr>
     * <td>true</td>
     * <td>-2562047:47:16.854775808</td>
     * <td>2562047:47:16.854775807</td>
     * </tr>
     * </tbody>
     * </table>
     */
    public static final boolean UNLIMITED_TIME_RANGE =
            Utils.getProperty("h2.unlimitedTimeRange", false);

    /**
     * System property <code>h2.pgClientEncoding</code> (default: UTF-8).<br />
     * Default client encoding for PG server. It is used if the client does not
     * sends his encoding.
     */
    public static final String PG_DEFAULT_CLIENT_ENCODING =
            Utils.getProperty("h2.pgClientEncoding", "UTF-8");

    /**
     * System property <code>h2.prefixTempFile</code> (default: h2.temp).<br />
     * The prefix for temporary files in the temp directory.
     */
    public static final String PREFIX_TEMP_FILE =
            Utils.getProperty("h2.prefixTempFile", "h2.temp");

    /**
     * System property <code>h2.serverCachedObjects</code> (default: 64).<br />
     * TCP Server: number of cached objects per session.
     */
    public static final int SERVER_CACHED_OBJECTS =
            Utils.getProperty("h2.serverCachedObjects", 64);

    /**
     * System property <code>h2.serverResultSetFetchSize</code>
     * (default: 100).<br />
     * The default result set fetch size when using the server mode.
     */
    public static final int SERVER_RESULT_SET_FETCH_SIZE =
            Utils.getProperty("h2.serverResultSetFetchSize", 100);

    /**
     * System property <code>h2.socketConnectRetry</code> (default: 16).<br />
     * The number of times to retry opening a socket. Windows sometimes fails
     * to open a socket, see bug
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6213296
     */
    public static final int SOCKET_CONNECT_RETRY =
            Utils.getProperty("h2.socketConnectRetry", 16);

    /**
     * System property <code>h2.socketConnectTimeout</code>
     * (default: 2000).<br />
     * The timeout in milliseconds to connect to a server.
     */
    public static final int SOCKET_CONNECT_TIMEOUT =
            Utils.getProperty("h2.socketConnectTimeout", 2000);

    /**
     * System property <code>h2.sortBinaryUnsigned</code>
     * (default: false with version 1.3, true with version 1.4).<br />
     * Whether binary data should be sorted in unsigned mode
     * (0xff is larger than 0x00).
     */
    public static final boolean SORT_BINARY_UNSIGNED =
            Utils.getProperty("h2.sortBinaryUnsigned",
                    Constants.VERSION_MINOR >= 4);

    /**
     * System property <code>h2.sortNullsHigh</code> (default: false).<br />
     * Invert the default sorting behavior for NULL, such that NULL
     * is at the end of a result set in an ascending sort and at
     * the beginning of a result set in a descending sort.
     */
    public static final boolean SORT_NULLS_HIGH =
            Utils.getProperty("h2.sortNullsHigh", false);

    /**
     * System property <code>h2.splitFileSizeShift</code> (default: 30).<br />
     * The maximum file size of a split file is 1L &lt;&lt; x.
     */
    public static final long SPLIT_FILE_SIZE_SHIFT =
            Utils.getProperty("h2.splitFileSizeShift", 30);

    /**
     * System property <code>h2.syncMethod</code> (default: sync).<br />
     * What method to call when closing the database, on checkpoint, and on
     * CHECKPOINT SYNC. The following options are supported:
     * "sync" (default): RandomAccessFile.getFD().sync();
     * "force": RandomAccessFile.getChannel().force(true);
     * "forceFalse": RandomAccessFile.getChannel().force(false);
     * "": do not call a method (fast but there is a risk of data loss
     * on power failure).
     */
    public static final String SYNC_METHOD =
            Utils.getProperty("h2.syncMethod", "sync");

    /**
     * System property <code>h2.traceIO</code> (default: false).<br />
     * Trace all I/O operations.
     */
    public static final boolean TRACE_IO =
            Utils.getProperty("h2.traceIO", false);

    /**
     * System property <code>h2.threadDeadlockDetector</code>
     * (default: false).<br />
     * Detect thread deadlocks in a background thread.
     */
    public static final boolean THREAD_DEADLOCK_DETECTOR =
            Utils.getProperty("h2.threadDeadlockDetector", false);

    /**
     * System property <code>h2.implicitRelativePath</code>
     * (default: true for version 1.3, false for version 1.4).<br />
     * If disabled, relative paths in database URLs need to be written as
     * jdbc:h2:./test instead of jdbc:h2:test.
     */
    public static final boolean IMPLICIT_RELATIVE_PATH =
            Utils.getProperty("h2.implicitRelativePath",
                    Constants.VERSION_MINOR < 4);

    /**
     * System property <code>h2.urlMap</code> (default: null).<br />
     * A properties file that contains a mapping between database URLs. New
     * connections are written into the file. An empty value in the map means no
     * redirection is used for the given URL.
     */
    public static final String URL_MAP =
            Utils.getProperty("h2.urlMap", null);

    /**
     * System property <code>h2.useThreadContextClassLoader</code>
     * (default: false).<br />
     * Instead of using the default class loader when deserializing objects, the
     * current thread-context class loader will be used.
     */
    public static final boolean USE_THREAD_CONTEXT_CLASS_LOADER =
        Utils.getProperty("h2.useThreadContextClassLoader", false);

    /**
     * System property <code>h2.serializeJavaObject</code>
     * (default: true).<br />
     * <b>If true</b>, values of type OTHER will be stored in serialized form
     * and have the semantics of binary data for all operations (such as sorting
     * and conversion to string).
     * <br />
     * <b>If false</b>, the objects will be serialized only for I/O operations
     * and a few other special cases (for example when someone tries to get the
     * value in binary form or when comparing objects that are not comparable
     * otherwise).
     * <br />
     * If the object implements the Comparable interface, the method compareTo
     * will be used for sorting (but only if objects being compared have a
     * common comparable super type). Otherwise the objects will be compared by
     * type, and if they are the same by hashCode, and if the hash codes are
     * equal, but objects are not, the serialized forms (the byte arrays) are
     * compared.
     * <br />
     * The string representation of the values use the toString method of
     * object.
     * <br />
     * In client-server mode, the server must have all required classes in the
     * class path. On the client side, this setting is required to be disabled
     * as well, to have correct string representation and display size.
     * <br />
     * In embedded mode, no data copying occurs, so the user has to make
     * defensive copy himself before storing, or ensure that the value object is
     * immutable.
     */
    public static boolean serializeJavaObject =
            Utils.getProperty("h2.serializeJavaObject", true);

    /**
     * System property <code>h2.javaObjectSerializer</code>
     * (default: null).<br />
     * The JavaObjectSerializer class name for java objects being stored in
     * column of type OTHER. It must be the same on client and server to work
     * correctly.
     */
    public static final String JAVA_OBJECT_SERIALIZER =
            Utils.getProperty("h2.javaObjectSerializer", null);

    /**
     * System property <code>h2.customDataTypesHandler</code>
     * (default: null).<br />
     * The CustomDataTypesHandler class name that is used
     * to provide support for user defined custom data types.
     * It must be the same on client and server to work correctly.
     */
    public static final String CUSTOM_DATA_TYPES_HANDLER =
            Utils.getProperty("h2.customDataTypesHandler", null);

    private static final String H2_BASE_DIR = "h2.baseDir";

    private SysProperties() {
        // utility class
    }

    /**
     * INTERNAL
     */
    public static void setBaseDir(String dir) {
        if (!dir.endsWith("/")) {
            dir += "/";
        }
        System.setProperty(H2_BASE_DIR, dir);
    }

    /**
     * INTERNAL
     */
    public static String getBaseDir() {
        return Utils.getProperty(H2_BASE_DIR, null);
    }

    /**
     * System property <code>h2.scriptDirectory</code> (default: empty
     * string).<br />
     * Relative or absolute directory where the script files are stored to or
     * read from.
     *
     * @return the current value
     */
    public static String getScriptDirectory() {
        return Utils.getProperty(H2_SCRIPT_DIRECTORY, "");
    }

    /**
     * This method attempts to auto-scale some of our properties to take
     * advantage of more powerful machines out of the box. We assume that our
     * default properties are set correctly for approx. 1G of memory, and scale
     * them up if we have more.
     */
    private static int getAutoScaledForMemoryProperty(String key, int defaultValue) {
        String s = Utils.getProperty(key, null);
        if (s != null) {
            try {
                return Integer.decode(s).intValue();
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return Utils.scaleForAvailableMemory(defaultValue);
    }

}
