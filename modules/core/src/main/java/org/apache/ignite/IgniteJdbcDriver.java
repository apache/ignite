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

package org.apache.ignite;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.internal.jdbc.JdbcDriverPropertyInfo;

/**
 * JDBC driver implementation for In-Memory Data Grid.
 * <p>
 * Driver allows to get distributed data from Ignite cache using standard
 * SQL queries and standard JDBC API. It will automatically get only fields that
 * you actually need from objects stored in cache.
 * <h1 class="header">Limitations</h1>
 * Data in Ignite cache is usually distributed across several nodes,
 * so some queries may not work as expected since the query will be sent to each
 * individual node and results will then be collected and returned as JDBC result set.
 * Keep in mind following limitations (not applied if data is queried from one node only,
 * or data is fully co-located or fully replicated on multiple nodes):
 * <ul>
 *     <li>
 *         Joins will work correctly only if joined objects are stored in
 *         collocated mode. Refer to
 *         {@link AffinityKey}
 *         javadoc for more details.
 *     </li>
 *     <li>
 *         Note that if you are connected to local or replicated cache, all data will
 *         be queried only on one node, not depending on what caches participate in
 *         the query (some data from partitioned cache can be lost). And visa versa,
 *         if you are connected to partitioned cache, data from replicated caches
 *         will be duplicated.
 *     </li>
 * </ul>
 * <h1 class="header">SQL Notice</h1>
 * Driver allows to query data from several caches. Cache that driver is connected to is
 * treated as default schema in this case. Other caches can be referenced by their names.
 * <p>
 * Note that cache name is case sensitive and you have to always specify it in quotes.
 * <h1 class="header">Dependencies</h1>
 * JDBC driver is located in main Ignite JAR and depends on all libraries located in
 * {@code IGNITE_HOME/libs} folder. So if you are using JDBC driver in any external tool,
 * you have to add main Ignite JAR will all dependencies to its classpath.
 * <h1 class="header">Configuration</h1>
 *
 * JDBC driver can return two different types of connection: Ignite Java client based connection and
 * Ignite client node based connection. Java client best connection is deprecated and left only for
 * compatibility with previous version, so you should always use Ignite client node based mode.
 * It is also preferable because it has much better performance.
 *
 * The type of returned connection depends on provided JDBC connection URL.
 *
 * <h2 class="header">Configuration of Ignite client node based connection</h2>
 *
 * JDBC connection URL has the following pattern: {@code jdbc:ignite:cfg://[<params>@]<config_url>}.<br>
 *
 * {@code <config_url>} represents any valid URL which points to Ignite configuration file. It is required.<br>
 *
 * {@code <params>} are optional and have the following format: {@code param1=value1:param2=value2:...:paramN=valueN}.<br>
 *
 * The following parameters are supported:
 * <ul>
 *     <li>{@code cache} - cache name. If it is not defined than default cache will be used.</li>
 *     <li>
 *         {@code nodeId} - ID of node where query will be executed.
 *         It can be useful for querying through local caches.
 *         If node with provided ID doesn't exist, exception is thrown.
 *     </li>
 *     <li>
 *         {@code local} - query will be executed only on local node. Use this parameter with {@code nodeId} parameter.
 *         Default value is {@code false}.
 *     </li>
 *     <li>
 *          {@code collocated} - flag that used for optimization purposes. Whenever Ignite executes
 *          a distributed query, it sends sub-queries to individual cluster members.
 *          If you know in advance that the elements of your query selection are collocated
 *          together on the same node, usually based on some <b>affinity-key</b>, Ignite
 *          can make significant performance and network optimizations.
 *          Default value is {@code false}.
 *     </li>
 *     <li>
 *         {@code distributedJoins} - enables support of distributed joins feature. This flag does not make sense in
 *         combination with {@code local} and/or {@code collocated} flags with {@code true} value or in case of querying
 *         of local cache. Default value is {@code false}.
 *     </li>
 *     <li>
 *         {@code enforceJoinOrder} - Sets flag to enforce join order of tables in the query. If set to {@code true}
 *          query optimizer will not reorder tables in join. By default is {@code false}.
 *     </li>
 *     <li>
 *         {@code lazy} - Sets flag to enable lazy query execution.
 *         By default Ignite attempts to fetch the whole query result set to memory and send it to the client.
 *         For small and medium result sets this provides optimal performance and minimize duration of internal
 *         database locks, thus increasing concurrency.
 *
 *         <p> If result set is too big to fit in available memory this could lead to excessive GC pauses and even
 *         OutOfMemoryError. Use this flag as a hint for Ignite to fetch result set lazily, thus minimizing memory
 *         consumption at the cost of moderate performance hit.
 *
 *         <p> Defaults to {@code false}, meaning that the whole result set is fetched to memory eagerly.
 *     </li>
 * </ul>
 * <h1 class="header">Example</h1>
 * <pre name="code" class="java">
 *  // Open JDBC connection.
 * Connection conn = DriverManager.getConnection("jdbc:ignite:cfg//cache=persons@file:///etc/configs/ignite-jdbc.xml");
 *
 * // Query persons' names
 * ResultSet rs = conn.createStatement().executeQuery("select name from Person");
 *
 * while (rs.next()) {
 *     String name = rs.getString(1);
 *
 *     ...
 * }
 *
 * // Query persons with specific age
 * PreparedStatement stmt = conn.prepareStatement("select name, age from Person where age = ?");
 *
 * stmt.setInt(1, 30);
 *
 * ResultSet rs = stmt.executeQuery();
 *
 * while (rs.next()) {
 *     String name = rs.getString("name");
 *     int age = rs.getInt("age");
 *
 *     ...
 * }
 * </pre>
 */
public class IgniteJdbcDriver implements Driver {
    /** Prefix for property names. */
    private static final String PROP_PREFIX = "ignite.jdbc.";

    /** Node ID parameter name. */
    private static final String PARAM_NODE_ID = "nodeId";

    /** Cache parameter name. */
    private static final String PARAM_CACHE = "cache";

    /** Local parameter name. */
    private static final String PARAM_LOCAL = "local";

    /** Collocated parameter name. */
    private static final String PARAM_COLLOCATED = "collocated";

    /** Distributed joins parameter name. */
    private static final String PARAM_DISTRIBUTED_JOINS = "distributedJoins";

    /** Transactions allowed parameter name. */
    private static final String PARAM_TX_ALLOWED = "transactionsAllowed";

    /** DML streaming parameter name. */
    private static final String PARAM_STREAMING = "streaming";

    /** DML streaming auto flush frequency. */
    private static final String PARAM_STREAMING_FLUSH_FREQ = "streamingFlushFrequency";

    /** DML streaming node buffer size. */
    private static final String PARAM_STREAMING_PER_NODE_BUF_SIZE = "streamingPerNodeBufferSize";

    /** DML streaming parallel operations per node. */
    private static final String PARAM_STREAMING_PER_NODE_PAR_OPS = "streamingPerNodeParallelOperations";

    /** Whether DML streaming will overwrite existing cache entries. */
    private static final String PARAM_STREAMING_ALLOW_OVERWRITE = "streamingAllowOverwrite";

    /** Allow queries with multiple statements. */
    private static final String PARAM_MULTIPLE_STMTS = "multipleStatementsAllowed";

    /** Skip reducer on update property name. */
    private static final String PARAM_SKIP_REDUCER_ON_UPDATE = "skipReducerOnUpdate";

    /** Parameter: enforce join order flag (SQL hint). */
    public static final String PARAM_ENFORCE_JOIN_ORDER = "enforceJoinOrder";

    /** Parameter: replicated only flag (SQL hint). */
    public static final String PARAM_LAZY = "lazy";

    /** Parameter: schema name. */
    public static final String PARAM_SCHEMA = "schema";

    /** Hostname property name. */
    public static final String PROP_HOST = PROP_PREFIX + "host";

    /** Port number property name. */
    public static final String PROP_PORT = PROP_PREFIX + "port";

    /** Cache name property name. */
    public static final String PROP_CACHE = PROP_PREFIX + PARAM_CACHE;

    /** Node ID property name. */
    public static final String PROP_NODE_ID = PROP_PREFIX + PARAM_NODE_ID;

    /** Local property name. */
    public static final String PROP_LOCAL = PROP_PREFIX + PARAM_LOCAL;

    /** Collocated property name. */
    public static final String PROP_COLLOCATED = PROP_PREFIX + PARAM_COLLOCATED;

    /** Distributed joins property name. */
    public static final String PROP_DISTRIBUTED_JOINS = PROP_PREFIX + PARAM_DISTRIBUTED_JOINS;

    /** Transactions allowed property name. */
    public static final String PROP_TX_ALLOWED = PROP_PREFIX + PARAM_TX_ALLOWED;

    /** DML streaming property name. */
    public static final String PROP_STREAMING = PROP_PREFIX + PARAM_STREAMING;

    /** DML stream auto flush frequency property name. */
    public static final String PROP_STREAMING_FLUSH_FREQ = PROP_PREFIX + PARAM_STREAMING_FLUSH_FREQ;

    /** DML stream node buffer size property name. */
    public static final String PROP_STREAMING_PER_NODE_BUF_SIZE = PROP_PREFIX + PARAM_STREAMING_PER_NODE_BUF_SIZE;

    /** DML stream parallel operations per node property name. */
    public static final String PROP_STREAMING_PER_NODE_PAR_OPS = PROP_PREFIX + PARAM_STREAMING_PER_NODE_PAR_OPS;

    /** Whether DML streaming will overwrite existing cache entries. */
    public static final String PROP_STREAMING_ALLOW_OVERWRITE = PROP_PREFIX + PARAM_STREAMING_ALLOW_OVERWRITE;

    /** Allow query with multiple statements. */
    public static final String PROP_MULTIPLE_STMTS = PROP_PREFIX + PARAM_MULTIPLE_STMTS;

    /** Skip reducer on update update property name. */
    public static final String PROP_SKIP_REDUCER_ON_UPDATE = PROP_PREFIX + PARAM_SKIP_REDUCER_ON_UPDATE;

    /** Transactions allowed property name. */
    public static final String PROP_ENFORCE_JOIN_ORDER = PROP_PREFIX + PARAM_ENFORCE_JOIN_ORDER;

    /** Lazy property name. */
    public static final String PROP_LAZY = PROP_PREFIX + PARAM_LAZY;

    /** Schema property name. */
    public static final String PROP_SCHEMA = PROP_PREFIX + PARAM_SCHEMA;

    /** Cache name property name. */
    public static final String PROP_CFG = PROP_PREFIX + "cfg";

    /** Config URL prefix. */
    public static final String CFG_URL_PREFIX = "jdbc:ignite:cfg://";

    /** Default port. */
    public static final int DFLT_PORT = 11211;

    /** Major version. */
    private static final int MAJOR_VER = 1;

    /** Minor version. */
    private static final int MINOR_VER = 0;

    /** Logger. */
    private static final Logger LOG = Logger.getLogger(IgniteJdbcDriver.class.getName());

    /*
     * Static initializer.
     */
    static {
        try {
            DriverManager.registerDriver(new IgniteJdbcDriver());
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to register Ignite JDBC driver.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Connection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url))
            return null;

        if (!parseUrl(url, props))
            throw new SQLException("URL is invalid: " + url);

        return new org.apache.ignite.internal.jdbc2.JdbcConnection(url, props);
    }

    /** {@inheritDoc} */
    @Override public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(CFG_URL_PREFIX);
    }

    /** {@inheritDoc} */
    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (!parseUrl(url, info))
            throw new SQLException("URL is invalid: " + url);

        return new DriverPropertyInfo[]{
            new JdbcDriverPropertyInfo("Hostname", info.getProperty(PROP_HOST), ""),
            new JdbcDriverPropertyInfo("Port number", info.getProperty(PROP_PORT), ""),
            new JdbcDriverPropertyInfo("Cache name", info.getProperty(PROP_CACHE), ""),
            new JdbcDriverPropertyInfo("Node ID", info.getProperty(PROP_NODE_ID), ""),
            new JdbcDriverPropertyInfo("Local", info.getProperty(PROP_LOCAL), ""),
            new JdbcDriverPropertyInfo("Collocated", info.getProperty(PROP_COLLOCATED), ""),
            new JdbcDriverPropertyInfo("Distributed Joins", info.getProperty(PROP_DISTRIBUTED_JOINS), ""),
            new JdbcDriverPropertyInfo("Enforce Join Order", info.getProperty(PROP_ENFORCE_JOIN_ORDER), ""),
            new JdbcDriverPropertyInfo("Lazy query execution", info.getProperty(PROP_LAZY), ""),
            new JdbcDriverPropertyInfo("Transactions Allowed", info.getProperty(PROP_TX_ALLOWED), ""),
            new JdbcDriverPropertyInfo("Queries with multiple statements allowed", info.getProperty(PROP_MULTIPLE_STMTS), ""),
            new JdbcDriverPropertyInfo("Skip reducer on update", info.getProperty(PROP_SKIP_REDUCER_ON_UPDATE), ""),
            new JdbcDriverPropertyInfo("Schema name", info.getProperty(PROP_SCHEMA), ""),
            new JdbcDriverPropertyInfo("Configuration path", info.getProperty(PROP_CFG), "")
        };
    }

    /** {@inheritDoc} */
    @Override public int getMajorVersion() {
        return MAJOR_VER;
    }

    /** {@inheritDoc} */
    @Override public int getMinorVersion() {
        return MINOR_VER;
    }

    /** {@inheritDoc} */
    @Override public boolean jdbcCompliant() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging is not used.");
    }

    /**
     * Validates and parses connection URL.
     *
     * @param props Properties.
     * @param url URL.
     * @return Whether URL is valid.
     */
    private boolean parseUrl(String url, Properties props) {
        if (url == null)
            return false;

        if (url.startsWith(CFG_URL_PREFIX) && url.length() >= CFG_URL_PREFIX.length())
            return parseJdbcConfigUrl(url, props);

        return false;
    }

    /**
     * @param url Url.
     * @param props Properties.
     */
    private boolean parseJdbcConfigUrl(String url, Properties props) {
        url = url.substring(CFG_URL_PREFIX.length());

        String[] parts = url.split("@");

        if (parts.length > 2)
            return false;

        if (parts.length == 2) {
            if (!parseParameters(parts[0], ":", props))
                return false;
        }

        props.setProperty(PROP_CFG, parts[parts.length - 1]);

        return true;
    }

    /**
     * Validates and parses URL parameters.
     *
     * @param val Parameters string.
     * @param delim Delimiter.
     * @param props Properties.
     * @return Whether URL parameters string is valid.
     */
    private boolean parseParameters(String val, String delim, Properties props) {
        String[] params = val.split(delim);

        for (String param : params) {
            String[] pair = param.split("=");

            if (pair.length != 2 || pair[0].isEmpty() || pair[1].isEmpty())
                return false;

            props.setProperty(PROP_PREFIX + pair[0], pair[1]);
        }

        return true;
    }
}
