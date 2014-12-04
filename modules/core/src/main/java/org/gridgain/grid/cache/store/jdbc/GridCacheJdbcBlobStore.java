/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.sql.*;
import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * {@link GridCacheStore} implementation backed by JDBC. This implementation
 * stores objects in underlying database in {@code BLOB} format.
 * <p>
 * Store will create table {@code ENTRIES} in the database to store data.
 * Table will have {@code key} and {@code val} fields.
 * <p>
 * If custom DDL and DML statements are provided, table and field names have
 * to be consistent for all statements and sequence of parameters have to be
 * preserved.
 * <h2 class="header">Configuration</h2>
 * Sections below describe mandatory and optional configuration settings as well
 * as providing example using Java and Spring XML.
 * <h3>Mandatory</h3>
 * There are no mandatory configuration parameters.
 * <h3>Optional</h3>
 * <ul>
 *     <li>Data source (see {@link #setDataSource(DataSource)}</li>
 *     <li>Connection URL (see {@link #setConnectionUrl(String)})</li>
 *     <li>User name (see {@link #setUser(String)})</li>
 *     <li>Password (see {@link #setPassword(String)})</li>
 *     <li>Create table query (see {@link #setConnectionUrl(String)})</li>
 *     <li>Load entry query (see {@link #setLoadQuery(String)})</li>
 *     <li>Update entry query (see {@link #setUpdateQuery(String)})</li>
 *     <li>Insert entry query (see {@link #setInsertQuery(String)})</li>
 *     <li>Delete entry query (see {@link #setDeleteQuery(String)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 *     ...
 *     GridCacheJdbcBlobStore&lt;String, String&gt; store = new GridCacheJdbcBlobStore&lt;String, String&gt;();
 *     ...
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * <pre name="code" class="xml">
 *     ...
 *     &lt;bean id=&quot;cache.jdbc.store&quot;
 *         class=&quot;org.gridgain.grid.cache.store.jdbc.GridCacheJdbcBlobStore&quot;&gt;
 *         &lt;property name=&quot;connectionUrl&quot; value=&quot;jdbc:h2:mem:&quot;/&gt;
 *         &lt;property name=&quot;createTableQuery&quot;
 *             value=&quot;create table if not exists ENTRIES (key other, val other)&quot;/&gt;
 *     &lt;/bean&gt;
 *     ...
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public class GridCacheJdbcBlobStore<K, V> extends GridCacheStoreAdapter<K, V> {
    /** Default connection URL (value is <tt>jdbc:h2:mem:jdbcCacheStore;DB_CLOSE_DELAY=-1</tt>). */
    public static final String DFLT_CONN_URL = "jdbc:h2:mem:jdbcCacheStore;DB_CLOSE_DELAY=-1";

    /**
     * Default create table query
     * (value is <tt>create table if not exists ENTRIES (key other primary key, val other)</tt>).
     */
    public static final String DFLT_CREATE_TBL_QRY = "create table if not exists ENTRIES " +
        "(key binary primary key, val binary)";

    /** Default load entry query (value is <tt>select * from ENTRIES where key=?</tt>). */
    public static final String DFLT_LOAD_QRY = "select * from ENTRIES where key=?";

    /** Default update entry query (value is <tt>select * from ENTRIES where key=?</tt>). */
    public static final String DFLT_UPDATE_QRY = "update ENTRIES set val=? where key=?";

    /** Default insert entry query (value is <tt>insert into ENTRIES (key, val) values (?, ?)</tt>). */
    public static final String DFLT_INSERT_QRY = "insert into ENTRIES (key, val) values (?, ?)";

    /** Default delete entry query (value is <tt>delete from ENTRIES where key=?</tt>). */
    public static final String DFLT_DEL_QRY = "delete from ENTRIES where key=?";

    /** Connection attribute name. */
    private static final String ATTR_CONN = "JDBC_STORE_CONNECTION";

    /** Connection URL. */
    private String connUrl = DFLT_CONN_URL;

    /** Query to create table. */
    private String createTblQry = DFLT_CREATE_TBL_QRY;

    /** Query to load entry. */
    private String loadQry = DFLT_LOAD_QRY;

    /** Query to update entry. */
    private String updateQry = DFLT_UPDATE_QRY;

    /** Query to insert entries. */
    private String insertQry = DFLT_INSERT_QRY;

    /** Query to delete entries. */
    private String delQry = DFLT_DEL_QRY;

    /** User name for database access. */
    private String user;

    /** Password for database access. */
    @GridToStringExclude
    private String passwd;

    /** Data source. */
    private DataSource dataSrc;

    /** Flag for schema initialization. */
    private boolean initSchema = true;

    /** Log. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** Marshaller. */
    @IgniteMarshallerResource
    private GridMarshaller marsh;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Opened connections. */
    @GridToStringExclude
    private final LongAdder opened = new LongAdder();

    /** Closed connections. */
    @GridToStringExclude
    private final LongAdder closed = new LongAdder();

    /** Test mode flag. */
    @GridToStringExclude
    private boolean testMode;

    /** Successful initialization flag. */
    private boolean initOk;

    /** {@inheritDoc} */
    @Override public void txEnd(GridCacheTx tx, boolean commit) throws GridException {
        init();

        Connection conn = tx.removeMeta(ATTR_CONN);

        if (conn != null) {
            try {
                if (commit)
                    conn.commit();
                else
                    conn.rollback();
            }
            catch (SQLException e) {
                throw new GridException("Failed to end transaction [xid=" + tx.xid() + ", commit=" + commit + ']', e);
            }
            finally {
                closeConnection(conn);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Transaction ended [xid=" + tx.xid() + ", commit=" + commit + ']');
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public V load(@Nullable GridCacheTx tx, K key) throws GridException {
        init();

        if (log.isDebugEnabled())
            log.debug("Store load [key=" + key + ", tx=" + tx + ']');

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection(tx);

            stmt = conn.prepareStatement(loadQry);

            stmt.setObject(1, toBytes(key));

            ResultSet rs = stmt.executeQuery();

            if (rs.next())
                return fromBytes(rs.getBytes(2));
        }
        catch (SQLException e) {
            throw new GridException("Failed to load object: " + key, e);
        }
        finally {
            end(tx, conn, stmt);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, K key, V val) throws GridException {
        init();

        if (log.isDebugEnabled())
            log.debug("Store put [key=" + key + ", val=" + val + ", tx=" + tx + ']');

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection(tx);

            stmt = conn.prepareStatement(updateQry);

            stmt.setObject(1, toBytes(val));
            stmt.setObject(2, toBytes(key));

            if (stmt.executeUpdate() == 0) {
                stmt.close();

                stmt = conn.prepareStatement(insertQry);

                stmt.setObject(1, toBytes(key));
                stmt.setObject(2, toBytes(val));

                stmt.executeUpdate();
            }
        }
        catch (SQLException e) {
            throw new GridException("Failed to put object [key=" + key + ", val=" + val + ']', e);
        }
        finally {
            end(tx, conn, stmt);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, K key) throws GridException {
        init();

        if (log.isDebugEnabled())
            log.debug("Store remove [key=" + key + ", tx=" + tx + ']');

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection(tx);

            stmt = conn.prepareStatement(delQry);

            stmt.setObject(1, toBytes(key));

            stmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new GridException("Failed to remove object: " + key, e);
        }
        finally {
            end(tx, conn, stmt);
        }
    }

    /**
     * @param tx Cache transaction.
     * @return Connection.
     * @throws SQLException In case of error.
     */
    private Connection connection(@Nullable GridCacheTx tx) throws SQLException  {
        if (tx != null) {
            Connection conn = tx.meta(ATTR_CONN);

            if (conn == null) {
                conn = openConnection(false);

                // Store connection in transaction metadata, so it can be accessed
                // for other operations on the same transaction.
                tx.addMeta(ATTR_CONN, conn);
            }

            return conn;
        }
        // Transaction can be null in case of simple load operation.
        else
            return openConnection(true);
    }

    /**
     * Closes allocated resources depending on transaction status.
     *
     * @param tx Active transaction, if any.
     * @param conn Allocated connection.
     * @param st Created statement,
     */
    private void end(@Nullable GridCacheTx tx, Connection conn, Statement st) {
        U.closeQuiet(st);

        if (tx == null)
            // Close connection right away if there is no transaction.
            closeConnection(conn);
    }

    /**
     * Gets connection from a pool.
     *
     * @param autocommit {@code true} If connection should use autocommit mode.
     * @return Pooled connection.
     * @throws SQLException In case of error.
     */
    private Connection openConnection(boolean autocommit) throws SQLException {
        Connection conn = dataSrc != null ? dataSrc.getConnection() :
            DriverManager.getConnection(connUrl, user, passwd);

        if (testMode)
            opened.increment();

        conn.setAutoCommit(autocommit);

        return conn;
    }

    /**
     * Closes connection.
     *
     * @param conn Connection to close.
     */
    private void closeConnection(Connection conn) {
        U.closeQuiet(conn);

        if (testMode)
            closed.increment();
    }

    /**
     * Initializes store.
     *
     * @throws GridException If failed to initialize.
     */
    private void init() throws GridException {
        if (initLatch.getCount() > 0) {
            if (initGuard.compareAndSet(false, true)) {
                if (log.isDebugEnabled())
                    log.debug("Initializing cache store.");

                if (F.isEmpty(connUrl))
                    throw new GridException("Failed to initialize cache store (connection URL is not provided).");

                if (!initSchema) {
                    initLatch.countDown();

                    return;
                }

                if (F.isEmpty(createTblQry))
                    throw new GridException("Failed to initialize cache store (create table query is not provided).");

                Connection conn = null;

                Statement stmt = null;

                try {
                    conn = openConnection(false);

                    stmt = conn.createStatement();

                    stmt.execute(createTblQry);

                    conn.commit();

                    initOk = true;
                }
                catch (SQLException e) {
                    throw new GridException("Failed to create database table.", e);
                }
                finally {
                    U.closeQuiet(stmt);

                    closeConnection(conn);

                    initLatch.countDown();
                }
            }
            else
                U.await(initLatch);
        }

        if (!initOk)
            throw new GridException("Cache store was not properly initialized.");
    }

    /**
     * Flag indicating whether DB schema should be initialized by GridGain (default behaviour) or
     * was explicitly created by user.
     *
     * @param initSchema {@code True} if DB schema should be initialized by GridGain (default behaviour),
     *      {code @false} if schema was explicitly created by user.
     */
    public void setInitSchema(boolean initSchema) {
        this.initSchema = initSchema;
    }

    /**
     * Sets connection URL.
     *
     * @param connUrl Connection URL.
     */
    public void setConnectionUrl(String connUrl) {
        this.connUrl = connUrl;
    }

    /**
     * Sets create table query.
     *
     * @param createTblQry Create table query.
     */
    public void setCreateTableQuery(String createTblQry) {
        this.createTblQry = createTblQry;
    }

    /**
     * Sets load query.
     *
     * @param loadQry Load query
     */
    public void setLoadQuery(String loadQry) {
        this.loadQry = loadQry;
    }

    /**
     * Sets update entry query.
     *
     * @param updateQry Update entry query.
     */
    public void setUpdateQuery(String updateQry) {
        this.updateQry = updateQry;
    }

    /**
     * Sets insert entry query.
     *
     * @param insertQry Insert entry query.
     */
    public void setInsertQuery(String insertQry) {
        this.insertQry = insertQry;
    }

    /**
     * Sets delete entry query.
     *
     * @param delQry Delete entry query.
     */
    public void setDeleteQuery(String delQry) {
        this.delQry = delQry;
    }

    /**
     * Sets user name for database access.
     *
     * @param user User name.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Sets password for database access.
     *
     * @param passwd Password.
     */
    public void setPassword(String passwd) {
        this.passwd = passwd;
    }

    /**
     * Sets data source. Data source should be fully configured and ready-to-use.
     * <p>
     * Note that if data source is provided, all connections will be
     * acquired via this data source. If data source is not provided, a new connection
     * will be created for each store call ({@code connectionUrl},
     * {@code user} and {@code password} parameters will be used).
     *
     * @param dataSrc Data source.
     */
    public void setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheJdbcBlobStore.class, this, "passwd", passwd != null ? "*" : null);
    }

    /**
     * Serialize object to byte array using marshaller.
     *
     * @param obj Object to convert to byte array.
     * @return Byte array.
     * @throws GridException If failed to convert.
     */
    protected byte[] toBytes(Object obj) throws GridException {
        return marsh.marshal(obj);
    }

    /**
     * Deserialize object from byte array using marshaller.
     *
     * @param bytes Bytes to deserialize.
     * @param <X> Result object type.
     * @return Deserialized object.
     * @throws GridException If failed.
     */
    protected <X> X fromBytes(byte[] bytes) throws GridException {
        if (bytes == null || bytes.length == 0)
            return null;

        return marsh.unmarshal(bytes, getClass().getClassLoader());
    }
}
