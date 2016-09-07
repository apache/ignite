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

package org.apache.ignite.cache.store.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

/**
 * {@link CacheStore} implementation backed by JDBC. This implementation
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
 * <p>
 * Use {@link CacheJdbcBlobStoreFactory} factory to pass {@link CacheJdbcBlobStore} to {@link CacheConfiguration}.
 */
public class CacheJdbcBlobStore<K, V> extends CacheStoreAdapter<K, V> {
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

    /** Marshaller. */
    private static final Marshaller marsh = new JdkMarshaller();

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

    /** Auto-injected store session. */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

    /** Log. */
    @LoggerResource
    private IgniteLogger log;

    /** Marshaller. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Opened connections. */
    @GridToStringExclude
    private final LongAdder8 opened = new LongAdder8();

    /** Closed connections. */
    @GridToStringExclude
    private final LongAdder8 closed = new LongAdder8();

    /** Test mode flag. */
    @GridToStringExclude
    private boolean testMode;

    /** Successful initialization flag. */
    private boolean initOk;

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) {
        init();

        Transaction tx = transaction();

        Map<String, Connection> props = session().properties();

        Connection conn = props.remove(ATTR_CONN);

        if (conn != null) {
            try {
                if (commit)
                    conn.commit();
                else
                    conn.rollback();
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to end transaction [xid=" + tx.xid() + ", commit=" + commit + ']', e);
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
    @Override public V load(K key) {
        init();

        Transaction tx = transaction();

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
        catch (IgniteCheckedException | SQLException e) {
            throw new CacheLoaderException("Failed to load object: " + key, e);
        }
        finally {
            end(tx, conn, stmt);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends K, ? extends V> entry) {
        init();

        Transaction tx = transaction();

        K key = entry.getKey();
        V val = entry.getValue();

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
        catch (IgniteCheckedException | SQLException e) {
            throw new CacheWriterException("Failed to put object [key=" + key + ", val=" + val + ']', e);
        }
        finally {
            end(tx, conn, stmt);
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        init();

        Transaction tx = transaction();

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
        catch (IgniteCheckedException | SQLException e) {
            throw new CacheWriterException("Failed to remove object: " + key, e);
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
    private Connection connection(@Nullable Transaction tx) throws SQLException  {
        if (tx != null) {
            Map<String, Connection> props = session().properties();

            Connection conn = props.get(ATTR_CONN);

            if (conn == null) {
                conn = openConnection(false);

                // Store connection in session properties, so it can be accessed
                // for other operations on the same transaction.
                props.put(ATTR_CONN, conn);
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
    private void end(@Nullable Transaction tx, Connection conn, Statement st) {
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
     * @throws IgniteException If failed to initialize.
     */
    private void init() {
        if (initLatch.getCount() > 0) {
            if (initGuard.compareAndSet(false, true)) {
                if (log.isDebugEnabled())
                    log.debug("Initializing cache store.");

                if (F.isEmpty(connUrl))
                    throw new IgniteException("Failed to initialize cache store (connection URL is not provided).");

                if (!initSchema) {
                    initLatch.countDown();

                    return;
                }

                if (F.isEmpty(createTblQry))
                    throw new IgniteException("Failed to initialize cache store (create table query is not provided).");

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
                    throw new IgniteException("Failed to create database table.", e);
                }
                finally {
                    U.closeQuiet(stmt);

                    closeConnection(conn);

                    initLatch.countDown();
                }
            }
            else {
                try {
                    U.await(initLatch);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }

        if (!initOk)
            throw new IgniteException("Cache store was not properly initialized.");
    }

    /**
     * Flag indicating whether DB schema should be initialized by Ignite (default behaviour) or
     * was explicitly created by user.
     *
     * @param initSchema {@code True} if DB schema should be initialized by Ignite (default behaviour),
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
        return S.toString(CacheJdbcBlobStore.class, this, "passwd", passwd != null ? "*" : null);
    }

    /**
     * Serialize object to byte array using marshaller.
     *
     * @param obj Object to convert to byte array.
     * @return Byte array.
     * @throws IgniteCheckedException If failed to convert.
     */
    protected byte[] toBytes(Object obj) throws IgniteCheckedException {
        return marsh.marshal(obj);
    }

    /**
     * Deserialize object from byte array using marshaller.
     *
     * @param bytes Bytes to deserialize.
     * @param <X> Result object type.
     * @return Deserialized object.
     * @throws IgniteCheckedException If failed.
     */
    protected <X> X fromBytes(byte[] bytes) throws IgniteCheckedException {
        if (bytes == null || bytes.length == 0)
            return null;

        return marsh.unmarshal(bytes, getClass().getClassLoader());
    }

    /**
     * @return Current transaction.
     */
    @Nullable private Transaction transaction() {
        CacheStoreSession ses = session();

        return ses != null ? ses.transaction() : null;
    }

    /**
     * @return Store session.
     */
    protected CacheStoreSession session() {
        return ses;
    }
}