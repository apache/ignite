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

package org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;

/**
 * JDBC-based IP finder.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *     <li>Data source (see {@link #setDataSource(DataSource)}).</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 *     <li>Flag indicating whether DB schema should be initialized by Ignite (default behaviour) or
 *         was explicitly created by user (see {@link #setInitSchema(boolean)})</li>
 * </ul>
 * <p>
 * The database will contain 1 table which will hold IP addresses.
 */
public class TcpDiscoveryJdbcIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** Query to get addresses. */
    public static final String GET_ADDRS_QRY = "select hostname, port from tbl_addrs";

    /** Query to register address. */
    public static final String REG_ADDR_QRY = "insert into tbl_addrs values (?, ?)";

    /** Query to unregister address. */
    public static final String UNREG_ADDR_QRY = "delete from tbl_addrs where hostname = ? and port = ?";

    /** Query to create addresses table. */
    public static final String CREATE_ADDRS_TABLE_QRY =
        "create table if not exists tbl_addrs (" +
        "hostname VARCHAR(1024), " +
        "port INT)";

    /** Query to check database validity. */
    public static final String CHK_QRY = "select count(*) from tbl_addrs";

    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Data source. */
    private DataSource dataSrc;

    /** Flag for schema initialization. */
    private boolean initSchema = true;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /**
     * Constructor.
     */
    public TcpDiscoveryJdbcIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        init();

        Connection conn = null;

        PreparedStatement stmt = null;

        ResultSet rs = null;

        try {
            conn = dataSrc.getConnection();

            conn.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

            stmt = conn.prepareStatement(GET_ADDRS_QRY);

            rs = stmt.executeQuery();

            Collection<InetSocketAddress> addrs = new LinkedList<>();

            while (rs.next())
                addrs.add(new InetSocketAddress(rs.getString(1), rs.getInt(2)));

            return addrs;
        }
        catch (SQLException e) {
            throw new IgniteSpiException("Failed to get registered addresses version.", e);
        }
        finally {
            U.closeQuiet(rs);
            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        init();

        Connection conn = null;

        PreparedStatement stmtUnreg = null;

        PreparedStatement stmtReg = null;

        boolean committed = false;

        try {
            conn = dataSrc.getConnection();

            conn.setAutoCommit(false);

            conn.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

            stmtUnreg = conn.prepareStatement(UNREG_ADDR_QRY);
            stmtReg = conn.prepareStatement(REG_ADDR_QRY);

            for (InetSocketAddress addr : addrs) {
                stmtUnreg.setString(1, addr.getAddress().getHostAddress());
                stmtUnreg.setInt(2, addr.getPort());

                stmtUnreg.addBatch();

                stmtReg.setString(1, addr.getAddress().getHostAddress());
                stmtReg.setInt(2, addr.getPort());

                stmtReg.addBatch();
            }

            stmtUnreg.executeBatch();
            stmtUnreg.close();

            stmtReg.executeBatch();
            stmtReg.close();

            conn.commit();

            committed = true;
        }
        catch (SQLException e) {
            U.rollbackConnectionQuiet(conn);

            throw new IgniteSpiException("Failed to register addresses: " + addrs, e);
        }
        finally {
            if (!committed)
                U.rollbackConnectionQuiet(conn);

            U.closeQuiet(stmtUnreg);
            U.closeQuiet(stmtReg);
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        assert !F.isEmpty(addrs);

        init();

        Connection conn = null;

        PreparedStatement stmt = null;

        boolean committed = false;

        try {
            conn = dataSrc.getConnection();

            conn.setAutoCommit(false);

            conn.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

            stmt = conn.prepareStatement(UNREG_ADDR_QRY);

            for (InetSocketAddress addr : addrs) {
                stmt.setString(1, addr.getAddress().getHostAddress());
                stmt.setInt(2, addr.getPort());

                stmt.addBatch();
            }

            stmt.executeBatch();
            conn.commit();

            committed = true;
        }
        catch (SQLException e) {
            U.rollbackConnectionQuiet(conn);

            throw new IgniteSpiException("Failed to unregister addresses: " + addrs, e);
        }
        finally {
            if (!committed)
                U.rollbackConnectionQuiet(conn);

            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }
    }

    /**
     * Sets data source.
     * <p>
     * Data source should be fully configured and ready-to-use.
     *
     * @param dataSrc Data source.
     */
    @IgniteSpiConfiguration(optional = false)
    public void setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;
    }

    /**
     * Flag indicating whether DB schema should be initialized by Ignite (default behaviour) or
     * was explicitly created by user.
     *
     * @param initSchema {@code True} if DB schema should be initialized by Ignite (default behaviour),
     *      {code @false} if schema was explicitly created by user.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setInitSchema(boolean initSchema) {
        this.initSchema = initSchema;
    }

    /**
     * Checks configuration validity.
     *
     * @throws org.apache.ignite.spi.IgniteSpiException If any error occurs.
     */
    private void init() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true)) {
            if (dataSrc == null)
                throw new IgniteSpiException("Data source is null (you must configure it via setDataSource(..)" +
                    " configuration property)");

            if (!initSchema) {
                initLatch.countDown();

                checkSchema();

                return;
            }

            Connection conn = null;

            Statement stmt = null;

            boolean committed = false;

            try {
                conn = dataSrc.getConnection();

                conn.setAutoCommit(false);

                conn.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

                // Create tbl_addrs.
                stmt = conn.createStatement();

                stmt.executeUpdate(CREATE_ADDRS_TABLE_QRY);

                conn.commit();

                committed = true;

                if (log.isDebugEnabled())
                    log.debug("DB schema has been initialized.");
            }
            catch (SQLException e) {
                U.rollbackConnectionQuiet(conn);

                throw new IgniteSpiException("Failed to initialize DB schema.", e);
            }
            finally {
                if (!committed)
                    U.rollbackConnectionQuiet(conn);

                U.closeQuiet(stmt);
                U.closeQuiet(conn);

                initLatch.countDown();
            }
        }
        else
            checkSchema();
    }

    /**
     * Checks correctness of existing DB schema.
     *
     * @throws org.apache.ignite.spi.IgniteSpiException If schema wasn't properly initialized.
     */
    private void checkSchema() throws IgniteSpiException {
        try {
            U.await(initLatch);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteSpiException("Thread has been interrupted.", e);
        }

        Connection conn = null;

        Statement stmt = null;

        try {
            conn = dataSrc.getConnection();

            conn.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

            // Check if tbl_addrs exists and database initialized properly.
            stmt = conn.createStatement();

            stmt.execute(CHK_QRY);
        }
        catch (SQLException e) {
            throw new IgniteSpiException("IP finder has not been properly initialized.", e);
        }
        finally {
            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryJdbcIpFinder.class, this);
    }
}