/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore.jdbc;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import javax.sql.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.sql.Connection.*;

/**
 * JDBC-based metrics store.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *     <li>Data source (see {@link #setDataSource(DataSource)}).</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * There are no optional configuration parameters.
 * <p>
 * The database will contain 1 table to hold nodes metrics (<tt>tbl_metrics</tt>).
 */
public class GridTcpDiscoveryJdbcMetricsStore extends GridTcpDiscoveryMetricsStoreAdapter {
    /** Query to get metrics. */
    public static final String GET_METRICS_QRY = "select id, metrics from tbl_metrics where id = ?";

    /** Query to remove metrics. */
    public static final String REMOVE_METRICS_QRY = "delete from tbl_metrics where id = ?";

    /** Query to get all node IDs. */
    public static final String GET_ALL_IDS_QRY = "select id from tbl_metrics";

    /** Query to put metrics. */
    public static final String PUT_METRICS_QRY = "insert into tbl_metrics values (?, ?)";

    /** Query to create metrics table. */
    public static final String CREATE_METRICS_TABLE_QRY = "create table if not exists tbl_metrics (" +
        "id VARCHAR(36) UNIQUE, " +
        "metrics BLOB)";

    /** Query to check database validity. */
    public static final String CHK_QRY = "select count(*) from tbl_metrics";

    /** Grid logger. */
    @GridLoggerResource
    private GridLogger log;

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

    /** {@inheritDoc} */
    @Override protected Map<UUID, GridNodeMetrics> metrics0(Collection<UUID> nodeIds) throws GridSpiException {
        assert !F.isEmpty(nodeIds);

        init();

        Connection conn = null;

        PreparedStatement stmt = null;

        ResultSet rs = null;

        try {
            conn = dataSrc.getConnection();

            conn.setTransactionIsolation(TRANSACTION_REPEATABLE_READ);

            Map<UUID, GridNodeMetrics> res = new HashMap<>();

            for (UUID id : nodeIds) {
                stmt = conn.prepareStatement(GET_METRICS_QRY);

                stmt.setString(1, id.toString());
                rs = stmt.executeQuery();

                if (rs.first())
                    res.put(UUID.fromString(rs.getString(1)), GridDiscoveryMetricsHelper.deserialize(
                        rs.getBytes(2), 0));

                rs.close();

                stmt.close();
            }

            return res;
        }
        catch (SQLException e) {
            throw new GridSpiException("Failed to get metrics for nodes: " + nodeIds, e);
        }
        finally {
            U.closeQuiet(rs);
            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override protected void removeMetrics0(Collection<UUID> nodeIds) throws GridSpiException {
        assert !F.isEmpty(nodeIds);

        init();

        Connection conn = null;

        PreparedStatement stmt = null;

        boolean committed = false;

        try {
            conn = dataSrc.getConnection();

            conn.setAutoCommit(false);

            conn.setTransactionIsolation(TRANSACTION_REPEATABLE_READ);

            stmt = conn.prepareStatement(REMOVE_METRICS_QRY);

            for (UUID id : nodeIds) {
                stmt.setString(1, id.toString());

                stmt.addBatch();
            }

            stmt.executeBatch();

            conn.commit();

            committed = true;
        }
        catch (SQLException e) {
            U.rollbackConnectionQuiet(conn);

            throw new GridSpiException("Failed to remove metrics for nodes: " + nodeIds, e);
        }
        finally {
            if (!committed)
                U.rollbackConnectionQuiet(conn);

            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void updateLocalMetrics(UUID locNodeId, GridNodeMetrics metrics) throws GridSpiException {
        assert locNodeId != null;
        assert metrics != null;

        init();

        Connection conn = null;

        PreparedStatement stmt = null;

        boolean committed = false;

        try {
            conn = dataSrc.getConnection();

            conn.setAutoCommit(false);

            conn.setTransactionIsolation(TRANSACTION_REPEATABLE_READ);

            // Remove metrics if present.
            stmt = conn.prepareStatement(REMOVE_METRICS_QRY);

            stmt.setString(1, locNodeId.toString());

            stmt.executeUpdate();

            stmt.close();

            // Put metrics.
            stmt = conn.prepareStatement(PUT_METRICS_QRY);

            stmt.setString(1, locNodeId.toString());

            byte buf[] = new byte[GridDiscoveryMetricsHelper.METRICS_SIZE];

            GridDiscoveryMetricsHelper.serialize(buf, 0, metrics);

            stmt.setBytes(2, buf);

            stmt.executeUpdate();

            conn.commit();

            committed = true;
        }
        catch (SQLException e) {
            U.rollbackConnectionQuiet(conn);

            throw new GridSpiException("Failed to update metrics for node: " + locNodeId, e);
        }
        finally {
            if (!committed)
                U.rollbackConnectionQuiet(conn);

            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> allNodeIds() throws GridSpiException {
        init();

        Connection conn = null;

        PreparedStatement stmt = null;

        ResultSet rs = null;

        try {
            conn = dataSrc.getConnection();

            conn.setTransactionIsolation(TRANSACTION_REPEATABLE_READ);

            stmt = conn.prepareStatement(GET_ALL_IDS_QRY);

            rs = stmt.executeQuery();

            Collection<UUID> res = new LinkedList<>();

            while (rs.next())
                res.add(UUID.fromString(rs.getString(1)));

            return res;
        }
        catch (SQLException e) {
            throw new GridSpiException("Failed to get all node IDs.", e);
        }
        finally {
            U.closeQuiet(rs);
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
    @GridSpiConfiguration(optional = false)
    public void setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;
    }

    /**
     * Flag indicating whether DB schema should be initialized by GridGain (default behaviour) or
     * was explicitly created by user.
     *
     * @param initSchema {@code True} if DB schema should be initialized by GridGain (default behaviour),
     *      {code @false} if schema was explicitly created by user.
     */
    @GridSpiConfiguration(optional = true)
    public void setInitSchema(boolean initSchema) {
        this.initSchema = initSchema;
    }

    /**
     * Checks configuration validity.
     *
     * @throws GridSpiException If any error occurs.
     */
    private void init() throws GridSpiException {
        if (initLatch.getCount() > 0 && initGuard.compareAndSet(false, true)) {
            if (dataSrc == null)
                throw new GridSpiException("Data source is null (you must configure it via setDataSource(..)" +
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

                conn.setTransactionIsolation(TRANSACTION_REPEATABLE_READ);

                // Create tbl_metrics.
                stmt = conn.createStatement();

                stmt.executeUpdate(CREATE_METRICS_TABLE_QRY);

                conn.commit();

                committed = true;

                if (log.isDebugEnabled())
                    log.debug("DB schema has been initialized.");
            }
            catch (SQLException e) {
                U.rollbackConnectionQuiet(conn);

                throw new GridSpiException("Failed to initialize DB schema.", e);
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
     * @throws GridSpiException If schema wasn't properly initialized.
     */
    private void checkSchema() throws GridSpiException {
        try {
            U.await(initLatch);
        }
        catch (GridInterruptedException e) {
            throw new GridSpiException("Thread has been interrupted.", e);
        }

        Connection conn = null;

        Statement stmt = null;

        try {
            conn = dataSrc.getConnection();

            conn.setTransactionIsolation(TRANSACTION_REPEATABLE_READ);

            // Check if tbl_metrics exists and database initialized properly.
            stmt = conn.createStatement();

            stmt.execute(CHK_QRY);
        }
        catch (SQLException e) {
            throw new GridSpiException("Metrics store has not been properly initialized.", e);
        }
        finally {
            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryJdbcMetricsStore.class, this);
    }
}
