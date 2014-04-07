/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore.jdbc;

import com.mchange.v2.c3p0.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;

/**
 * JDBC metrics store test.
 */
public class GridTcpDiscoveryJdbcMetricsStoreSelfTest extends
    GridTcpDiscoveryMetricsStoreAbstractSelfTest<GridTcpDiscoveryJdbcMetricsStore> {
    /** */
    private ComboPooledDataSource dataSrc;

    /**
     * Constructor.
     *
     * @throws Exception If failed.
     */
    public GridTcpDiscoveryJdbcMetricsStoreSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridTcpDiscoveryJdbcMetricsStore metricsStore() throws Exception {
        GridTcpDiscoveryJdbcMetricsStore store = new GridTcpDiscoveryJdbcMetricsStore();

        dataSrc = new ComboPooledDataSource();
        dataSrc.setDriverClass("org.h2.Driver");
        dataSrc.setJdbcUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");

        store.setDataSource(dataSrc);

        return store;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        dataSrc.close();
    }
}
