/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore.vm;

import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;

/**
 * GridTcpDiscoveryVmMetricsStore test.
 */
public class GridTcpDiscoveryVmMetricsStoreSelfTest
    extends GridTcpDiscoveryMetricsStoreAbstractSelfTest<GridTcpDiscoveryVmMetricsStore> {
    /**
     * Constructor.
     *
     * @throws Exception If failed.
     */
    public GridTcpDiscoveryVmMetricsStoreSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridTcpDiscoveryVmMetricsStore metricsStore() {
        return new GridTcpDiscoveryVmMetricsStore();
    }
}
