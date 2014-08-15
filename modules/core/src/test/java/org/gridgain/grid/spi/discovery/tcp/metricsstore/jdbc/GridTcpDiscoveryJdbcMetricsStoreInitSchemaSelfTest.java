/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore.jdbc;

import com.mchange.v2.c3p0.*;
import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Tests for init schema flag.
 */
public class GridTcpDiscoveryJdbcMetricsStoreInitSchemaSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testInitSchemaFlag() throws Exception {
        GridTcpDiscoveryJdbcMetricsStore store = new GridTcpDiscoveryJdbcMetricsStore();

        ComboPooledDataSource dataSrc = new ComboPooledDataSource();

        try {
            dataSrc.setDriverClass("org.h2.Driver");
            dataSrc.setJdbcUrl("jdbc:h2:mem:jdbc_metrics_not_initialized_schema");

            store.setDataSource(dataSrc);

            store.setInitSchema(false);

            try {
                store.allNodeIds();

                fail("JDBC metrics store didn't throw expected exception.");
            }
            catch (GridSpiException e) {
                assertTrue(e.getMessage().contains("Metrics store has not been properly initialized."));
            }
        }
        finally {
            dataSrc.close();
        }
    }
}
