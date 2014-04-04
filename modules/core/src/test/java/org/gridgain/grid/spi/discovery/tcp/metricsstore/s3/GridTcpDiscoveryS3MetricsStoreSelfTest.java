/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore.s3;

import com.amazonaws.auth.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;
import org.gridgain.testframework.config.*;

import java.net.*;

/**
 * GridTcpDiscoveryS3MetricsStore test.
 */
public class GridTcpDiscoveryS3MetricsStoreSelfTest extends
    GridTcpDiscoveryMetricsStoreAbstractSelfTest<GridTcpDiscoveryS3MetricsStore> {
    /**
     * Constructor.
     *
     * @throws Exception If failed.
     */
    public GridTcpDiscoveryS3MetricsStoreSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testStoreMultiThreaded() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridTcpDiscoveryS3MetricsStore metricsStore() throws Exception {
        GridTcpDiscoveryS3MetricsStore store = new GridTcpDiscoveryS3MetricsStore();

        store.setAwsCredentials(new BasicAWSCredentials(GridTestProperties.getProperty("amazon.access.key"),
            GridTestProperties.getProperty("amazon.secret.key")));

        // Bucket name should be unique for the host to parallel test run on one bucket.
        store.setBucketName("metrics-store-test-bucket-" + InetAddress.getLocalHost().getAddress()[3]);

        return store;
    }
}
