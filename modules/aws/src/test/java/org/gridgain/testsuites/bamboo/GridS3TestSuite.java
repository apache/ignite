/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.apache.ignite.spi.checkpoint.s3.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.s3.*;

/**
 * S3 integration tests.
 */
public class GridS3TestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("S3 Integration Test Suite");

        // Checkpoint SPI.
        suite.addTest(new TestSuite(GridS3CheckpointSpiConfigSelfTest.class));
        suite.addTest(new TestSuite(GridS3CheckpointSpiSelfTest.class));
        suite.addTest(new TestSuite(GridS3CheckpointSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridS3CheckpointManagerSelfTest.class));
        suite.addTest(new TestSuite(GridS3SessionCheckpointSelfTest.class));

        // S3 IP finder.
        suite.addTest(new TestSuite(GridTcpDiscoveryS3IpFinderSelfTest.class));

        return suite;
    }

    /**
     * @return Access key.
     */
    public static String getAccessKey() {
        String key = System.getenv("test.amazon.access.key");

        assert key != null : "Environment variable 'test.amazon.access.key' is not set";

        return key;
    }

    /**
     * @return Access key.
     */
    public static String getSecretKey() {
        String key = System.getenv("test.amazon.secret.key");

        assert key != null : "Environment variable 'test.amazon.secret.key' is not set";

        return key;
    }
}
