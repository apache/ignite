/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.suite;

import junit.framework.*;
import org.gridgain.client.*;
import org.gridgain.client.impl.*;
import org.gridgain.client.integration.*;
import org.gridgain.client.router.*;
import org.gridgain.client.util.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.protocols.tcp.*;

/**
 * Test suite includes all test that concern REST processors.
 */
public class GridClientTestSuite extends TestSuite {
    /**
     * @return Suite that contains all tests for REST.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Gridgain Clients Test Suite");

        suite.addTest(new TestSuite(GridRouterFactorySelfTest.class));

        // Parser standalone test.
        suite.addTest(new TestSuite(GridTcpRestParserSelfTest.class));

        // Test memcache protocol with custom test client.
        suite.addTest(new TestSuite(GridRestMemcacheProtocolSelfTest.class));

        // Test custom binary protocol with test client.
        suite.addTest(new TestSuite(GridRestBinaryProtocolSelfTest.class));

        // Test jetty rest processor
        suite.addTest(new TestSuite(GridJettyRestProcessorSignedSelfTest.class));
        suite.addTest(new TestSuite(GridJettyRestProcessorUnsignedSelfTest.class));

        // Test TCP rest processor with original memcache client.
        suite.addTest(new TestSuite(GridClientMemcachedProtocolSelfTest.class));

        suite.addTest(new TestSuite(GridRestProcessorStartSelfTest.class));

        // Test cache flag conversion.
        suite.addTest(new TestSuite(GridClientCacheFlagsCodecTest.class));

        // Test multi-start.
        suite.addTest(new TestSuite(GridRestProcessorMultiStartSelfTest.class));

        // Test clients.
        suite.addTest(new TestSuite(GridClientDataImplSelfTest.class));
        suite.addTest(new TestSuite(GridClientComputeImplSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpDirectSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslDirectSelfTest.class));

        // Test client with many nodes.
        suite.addTest(new TestSuite(GridClientTcpMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpDirectMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslDirectMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpUnreachableMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientPreferDirectSelfTest.class));

        // Test client with many nodes and in multithreaded scenarios
        suite.addTest(new TestSuite(GridClientTcpMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslMultiThreadedSelfTest.class));

        // Test client authentication.
        suite.addTest(new TestSuite(GridClientTcpSslAuthenticationSelfTest.class));

        suite.addTest(new TestSuite(GridClientTcpConnectivitySelfTest.class));
        suite.addTest(new TestSuite(GridClientReconnectionSelfTest.class));

        // Rest task command handler test.
        suite.addTest(new TestSuite(GridTaskCommandHandlerSelfTest.class));

        // Default cache only test.
        suite.addTest(new TestSuite(GridClientDefaultCacheSelfTest.class));

        suite.addTestSuite(GridClientFutureAdapterSelfTest.class);
        suite.addTestSuite(GridClientPartitionAffinitySelfTest.class);
        suite.addTestSuite(GridClientPropertiesConfigurationSelfTest.class);
        suite.addTestSuite(GridClientConsistentHashSelfTest.class);
        suite.addTestSuite(GridClientJavaHasherSelfTest.class);

        suite.addTestSuite(GridClientByteUtilsTest.class);

        suite.addTest(new TestSuite(GridClientTopologyCacheSelfTest.class));

        // Router tests.
        suite.addTest(new TestSuite(GridTcpRouterSelfTest.class));
        suite.addTest(new TestSuite(GridTcpSslRouterSelfTest.class));
        suite.addTest(new TestSuite(GridTcpRouterMultiNodeSelfTest.class));

        suite.addTest(new TestSuite(GridClientFailedInitSelfTest.class));

        suite.addTest(new TestSuite(GridClientTcpTaskExecutionAfterTopologyRestartSelfTest.class));

        return suite;
    }
}
