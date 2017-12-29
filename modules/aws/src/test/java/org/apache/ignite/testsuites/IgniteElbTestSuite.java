package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.spi.discovery.tcp.ipfinder.elb.TcpDiscoveryElbIpFinderSelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

public class IgniteElbTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new IgniteTestSuite("ELB Integration Test Suite");

        // Cloud Nodes IP finder.
        suite.addTestSuite(TcpDiscoveryElbIpFinderSelfTest.class);

        return suite;
    }
}
