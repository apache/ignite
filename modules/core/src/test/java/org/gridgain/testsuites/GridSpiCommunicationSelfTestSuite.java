/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.apache.ignite.spi.communication.tcp.*;

/**
 * Test suite for all communication SPIs.
 */
public class GridSpiCommunicationSelfTestSuite extends TestSuite {
    /**
     * @return Communication SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Communication SPI Test Suite");

        suite.addTest(new TestSuite(GridTcpCommunicationSpiTcpSelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiShmemSelfTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiStartStopSelfTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiMultithreadedTcpSelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiMultithreadedShmemTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiConfigSelfTest.class));

        return suite;
    }
}
