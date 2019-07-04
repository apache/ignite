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

package org.apache.ignite.logger.log4j2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.logging.log4j.Level;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Grid Log4j2 SPI test.
 */
public class Log4j2LoggerVerboseModeSelfTest {
    /** */
    private static final String LOG_PATH_VERBOSE_TEST = "modules/core/src/test/config/log4j2-verbose-test.xml";

    /** */
    @Before
    public void setUp() {
        Log4J2Logger.cleanup();
    }

    /**
     * Test works fine after other tests. Please do not forget to call Log4J2Logger.cleanup()
     *
     * @throws Exception If failed.
     */
    @Test
    public void testVerboseMode() throws Exception {
        final PrintStream backupSysOut = System.out;
        final PrintStream backupSysErr = System.err;

        final ByteArrayOutputStream testOut = new ByteArrayOutputStream();
        final ByteArrayOutputStream testErr = new ByteArrayOutputStream();

        String consoleOut = "Empty";
        String consoleErr = "Empty";
        String testMsg = "******* Hello Tester! ******* ";

        try {
            System.setOut(new PrintStream(testOut));
            System.setErr(new PrintStream(testErr));

            System.setProperty("IGNITE_QUIET", "false");

            try (Ignite ignite = G.start(getConfiguration("verboseLogGrid", LOG_PATH_VERBOSE_TEST))) {

                ignite.log().error(testMsg + Level.ERROR);
                ignite.log().warning(testMsg + Level.WARN);
                ignite.log().info(testMsg + Level.INFO);
                ignite.log().debug(testMsg + Level.DEBUG);
                ignite.log().trace(testMsg + Level.TRACE);
            }

        }
        finally {
            System.setProperty("IGNITE_QUIET", "true");

            System.setOut(backupSysOut);
            System.setErr(backupSysErr);
        }

        testOut.flush();
        testErr.flush();

        consoleOut = testOut.toString();
        consoleErr = testErr.toString();

        System.out.println("**************** Out Console content ***************");
        System.out.println(consoleOut);

        System.out.println("**************** Err Console content ***************");
        System.out.println(consoleErr);

        assertTrue(consoleOut.contains(testMsg + Level.INFO));
        assertTrue(consoleOut.contains(testMsg + Level.DEBUG));
        assertTrue(consoleOut.contains(testMsg + Level.TRACE));
        assertTrue(consoleOut.contains(testMsg + Level.ERROR));
        assertTrue(consoleOut.contains(testMsg + Level.WARN));

        assertTrue(consoleErr.contains(testMsg + Level.ERROR));
        assertTrue(consoleErr.contains(testMsg + Level.WARN));
        assertTrue(!consoleErr.contains(testMsg + Level.INFO));
        assertTrue(consoleErr.contains(testMsg + Level.DEBUG));
        assertTrue(consoleErr.contains(testMsg + Level.TRACE));
    }

    /**
     * Creates grid configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param logPath Logger configuration path.
     * @return Grid configuration.
     * @throws Exception If error occurred.
     */
    private static IgniteConfiguration getConfiguration(String igniteInstanceName, String logPath)
        throws Exception {
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(false) {{
            setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
        }});

        File xml = GridTestUtils.resolveIgnitePath(LOG_PATH_VERBOSE_TEST);

        Log4J2Logger logger = new Log4J2Logger(xml);

        return new IgniteConfiguration()
            .setIgniteInstanceName(igniteInstanceName)
            .setGridLogger(logger)
            .setConnectorConfiguration(null)
            .setDiscoverySpi(disco);
    }
}
