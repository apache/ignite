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

package org.apache.ignite.internal.util;

import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.logger.java.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.internal.util.IgniteUtils.*;

/**
 * Checks that node can be started without operations with undefined IGNITE_HOME.
 * <p>
 * Notes:
 * 1. The test intentionally extends JUnit {@link TestCase} class to make the test
 * independent from {@link GridCommonAbstractTest} stuff.
 * 2. Do not replace native Java asserts with JUnit ones - test won't fall on TeamCity.
 */
public class GridStartupWithUndefinedIgniteHomeSelfTest extends TestCase {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_COUNT = 2;

    /** {@inheritDoc} */
    @Override protected void tearDown() throws Exception {
        // Next grid in the same VM shouldn't use cached values produced by these tests.
        nullifyHomeDirectory();

        U.getIgniteHome();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopWithUndefinedIgniteHome() throws Exception {
        IgniteUtils.nullifyHomeDirectory();

        // We can't use U.getIgniteHome() here because
        // it will initialize cached value which is forbidden to override.
        String igniteHome = IgniteSystemProperties.getString(IGNITE_HOME);

        assert igniteHome != null;

        U.setIgniteHome(null);

        String igniteHome0 = U.getIgniteHome();

        assert igniteHome0 == null;

        IgniteLogger log = new JavaLogger();

        log.info(">>> Test started: " + getName());
        log.info("Grid start-stop test count: " + GRID_COUNT);

        for (int i = 0; i < GRID_COUNT; i++) {
            TcpDiscoverySpi disc = new TcpDiscoverySpi();

            disc.setIpFinder(IP_FINDER);

            IgniteConfiguration cfg = new IgniteConfiguration();

            // Default console logger is used
            cfg.setGridLogger(log);
            cfg.setDiscoverySpi(disc);
            cfg.setConnectorConfiguration(null);

            try (Ignite ignite = G.start(cfg)) {
                assert ignite != null;

                igniteHome0 = U.getIgniteHome();

                assert igniteHome0 == null;

                X.println("Stopping grid " + ignite.cluster().localNode().id());
            }
        }
    }
}
