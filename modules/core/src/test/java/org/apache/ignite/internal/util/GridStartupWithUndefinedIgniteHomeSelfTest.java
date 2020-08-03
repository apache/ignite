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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_HOME;
import static org.apache.ignite.internal.util.IgniteUtils.nullifyHomeDirectory;

/**
 * Checks that node can be started without operations with undefined IGNITE_HOME.
 * <p>
 * Notes:
 * 1. The test is intentionally made  independent from {@link GridCommonAbstractTest} stuff.
 * 2. Do not replace native Java asserts with JUnit ones - test won't fall on TeamCity.
 */
public class GridStartupWithUndefinedIgniteHomeSelfTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_COUNT = 2;

    /** */
    @After
    public void tearDown() {
        // Next grid in the same VM shouldn't use cached values produced by these tests.
        nullifyHomeDirectory();

        U.getIgniteHome();
    }

    /** */
    @Test
    public void testStartStopWithUndefinedIgniteHome() {
        IgniteUtils.nullifyHomeDirectory();

        // We can't use U.getIgniteHome() here because
        // it will initialize cached value which is forbidden to override.
        String igniteHome = IgniteSystemProperties.getString(IGNITE_HOME);

        assert igniteHome != null;

        U.setIgniteHome(null);

        String igniteHome0 = U.getIgniteHome();

        assert igniteHome0 == null;

        IgniteLogger log = new JavaLogger();

        log.info(">>> Test started: start-stop");
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
