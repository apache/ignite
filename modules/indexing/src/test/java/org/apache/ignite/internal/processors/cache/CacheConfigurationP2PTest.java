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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 *
 */
public class CacheConfigurationP2PTest extends GridCommonAbstractTest {
    /** */
    public static final String NODE_START_MSG = "Test external node started";

    /** */
    private static final String CLIENT_CLS_NAME =
        "org.apache.ignite.tests.p2p.startcache.CacheConfigurationP2PTestClient";

    /**
     * @return Configuration.
     */
    static IgniteConfiguration createConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setPeerClassLoadingEnabled(true);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinderCleanFrequency(1000);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheConfigurationP2P() throws Exception {
        fail("Enable when IGNITE-537 is fixed.");

        try (Ignite ignite = Ignition.start(createConfiguration())) {
            final CountDownLatch srvsReadyLatch = new CountDownLatch(2);

            final CountDownLatch clientReadyLatch = new CountDownLatch(1);

            GridJavaProcess node1 = null;
            GridJavaProcess node2 = null;
            GridJavaProcess clientNode = null;

            Collection<String> jvmArgs = Arrays.asList("-ea", "-DIGNITE_QUIET=false");

            try {
                node1 = GridJavaProcess.exec(
                    CacheConfigurationP2PTestServer.class.getName(),
                    null,
                    log,
                    new CI1<String>() {
                        @Override public void apply(String s) {
                            info("Server node1: " + s);

                            if (s.contains(NODE_START_MSG))
                                srvsReadyLatch.countDown();
                        }
                    },
                    null,
                    jvmArgs,
                    null
                );

                node2 = GridJavaProcess.exec(
                    CacheConfigurationP2PTestServer.class.getName(), null,
                    log,
                    new CI1<String>() {
                        @Override public void apply(String s) {
                            info("Server node2: " + s);

                            if (s.contains(NODE_START_MSG))
                                srvsReadyLatch.countDown();
                        }
                    },
                    null,
                    jvmArgs,
                    null
                );

                assertTrue(srvsReadyLatch.await(60, SECONDS));

                String cp = U.getIgniteHome() + "/modules/extdata/p2p/target/classes/";

                clientNode = GridJavaProcess.exec(
                    CLIENT_CLS_NAME, null,
                    log,
                    new CI1<String>() {
                        @Override public void apply(String s) {
                            info("Client node: " + s);

                            if (s.contains(NODE_START_MSG))
                                clientReadyLatch.countDown();
                        }
                    },
                    null,
                    jvmArgs,
                    cp
                );

                assertTrue(clientReadyLatch.await(60, SECONDS));

                int exitCode = clientNode.getProcess().waitFor();

                assertEquals("Unexpected exit code", 0, exitCode);

                node1.killProcess();
                node2.killProcess();

                final DiscoverySpi spi = ignite.configuration().getDiscoverySpi();

                boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        Map p2pLdrs = GridTestUtils.getFieldValue(spi, "p2pLdrs");

                        log.info("p2pLdrs: " + p2pLdrs.size());

                        return p2pLdrs.isEmpty();
                    }
                }, 10_000);

                assertTrue(wait);
            }
            finally {
                if (node1 != null)
                    node1.killProcess();

                if (node2 != null)
                    node2.killProcess();

                if (clientNode != null)
                    clientNode.killProcess();
            }
        }
    }
}
