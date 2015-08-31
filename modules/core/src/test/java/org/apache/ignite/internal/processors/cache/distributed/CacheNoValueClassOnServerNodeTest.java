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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 *
 */
public class CacheNoValueClassOnServerNodeTest extends GridCommonAbstractTest {
    /** */
    public static final String NODE_START_MSG = "Test external node started";

    /** */
    private static final String CLIENT_CLS_NAME =
        "org.apache.ignite.tests.p2p.cache.CacheNoValueClassOnServerTestClient";

    /**
     * @return Configuration.
     */
    private IgniteConfiguration createConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setPeerClassLoadingEnabled(false);

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
    public void testNoValueClassOnServerNode() throws Exception {
        // Check class is really not available.
        try {
            Class.forName("org.apache.ignite.tests.p2p.cache.Person");

            fail();
        }
        catch (ClassNotFoundException ignore) {
            // Expected exception.
        }

        try (Ignite ignite = Ignition.start(createConfiguration())) {
            CacheConfiguration cfg = new CacheConfiguration();

            cfg.setCopyOnRead(true); // To store only value bytes.

            ignite.createCache(cfg);

            final CountDownLatch clientReadyLatch = new CountDownLatch(1);

            Collection<String> jvmArgs = Arrays.asList("-ea", "-DIGNITE_QUIET=false");

            GridJavaProcess clientNode = null;

            try {
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
            }
            finally {
                if (clientNode != null)
                    clientNode.killProcess();
            }
        }
    }

}