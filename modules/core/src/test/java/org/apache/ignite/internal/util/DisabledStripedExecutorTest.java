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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Striped thread pool disabling test.
 */
public class DisabledStripedExecutorTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStripedExecutorService() throws Exception {
        checkStripedExecutorService(-1);

        checkStripedExecutorService(0);

        checkStripedExecutorService(2);
    }

    /**
     * Check that striped pool is disabled if size is not a positive value.
     *
     * @param stripedPoolSize Striped pool size.
     * @throws Exception If failed.
     */
    private void checkStripedExecutorService(int stripedPoolSize) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setStripedPoolSize(stripedPoolSize);

        try (Ignite ignite = startGrid("grid" + stripedPoolSize, cfg)) {
            final GridKernalContext ctx = ((IgniteKernal)ignite).context();

            if (stripedPoolSize < 1) {
                assertTrue("Striped Executor should be disabled",
                    ctx.isStripedExecutorDisabled());

                // getStripedExecutorService should throw AssertionError.
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() {
                        ctx.getStripedExecutorService();

                        return null;
                    }
                }, AssertionError.class, null);
            }
            else {
                assertFalse("Striped Executor should be enabled",
                    ctx.isStripedExecutorDisabled());

                // getStripedExecutorService should not throw AssertionError.
                ctx.getStripedExecutorService();
            }
        }
    }
}
