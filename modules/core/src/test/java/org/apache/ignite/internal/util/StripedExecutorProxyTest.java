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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Striped thread pool disabling test
 */
public class StripedExecutorProxyTest extends GridCommonAbstractTest {
    /** */
    private static final String STRIPED_THREAD_NAME_PREFIX = "sys-stripe-";

    /** */
    private static final String SYSTEM_THREAD_NAME_PREFIX = "sys-";

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStripedExecutorService() throws Exception {
        checkStripedExecutorService(-1);

        checkStripedExecutorService(0);

        checkStripedExecutorService(1);
    }

    /**
     * Check that compute job will be executed in the system thread pool if striped one is disabled.
     *
     * @param stripedPoolSize Striped pool size.
     * @throws Exception If failed.
     */
    private void checkStripedExecutorService(int stripedPoolSize) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setStripedPoolSize(stripedPoolSize);

        Ignite ignite = startGrid("grid" + stripedPoolSize, cfg);

        GridKernalContext ctx = ((IgniteKernal)ignite).context();

        assertNotNull("StripedExecutorService should not be null.", ctx.getStripedExecutorService());

        final CountDownLatch taskExecuted = new CountDownLatch(1);

        final AtomicReference<String> threadName = new AtomicReference<>("");

        ctx.getStripedExecutorService().execute(new Runnable() {
            @Override public void run() {
                threadName.set(Thread.currentThread().getName());

                taskExecuted.countDown();
            }
        });

        if (!taskExecuted.await(10000, TimeUnit.MILLISECONDS))
            fail("Compute job was not completed.");

        if (stripedPoolSize <= 0) {
            assertFalse("Compute job should not be executed in striped pool",
                threadName.get().startsWith(STRIPED_THREAD_NAME_PREFIX));

            assertTrue("Compute job should be executed in system pool",
                threadName.get().startsWith(SYSTEM_THREAD_NAME_PREFIX));
        }
        else {
            assertTrue("Compute job should be executed in striped pool",
                threadName.get().startsWith(STRIPED_THREAD_NAME_PREFIX));
        }
    }
}
