/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery.tcp;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests IP finder cleaner.
 */
public class TcpDiscoveryIpFinderCleanerTest extends GridCommonAbstractTest {
    /** */
    private static final long IP_FINDER_CLEAN_FREQ = 1000;

    /** */
    private static final long NODE_STOPPING_TIMEOUT = 20000;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checks the node stops gracefully even if {@link TcpDiscoveryIpFinder} ignores {@link InterruptedException}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStops() throws Exception {
        CustomIpFinder ipFinder = new CustomIpFinder(true);

        Ignite ignite = Ignition.start(getConfiguration(ipFinder));

        try {
            if (!ipFinder.suspend().await(IP_FINDER_CLEAN_FREQ * 5, TimeUnit.MILLISECONDS))
                fail("Failed to suspend IP finder.");

            if (!stopNodeAsync(ignite).await(NODE_STOPPING_TIMEOUT, TimeUnit.MILLISECONDS))
                fail("Node was not stopped.");
        }
        finally {
            ipFinder.interruptCleanerThread();
        }
    }

    /**
     * @param ipFinder IP finder.
     * @return Grid test configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration getConfiguration(TcpDiscoveryIpFinder ipFinder) throws Exception {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi()
            .setIpFinder(ipFinder)
            .setIpFinderCleanFrequency(IP_FINDER_CLEAN_FREQ);

        return getConfiguration()
            .setDiscoverySpi(discoverySpi);
    }

    /**
     * Stop the node asynchronously.
     *
     * @param node Ignite instance.
     * @return Latch to signal when the node is stopped completely.
     */
    private static CountDownLatch stopNodeAsync(final Ignite node) {
        final CountDownLatch latch = new CountDownLatch(1);

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    node.close();
                }
                finally {
                    latch.countDown();
                }
            }
        });

        return latch;
    }

    /**
     * Custom IP finder.
     */
    private static class CustomIpFinder extends TcpDiscoveryVmIpFinder {
        /** */
        private volatile boolean suspendFinderAndResetInterruptedFlag;

        /** */
        private final CountDownLatch suspended = new CountDownLatch(1);

        /** */
        private volatile Thread cleanerThread;

        /** {@inheritDoc} */
        public CustomIpFinder(boolean shared) {
            super(shared);
        }

        /** {@inheritDoc} */
        @Override public synchronized Collection<InetSocketAddress> getRegisteredAddresses() {
            if (suspendFinderAndResetInterruptedFlag) {
                cleanerThread = Thread.currentThread();

                suspended.countDown();

                try {
                    new CountDownLatch(1).await();
                }
                catch (InterruptedException ignore) {
                    suspendFinderAndResetInterruptedFlag = false;
                }
            }

            return super.getRegisteredAddresses();
        }

        /**
         * Suspend IP finder in {@link CustomIpFinder#getRegisteredAddresses()} method.
         *
         * @return Latch to signal when IP finder is suspended.
         */
        public CountDownLatch suspend() {
            suspendFinderAndResetInterruptedFlag = true;

            return suspended;
        }

        /**
         * Interrupt IP finder cleaner thread.
         */
        public void interruptCleanerThread() {
            if (cleanerThread != null)
                cleanerThread.interrupt();
        }
    }
}
