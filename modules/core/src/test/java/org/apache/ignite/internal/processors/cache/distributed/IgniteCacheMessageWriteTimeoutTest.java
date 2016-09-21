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

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCacheMessageWriteTimeoutTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)cfg.getCommunicationSpi();

        // Try provoke connection close on socket writeTimeout.
        commSpi.setSharedMemoryPort(-1);
        commSpi.setMessageQueueLimit(10);
        commSpi.setSocketReceiveBuffer(40);
        commSpi.setSocketSendBuffer(40);
        commSpi.setSocketWriteTimeout(100);
        commSpi.setUnacknowledgedMessagesBufferSize(1000);
        commSpi.setConnectTimeout(10_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMessageQueueLimit() throws Exception {
        for (int i = 0; i < 3; i++) {
            log.info("Iteration: " + i);

            startGridsMultiThreaded(3);

            IgniteInternalFuture<?> fut1 = startJobThreads(50);

            U.sleep(100);

            IgniteInternalFuture<?> fut2 = startJobThreads(50);

            fut1.get();
            fut2.get();

            stopAllGrids();
        }
    }

    /**
     * @param cnt Threads count.
     * @return Future.
     */
    private IgniteInternalFuture<?> startJobThreads(int cnt) {
        final CyclicBarrier b = new CyclicBarrier(cnt);

        return GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int idx = b.await();

                Ignite node = ignite(idx % 3);

                IgniteCompute comp = node.compute(node.cluster().forRemotes());

                comp.run(new TestJob());

                return null;
            }

        }, cnt, "job-thread");
    }

    /**
     *
     */
    static class TestJob implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            try {
                long stop = System.currentTimeMillis() + 1000;

                while (System.currentTimeMillis() < stop)
                    assertTrue(Math.sqrt(hashCode()) >= 0);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
