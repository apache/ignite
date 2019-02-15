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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteCacheMessageWriteTimeoutTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)cfg.getCommunicationSpi();

        // Try provoke connection close on socket writeTimeout.
        commSpi.setSharedMemoryPort(-1);
        commSpi.setMessageQueueLimit(10);
        commSpi.setSocketReceiveBuffer(64);
        commSpi.setSocketSendBuffer(64);
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
    @Test
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
