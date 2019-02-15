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

package org.apache.ignite.internal.managers.communication;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteVariousConnectionNumberTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 6;

    /** */
    private static Random rnd = new Random();

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int connections = rnd.nextInt(10) + 1;

        log.info("Node connections [name=" + igniteInstanceName + ", connections=" + connections + ']');

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setConnectionsPerNode(connections);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setUsePairedConnections(rnd.nextBoolean());
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        long seed = U.currentTimeMillis();

        rnd.setSeed(seed);

        log.info("Random seed: " + seed);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVariousConnectionNumber() throws Exception {
        startGridsMultiThreaded(3);

        client = true;

        startGridsMultiThreaded(3, 3);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(REPLICATED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        ignite(0).createCache(ccfg);

        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            runOperations(5000);

            awaitPartitionMapExchange();

            int idx = ThreadLocalRandom.current().nextInt(NODES);

            Ignite node = ignite(idx);

            client = node.configuration().isClientMode();

            stopGrid(idx);

            startGrid(idx);
        }
    }

    /**
     * @param time Execution time.
     * @throws Exception If failed.
     */
    private void runOperations(final long time) throws Exception {
        final AtomicInteger idx = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignite node = ignite(idx.getAndIncrement() % NODES);

                IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

                long stopTime = U.currentTimeMillis() + time;

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (U.currentTimeMillis() < stopTime) {
                    cache.put(rnd.nextInt(10_000), 0);

                    node.compute().broadcast(new DummyJob());
                }

                return null;
            }
        }, NODES * 10, "test-thread");
    }

    /**
     *
     */
    private static class DummyJob implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            // No-op.
        }
    }
}
