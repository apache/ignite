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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests message delivery after reconnection.
 */
public abstract class IgniteCacheMessageRecoveryAbstractTest extends GridCommonAbstractTest {
    /** Grid count. */
    public static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSocketWriteTimeout(1000);
        commSpi.setSharedMemoryPort(-1);
        commSpi.setConnectionsPerNode(connectionsPerNode());

        cfg.setCommunicationSpi(commSpi);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setBackups(1);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setNearConfiguration(null);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Value for {@link TcpCommunicationSpi#setConnectionsPerNode(int)}.
     */
    protected int connectionsPerNode() {
        return TcpCommunicationSpi.DFLT_CONN_PER_NODE;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            final IgniteKernal grid = (IgniteKernal)grid(i);

            GridTestUtils.retryAssert(log, 10, 100, new CA() {
                @Override public void apply() {
                    assertTrue(grid.internalCache().context().mvcc().atomicFutures().isEmpty());
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMessageRecovery() throws Exception {
        final Ignite ignite = grid(0);

        final IgniteCache<Object, String> cache = ignite.cache(null);

        Map<Integer, String> map = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            map.put(i, "0");

        cache.putAll(map);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("update-thread");

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int iter = 0;

                while (!stop.get()) {
                    Map<Integer, String> map = new HashMap<>();

                    for (int i = 0; i < 100; i++)
                        map.put(rnd.nextInt(0, 1000), String.valueOf(i));

                    cache.putAll(map);

                    if (++iter % 100 == 0)
                        log.info("Iteration: " + iter);
                }

                return null;
            }
        });

        try {
            boolean closed = false;

            for (int i = 0; i < 30; i++) {
                Thread.sleep(1000);

                Ignite node0 = ignite(ThreadLocalRandom.current().nextInt(0, GRID_CNT));

                log.info("Close sessions for: " + ignite.name());

                closed |= closeSessions(node0);
            }

            assertTrue(closed);
        }
        finally {
            stop.set(true);
        }

        fut.get();
    }

    /**
     * @param ignite Node.
     * @throws Exception If failed.
     * @return {@code True} if closed at least one session.
     */
    static boolean closeSessions(Ignite ignite) throws Exception {
        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)ignite.configuration().getCommunicationSpi();

        Map<UUID, GridCommunicationClient[]> clients = U.field(commSpi, "clients");

        boolean closed = false;

        for (GridCommunicationClient[] clients0 : clients.values()) {
            for (GridCommunicationClient client : clients0) {
                if (client != null) {
                    GridTcpNioCommunicationClient client0 = (GridTcpNioCommunicationClient)client;

                    GridNioSession ses = client0.session();

                    ses.close();

                    closed = true;
                }
            }
        }

        return closed;
    }
}