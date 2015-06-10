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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public class IgniteCacheManyClientsTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SRVS = 4;

    /** */
    private boolean client;

    /** */
    private boolean clientDiscovery;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (!clientDiscovery)
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyClients() throws Exception {
        manyClientsPutGet();
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyClientsClientDiscovery() throws Exception {
        clientDiscovery = true;

        manyClientsPutGet();
    }

    /**
     * @throws Exception If failed.
     */
    private void manyClientsPutGet() throws Exception {
        client = true;

        final AtomicInteger idx = new AtomicInteger(SRVS);

        final AtomicBoolean stop = new AtomicBoolean();

        final int THREADS = 30;

        final CountDownLatch latch = new CountDownLatch(THREADS);

        try {
            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Ignite ignite = startGrid(idx.getAndIncrement())) {
                        log.info("Started node: " + ignite.name());

                        assertTrue(ignite.configuration().isClientMode());

                        IgniteCache<Object, Object> cache = ignite.cache(null);

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        int iter = 0;

                        Integer key = rnd.nextInt(0, 1000);

                        cache.put(key, iter++);

                        assertNotNull(cache.get(key));

                        latch.countDown();

                        while (!stop.get()) {
                            key = rnd.nextInt(0, 1000);

                            cache.put(key, iter++);

                            assertNotNull(cache.get(key));
                        }

                        log.info("Stopping node: " + ignite.name());
                    }

                    return null;
                }
            }, THREADS, "client-thread");

            latch.await();

            Thread.sleep(10_000);

            log.info("Stop clients.");

            stop.set(true);

            fut.get();
        }
        finally {
            stop.set(true);
        }
    }
}
