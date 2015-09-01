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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

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

        cfg.setConnectorConfiguration(null);
        cfg.setPeerClassLoadingEnabled(false);
        cfg.setTimeServerPortRange(200);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setLocalPortRange(200);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinderCleanFrequency(10 * 60_000);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(2 * 60_000);

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

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyClients() throws Throwable {
        manyClientsPutGet();
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyClientsClientDiscovery() throws Throwable {
        clientDiscovery = true;

        manyClientsPutGet();
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyClientsSequentiallyClientDiscovery() throws Exception {
        clientDiscovery = true;

        manyClientsSequentially();
    }

    /**
     * @throws Exception If failed.
     */
    private void manyClientsSequentially() throws Exception {
        client = true;

        List<Ignite> clients = new ArrayList<>();

        final int CLIENTS = 50;

        int idx = SRVS;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < CLIENTS; i++) {
            Ignite ignite = startGrid(idx++);

            log.info("Started node: " + ignite.name());

            assertTrue(ignite.configuration().isClientMode());

            clients.add(ignite);

            IgniteCache<Object, Object> cache = ignite.cache(null);

            Integer key = rnd.nextInt(0, 1000);

            cache.put(key, i);

            assertNotNull(cache.get(key));
        }

        log.info("All clients started.");

        try {
            checkNodes(SRVS + CLIENTS);
        }
        finally {
            for (Ignite client : clients)
                client.close();
        }
    }

    /**
     * @param expCnt Expected number of nodes.
     */
    private void checkNodes(int expCnt) {
        assertEquals(expCnt, G.allGrids().size());

        long topVer = -1L;

        for (Ignite ignite : G.allGrids()) {
            log.info("Check node: " + ignite.name());

            if (topVer == -1L)
                topVer = ignite.cluster().topologyVersion();
            else
                assertEquals("Unexpected topology version for node: " + ignite.name(),
                    topVer,
                    ignite.cluster().topologyVersion());

            assertEquals("Unexpected number of nodes for node: " + ignite.name(),
                expCnt,
                ignite.cluster().nodes().size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void manyClientsPutGet() throws Throwable {
        client = true;

        final AtomicInteger idx = new AtomicInteger(SRVS);

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicReference<Throwable> err = new AtomicReference<>();

        final int THREADS = 50;

        final CountDownLatch latch = new CountDownLatch(THREADS);

        try {
            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    boolean counted = false;

                    try {
                        int nodeIdx = idx.getAndIncrement();

                        Thread.currentThread().setName("client-thread-node-" + nodeIdx);

                        try (Ignite ignite = startGrid(nodeIdx)) {
                            log.info("Started node: " + ignite.name());

                            assertTrue(ignite.configuration().isClientMode());

                            IgniteCache<Object, Object> cache = ignite.cache(null);

                            ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            int iter = 0;

                            Integer key = rnd.nextInt(0, 1000);

                            cache.put(key, iter++);

                            assertNotNull(cache.get(key));

                            latch.countDown();

                            counted = true;

                            while (!stop.get() && err.get() == null) {
                                key = rnd.nextInt(0, 1000);

                                cache.put(key, iter++);

                                assertNotNull(cache.get(key));

                                Thread.sleep(1);
                            }

                            log.info("Stopping node: " + ignite.name());
                        }

                        return null;
                    }
                    catch (Throwable e) {
                        err.compareAndSet(null, e);

                        log.error("Unexpected error in client thread: " + e, e);

                        throw e;
                    }
                    finally {
                        if (!counted)
                            latch.countDown();
                    }
                }
            }, THREADS, "client-thread");

            assertTrue(latch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

            log.info("All clients started.");

            Thread.sleep(10_000);

            Throwable err0 = err.get();

            if (err0 != null)
                throw err0;

            boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    try {
                        checkNodes(SRVS + THREADS);

                        return true;
                    }
                    catch (AssertionFailedError e) {
                        log.info("Check failed, will retry: " + e);
                    }

                    return false;
                }
            }, 10_000);

            if (!wait)
                checkNodes(SRVS + THREADS);

            log.info("Stop clients.");

            stop.set(true);

            fut.get();
        }
        catch (Throwable e) {
            log.error("Unexpected error: " + e, e);

            throw e;
        }
        finally {
            stop.set(true);
        }
    }
}