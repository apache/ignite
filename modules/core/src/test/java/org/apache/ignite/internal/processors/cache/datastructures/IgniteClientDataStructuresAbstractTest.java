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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public abstract class IgniteClientDataStructuresAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODE_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.equals(getTestGridName(NODE_CNT - 1))) {
            cfg.setClientMode(true);

            if (!clientDiscovery())
                ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @return {@code True} if use client discovery.
     */
    protected abstract boolean clientDiscovery();

    /**
     * @throws Exception If failed.
     */
    public void testSequence() throws Exception {
        Ignite clientNode = clientIgnite();
        Ignite srvNode = serverNode();

        testSequence(clientNode, srvNode);
        testSequence(srvNode, clientNode);
    }

    /**
     * @param creator Creator node.
     * @param other Other node.
     * @throws Exception If failed.
     */
    private void testSequence(Ignite creator, Ignite other) throws Exception {
        assertNull(creator.atomicSequence("seq1", 1L, false));
        assertNull(other.atomicSequence("seq1", 1L, false));

        try (IgniteAtomicSequence seq = creator.atomicSequence("seq1", 1L, true)) {
            assertNotNull(seq);

            assertEquals(1L, seq.get());

            assertEquals(1L, seq.getAndAdd(1));

            assertEquals(2L, seq.get());

            IgniteAtomicSequence seq0 = other.atomicSequence("seq1", 1L, false);

            assertNotNull(seq0);
        }

        assertNull(creator.atomicSequence("seq1", 1L, false));
        assertNull(other.atomicSequence("seq1", 1L, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLong() throws Exception {
        Ignite clientNode = clientIgnite();
        Ignite srvNode = serverNode();

        testAtomicLong(clientNode, srvNode);
        testAtomicLong(srvNode, clientNode);
    }

    /**
     * @param creator Creator node.
     * @param other Other node.
     * @throws Exception If failed.
     */
    private void testAtomicLong(Ignite creator, Ignite other) throws Exception {
        assertNull(creator.atomicLong("long1", 1L, false));
        assertNull(other.atomicLong("long1", 1L, false));

        try (IgniteAtomicLong cntr = creator.atomicLong("long1", 1L, true)) {
            assertNotNull(cntr);

            assertEquals(1L, cntr.get());

            assertEquals(1L, cntr.getAndAdd(1));

            assertEquals(2L, cntr.get());

            IgniteAtomicLong cntr0 = other.atomicLong("long1", 1L, false);

            assertNotNull(cntr0);

            assertEquals(2L, cntr0.get());

            assertEquals(3L, cntr0.incrementAndGet());

            assertEquals(3L, cntr.get());
        }

        assertNull(creator.atomicLong("long1", 1L, false));
        assertNull(other.atomicLong("long1", 1L, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSet() throws Exception {
        Ignite clientNode = clientIgnite();
        Ignite srvNode = serverNode();

        testSet(clientNode, srvNode);
        testSet(srvNode, clientNode);
    }

    /**
     * @param creator Creator node.
     * @param other Other node.
     * @throws Exception If failed.
     */
    private void testSet(Ignite creator, Ignite other) throws Exception {
        assertNull(creator.set("set1", null));
        assertNull(other.set("set1", null));

        CollectionConfiguration colCfg = new CollectionConfiguration();

        try (IgniteSet<Integer> set = creator.set("set1", colCfg)) {
            assertNotNull(set);

            assertEquals(0, set.size());

            assertFalse(set.contains(1));

            assertTrue(set.add(1));

            assertTrue(set.contains(1));

            IgniteSet<Integer> set0 = other.set("set1", null);

            assertTrue(set0.contains(1));

            assertEquals(1, set0.size());

            assertTrue(set0.remove(1));

            assertFalse(set.contains(1));
        }

        assertNull(creator.set("set1", null));
        assertNull(other.set("set1", null));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatch() throws Exception {
        Ignite clientNode = clientIgnite();
        Ignite srvNode = serverNode();

        testLatch(clientNode, srvNode);
        testLatch(srvNode, clientNode);
    }

    /**
     * @param creator Creator node.
     * @param other Other node.
     * @throws Exception If failed.
     */
    private void testLatch(Ignite creator, final Ignite other) throws Exception {
        assertNull(creator.countDownLatch("latch1", 1, true, false));
        assertNull(other.countDownLatch("latch1", 1, true, false));

        try (IgniteCountDownLatch latch = creator.countDownLatch("latch1", 1, true, true)) {
            assertNotNull(latch);

            assertEquals(1, latch.count());

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    U.sleep(1000);

                    IgniteCountDownLatch latch0 = other.countDownLatch("latch1", 1, true, false);

                    assertEquals(1, latch0.count());

                    log.info("Count down latch.");

                    latch0.countDown();

                    assertEquals(0, latch0.count());

                    return null;
                }
            });

            log.info("Await latch.");

            assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));

            log.info("Finished wait.");

            fut.get();
        }

        assertNull(creator.countDownLatch("latch1", 1, true, false));
        assertNull(other.countDownLatch("latch1", 1, true, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueue() throws Exception {
        Ignite clientNode = clientIgnite();
        Ignite srvNode = serverNode();

        testQueue(clientNode, srvNode);
        testQueue(srvNode, clientNode);
    }

    /**
     * @param creator Creator node.
     * @param other Other node.
     * @throws Exception If failed.
     */
    private void testQueue(Ignite creator, final Ignite other) throws Exception {
        assertNull(creator.queue("q1", 0, null));
        assertNull(other.queue("q1", 0, null));

        try (IgniteQueue<Integer> queue = creator.queue("q1", 0, new CollectionConfiguration())) {
            assertNotNull(queue);

            queue.add(1);

            assertEquals(1, queue.poll().intValue());

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    U.sleep(1000);

                    IgniteQueue<Integer> queue0 = other.queue("q1", 0, null);

                    assertEquals(0, queue0.size());

                    log.info("Add in queue.");

                    queue0.add(2);

                    return null;
                }
            });

            log.info("Try take.");

            assertEquals(2, queue.take().intValue());

            log.info("Finished take.");

            fut.get();
        }

        assertNull(creator.queue("q1", 0, null));
        assertNull(other.queue("q1", 0, null));
    }

    /**
     * @return Client node.
     */
    private Ignite clientIgnite() {
        Ignite ignite = ignite(NODE_CNT - 1);

        assertTrue(ignite.configuration().isClientMode());

        assertEquals(clientDiscovery(), ignite.configuration().getDiscoverySpi().isClientMode());

        return ignite;
    }

    /**
     * @return Server node.
     */
    private Ignite serverNode() {
        Ignite ignite = ignite(0);

        assertFalse(ignite.configuration().isClientMode());

        return ignite;
    }
}