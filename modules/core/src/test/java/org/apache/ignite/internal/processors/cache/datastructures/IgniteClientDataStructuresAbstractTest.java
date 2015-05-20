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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

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

            if (clientDiscovery())
                cfg.setDiscoverySpi(new TcpClientDiscoverySpi());
        }

        ((TcpDiscoverySpiAdapter)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

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

        assertNull(clientNode.atomicSequence("seq1", 1L, false));

        try (IgniteAtomicSequence seq = clientNode.atomicSequence("seq1", 1L, true)) {
            assertNotNull(seq);

            assertEquals(1L, seq.get());

            assertEquals(1L, seq.getAndAdd(1));

            assertEquals(2L, seq.get());

            IgniteAtomicSequence seq0 = srvNode.atomicSequence("seq1", 1L, false);

            assertNotNull(seq0);
        }

        assertNull(clientNode.atomicSequence("seq1", 1L, false));
        assertNull(srvNode.atomicSequence("seq1", 1L, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLong() throws Exception {
        Ignite clientNode = clientIgnite();

        Ignite srvNode = serverNode();

        assertNull(clientNode.atomicLong("long1", 1L, false));

        try (IgniteAtomicLong cntr = clientNode.atomicLong("long1", 1L, true)) {
            assertNotNull(cntr);

            assertEquals(1L, cntr.get());

            assertEquals(1L, cntr.getAndAdd(1));

            assertEquals(2L, cntr.get());

            IgniteAtomicLong cntr0 = srvNode.atomicLong("long1", 1L, false);

            assertNotNull(cntr0);

            assertEquals(2L, cntr0.get());

            assertEquals(3L, cntr0.incrementAndGet());

            assertEquals(3L, cntr.get());
        }

        assertNull(clientNode.atomicLong("long1", 1L, false));
        assertNull(srvNode.atomicLong("long1", 1L, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSet() throws Exception {
        Ignite clientNode = clientIgnite();

        Ignite srvNode = serverNode();

        assertNull(clientNode.set("set1", null));

        CollectionConfiguration colCfg = new CollectionConfiguration();

        try (IgniteSet<Integer> set = clientNode.set("set1", colCfg)) {
            assertNotNull(set);

            assertEquals(0, set.size());

            assertFalse(set.contains(1));

            assertTrue(set.add(1));

            assertTrue(set.contains(1));

            IgniteSet<Integer> set0 = srvNode.set("set1", null);

            assertTrue(set0.contains(1));

            assertEquals(1, set0.size());

            assertTrue(set0.remove(1));

            assertFalse(set.contains(1));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatch() throws Exception {
        Ignite clientNode = clientIgnite();

        final Ignite srvNode = serverNode();

        assertNull(clientNode.countDownLatch("latch1", 1, true, false));

        try (IgniteCountDownLatch latch = clientNode.countDownLatch("latch1", 1, true, true)) {
            assertNotNull(latch);

            assertEquals(1, latch.count());

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    U.sleep(1000);

                    IgniteCountDownLatch latch0 = srvNode.countDownLatch("latch1", 1, true, false);

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
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueue() throws Exception {
        Ignite clientNode = clientIgnite();

        final Ignite srvNode = serverNode();

        CollectionConfiguration colCfg = new CollectionConfiguration();

        assertNull(clientNode.queue("q1", 0, null));

        try (IgniteQueue<Integer> queue = clientNode.queue("q1", 0, colCfg)) {
            assertNotNull(queue);

            queue.add(1);

            assertEquals(1, queue.poll().intValue());

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    U.sleep(1000);

                    IgniteQueue<Integer> queue0 = srvNode.queue("q1", 0, null);

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
    }

    /**
     * @return Client node.
     */
    private Ignite clientIgnite() {
        Ignite ignite = ignite(NODE_CNT - 1);

        assertTrue(ignite.configuration().isClientMode());

        if (clientDiscovery())
            assertTrue(ignite.configuration().getDiscoverySpi() instanceof TcpClientDiscoverySpi);
        else
            assertTrue(ignite.configuration().getDiscoverySpi() instanceof TcpDiscoverySpi);

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
