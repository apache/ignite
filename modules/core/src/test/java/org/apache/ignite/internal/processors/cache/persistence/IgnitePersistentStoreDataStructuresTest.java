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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePersistentStoreDataStructuresTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static volatile boolean autoActivationEnabled = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(200 * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setAutoActivationEnabled(autoActivationEnabled);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        autoActivationEnabled = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueue() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteQueue<Object> queue = ignite.queue("testQueue", 100, new CollectionConfiguration());

        for (int i = 0; i < 100; i++)
            queue.offer(i);

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        queue = ignite.queue("testQueue", 0, null);

        for (int i = 0; i < 100; i++)
            assertEquals(i, queue.poll());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteAtomicLong atomicLong = ignite.atomicLong("testLong", 0, true);

        for (int i = 0; i < 100; i++)
            atomicLong.incrementAndGet();

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        atomicLong = ignite.atomicLong("testLong", 0, false);

        for (int i = 100; i != 0; )
            assertEquals(i--, atomicLong.getAndDecrement());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSequence() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteAtomicSequence sequence = ignite.atomicSequence("testSequence", 0, true);

        int i = 0;

        while (i < 1000) {
            sequence.incrementAndGet();

            i++;
        }

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        sequence = ignite.atomicSequence("testSequence", 0, false);

        assertTrue(sequence.incrementAndGet() > i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSequenceAfterAutoactivation() throws Exception {
        final String seqName = "testSequence";

        autoActivationEnabled = true;

        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        ignite.atomicSequence(seqName, 0, true);

        stopAllGrids(true);

        final Ignite node = startGrids(2);

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                // Should not hang.
                node.atomicSequence(seqName, 0, false);
            }
        });

        try {
            fut.get(10, TimeUnit.SECONDS);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            fut.cancel();

            fail("Ignite was stuck on getting the atomic sequence after autoactivation.");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSet() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5553");

        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteSet<Object> set = ignite.set("testSet", new CollectionConfiguration());

        for (int i = 0; i < 100; i++)
            set.add(i);

        assertEquals(100, set.size());

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        set = ignite.set("testSet", null);

        assertFalse(set.add(99));

        for (int i = 0; i < 100; i++)
            assertTrue(set.contains(i));

        assertEquals(100, set.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockVolatility() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteLock lock = ignite.reentrantLock("test", false, true, true);

        assert lock != null;

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        lock = ignite.reentrantLock("test", false, true, false);

        assert lock == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSemaphoreVolatility() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteSemaphore sem = ignite.semaphore("test", 10, false, true);

        assert sem != null;

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        sem = ignite.semaphore("test", 10, false, false);

        assert sem == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchVolatility() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.active(true);

        IgniteCountDownLatch latch = ignite.countDownLatch("test", 10, false, true);

        assert latch != null;

        stopAllGrids();

        ignite = startGrids(4);

        ignite.active(true);

        latch = ignite.countDownLatch("test", 10, false, false);

        assert latch == null;
    }

}
