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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public abstract class IgniteClientDataStructuresAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final int NODE_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals(getTestIgniteInstanceName(NODE_CNT - 1))) {
            if (!clientDiscovery())
                ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        }

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_CNT - 1);
        startClientGrid(NODE_CNT - 1);
    }

    /**
     * @return {@code True} if use client discovery.
     */
    protected abstract boolean clientDiscovery();

    /**
     * @throws Exception If failed.
     */
    @Test
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

        List<IgniteAtomicSequence> sequences = new ArrayList<>(2);

        try (IgniteAtomicSequence seq = creator.atomicSequence("seq1", 1L, true)) {
            sequences.add(seq);

            assertNotNull(seq);

            assertEquals(1L, seq.get());

            assertEquals(1L, seq.getAndAdd(1));

            assertEquals(2L, seq.get());

            IgniteAtomicSequence seq0 = other.atomicSequence("seq1", 1L, false);

            sequences.add(seq0);

            assertNotNull(seq0);
        }

        for (IgniteAtomicSequence seq : sequences) {
            try {
                seq.getAndAdd(seq.batchSize());

                fail("Operations with closed sequence must fail");
            }
            catch (Throwable ignore) {
                // No-op.
            }
        }

        for (Ignite ignite : F.asList(creator, other)) {
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ignite.atomicSequence("seq1", 1L, false) == null;
                }
            }, 3_000L));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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

        assertAtomicLongClosedCorrect(creator.atomicLong("long1", 1L, false));
        assertAtomicLongClosedCorrect(other.atomicLong("long1", 1L, false));
    }

    /**
     * It is possible 3 variants:
     * * input value is null, because it already delete.
     * * input value is not null, but call 'get' method causes IllegalStateException because IgniteAtomicLong marked as delete.
     * * input value is not null, but call 'get' method causes IgniteException
     * because IgniteAtomicLong have not marked as delete yet but already removed from cache.
     */
    private void assertAtomicLongClosedCorrect(IgniteAtomicLong atomicLong) {
        if (atomicLong == null)
            assertNull(atomicLong);
        else {
            try {
                atomicLong.get();

                fail("Always should be exception because atomicLong was closed");
            }
            catch (IllegalStateException e) {
                String expectedMessage = "Sequence was removed from cache";

                assertTrue(
                    String.format("Exception should start with '%s' but was '%s'", expectedMessage, e.getMessage()),
                    e.getMessage().startsWith(expectedMessage)
                );
            }
            catch (IgniteException e) {
                String expectedMessage = "Failed to find atomic long:";

                assertTrue(
                    String.format("Exception should start with '%s' but was '%s'", expectedMessage, e.getMessage()),
                    e.getMessage().startsWith(expectedMessage)
                );
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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

        List<IgniteCountDownLatch> latches = new ArrayList<>(2);

        try (IgniteCountDownLatch latch = creator.countDownLatch("latch1", 1, true, true)) {
            latches.add(latch);

            assertNotNull(latch);

            assertEquals(1, latch.count());

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    U.sleep(1000);

                    IgniteCountDownLatch latch0 = other.countDownLatch("latch1", 1, true, false);

                    latches.add(latch0);

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

        for (IgniteCountDownLatch latch : latches) {
            try {
                latch.await(5_000L);

                fail("Operations with closed latch must fail");
            }
            catch (Throwable ignore) {
                // No-op.
            }
        }

        for (Ignite ignite : F.asList(creator, other)) {
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ignite.countDownLatch("latch1", 1, true, false) == null;
                }
            }, 3_000L));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSemaphore() throws Exception {
        Ignite clientNode = clientIgnite();
        Ignite srvNode = serverNode();

        testSemaphore(clientNode, srvNode);
        testSemaphore(srvNode, clientNode);
    }

    /**
     * @param creator Creator node.
     * @param other Other node.
     * @throws Exception If failed.
     */
    private void testSemaphore(Ignite creator, final Ignite other) throws Exception {
        assertNull(creator.semaphore("semaphore1", 1, true, false));
        assertNull(other.semaphore("semaphore1", 1, true, false));

        final List<IgniteSemaphore> semaphores = new ArrayList(2);

        try (IgniteSemaphore semaphore = creator.semaphore("semaphore1", -1, true, true)) {
            semaphores.add(semaphore);

            assertNotNull(semaphore);

            assertEquals(-1, semaphore.availablePermits());

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    U.sleep(1000);

                    IgniteSemaphore semaphore0 = other.semaphore("semaphore1", -1, true, false);

                    semaphores.add(semaphore0);

                    assertEquals(-1, semaphore0.availablePermits());

                    log.info("Release semaphore.");

                    semaphore0.release(2);

                    return null;
                }
            });

            log.info("Acquire semaphore.");

            assertTrue(semaphore.tryAcquire(1, 5000, TimeUnit.MILLISECONDS));

            log.info("Finished wait.");

            fut.get();

            assertEquals(0, semaphore.availablePermits());
        }

        for (IgniteSemaphore semaphore : semaphores) {
            try {
                semaphore.release();

                fail("Operations with closed semaphore must fail");
            }
            catch (Throwable ignore) {
                // No-op.
            }
        }

        for (Ignite ignite : F.asList(creator, other)) {
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ignite.semaphore("semaphore1", 1, true, false) == null;
                }
            }, 3_000L));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReentrantLock() throws Exception {
        Ignite clientNode = clientIgnite();
        Ignite srvNode = serverNode();

        testReentrantLock(clientNode, srvNode);
        testReentrantLock(srvNode, clientNode);
    }

    /**
     * @param creator Creator node.
     * @param other Other node.
     * @throws Exception If failed.
     */
    private void testReentrantLock(Ignite creator, final Ignite other) throws Exception {
        assertNull(creator.reentrantLock("lock1", true, false, false));
        assertNull(other.reentrantLock("lock1", true, false, false));

        List<IgniteLock> locks = new ArrayList<>(2);

        try (IgniteLock lock = creator.reentrantLock("lock1", true, false, true)) {
            locks.add(lock);

            assertNotNull(lock);

            assertFalse(lock.isLocked());

            final Semaphore semaphore = new Semaphore(0);

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    IgniteLock lock0 = other.reentrantLock("lock1", true, false, false);

                    locks.add(lock0);

                    lock0.lock();

                    assertTrue(lock0.isLocked());

                    semaphore.release();

                    U.sleep(1000);

                    log.info("Release reentrant lock.");

                    lock0.unlock();

                    return null;
                }
            });

            semaphore.acquire();

            log.info("Try acquire lock.");

            assertTrue(lock.tryLock(5000, TimeUnit.MILLISECONDS));

            log.info("Finished wait.");

            fut.get();

            assertTrue(lock.isLocked());

            lock.unlock();

            assertFalse(lock.isLocked());
        }

        for (IgniteLock lock : locks) {
            try {
                lock.tryLock(5_000L, TimeUnit.MILLISECONDS);

                fail("Operations with closed lock must fail");
            }
            catch (Throwable ignore) {
                // No-op.
            }
        }

        for (Ignite ignite : F.asList(creator, other)) {
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ignite.reentrantLock("lock1", true, false, false) == null;
                }
            }, 3_000L));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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

        if (tcpDiscovery())
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
