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

package org.apache.ignite.internal.metric;

import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.datastructures.AtomicLongView;
import org.apache.ignite.spi.systemview.view.datastructures.AtomicReferenceView;
import org.apache.ignite.spi.systemview.view.datastructures.AtomicSequenceView;
import org.apache.ignite.spi.systemview.view.datastructures.AtomicStampedView;
import org.apache.ignite.spi.systemview.view.datastructures.CountDownLatchView;
import org.apache.ignite.spi.systemview.view.datastructures.QueueView;
import org.apache.ignite.spi.systemview.view.datastructures.ReentrantLockView;
import org.apache.ignite.spi.systemview.view.datastructures.SemaphoreView;
import org.apache.ignite.spi.systemview.view.datastructures.SetView;
import org.junit.Test;

import static org.apache.ignite.configuration.AtomicConfiguration.DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DEFAULT_DS_GROUP_NAME;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DEFAULT_VOLATILE_DS_GROUP_NAME;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LATCHES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LOCKS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LONGS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.QUEUES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.REFERENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEMAPHORES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEQUENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SETS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.STAMPED_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.VOLATILE_DATA_REGION_NAME;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests for {@link SystemView} for data structures. */
public class SystemViewDSTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testAtomicSequence() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteAtomicSequence s1 = g0.atomicSequence("seq-1", 42, true);
            IgniteAtomicSequence s2 = g0.atomicSequence("seq-2",
                new AtomicConfiguration().setBackups(1).setGroupName("my-group"), 43, true);

            s1.batchSize(42);

            SystemView<AtomicSequenceView> seqs0 = g0.context().systemView().view(SEQUENCES_VIEW);
            SystemView<AtomicSequenceView> seqs1 = g1.context().systemView().view(SEQUENCES_VIEW);

            assertEquals(2, seqs0.size());
            assertEquals(0, seqs1.size());

            for (AtomicSequenceView s : seqs0) {
                if ("seq-1".equals(s.name())) {
                    assertEquals(42, s.value());
                    assertEquals(42, s.batchSize());
                    assertEquals(DEFAULT_DS_GROUP_NAME, s.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), s.groupId());

                    long val = s1.addAndGet(42);

                    assertEquals(val, s.value());
                    assertFalse(s.removed());
                }
                else {
                    assertEquals("seq-2", s.name());
                    assertEquals(DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE, s.batchSize());
                    assertEquals(43, s.value());
                    assertEquals("my-group", s.groupName());
                    assertEquals(CU.cacheId("my-group"), s.groupId());
                    assertFalse(s.removed());

                    s2.close();

                    assertTrue(waitForCondition(s::removed, getTestTimeout()));
                }
            }

            g1.atomicSequence("seq-1", 42, true);

            assertEquals(1, seqs1.size());

            AtomicSequenceView s = seqs1.iterator().next();

            assertEquals(DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE + 42, s.value());
            assertEquals(DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE, s.batchSize());
            assertEquals(DEFAULT_DS_GROUP_NAME, s.groupName());
            assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), s.groupId());
            assertFalse(s.removed());

            s1.close();

            assertTrue(waitForCondition(s::removed, getTestTimeout()));

            assertEquals(0, seqs0.size());
            assertEquals(0, seqs1.size());
        }
    }

    /** */
    @Test
    public void testAtomicLongs() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteAtomicLong l1 = g0.atomicLong("long-1", 42, true);
            IgniteAtomicLong l2 = g0.atomicLong("long-2",
                new AtomicConfiguration().setBackups(1).setGroupName("my-group"), 43, true);

            SystemView<AtomicLongView> longs0 = g0.context().systemView().view(LONGS_VIEW);
            SystemView<AtomicLongView> longs1 = g1.context().systemView().view(LONGS_VIEW);

            assertEquals(2, longs0.size());
            assertEquals(0, longs1.size());

            for (AtomicLongView l : longs0) {
                if ("long-1".equals(l.name())) {
                    assertEquals(42, l.value());
                    assertEquals(DEFAULT_DS_GROUP_NAME, l.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), l.groupId());

                    long val = l1.addAndGet(42);

                    assertEquals(val, l.value());
                    assertFalse(l.removed());
                }
                else {
                    assertEquals("long-2", l.name());
                    assertEquals(43, l.value());
                    assertEquals("my-group", l.groupName());
                    assertEquals(CU.cacheId("my-group"), l.groupId());
                    assertFalse(l.removed());

                    l2.close();

                    assertTrue(waitForCondition(l::removed, getTestTimeout()));
                }
            }

            g1.atomicLong("long-1", 42, true);

            assertEquals(1, longs1.size());

            AtomicLongView l = longs1.iterator().next();

            assertEquals(84, l.value());
            assertEquals(DEFAULT_DS_GROUP_NAME, l.groupName());
            assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), l.groupId());
            assertFalse(l.removed());

            l1.close();

            assertTrue(waitForCondition(l::removed, getTestTimeout()));

            assertEquals(0, longs0.size());
            assertEquals(0, longs1.size());
        }
    }

    /** */
    @Test
    public void testAtomicReference() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteAtomicReference<String> l1 = g0.atomicReference("ref-1", "str1", true);
            IgniteAtomicReference<Integer> l2 = g0.atomicReference("ref-2",
                new AtomicConfiguration().setBackups(1).setGroupName("my-group"), 43, true);

            SystemView<AtomicReferenceView> refs0 = g0.context().systemView().view(REFERENCES_VIEW);
            SystemView<AtomicReferenceView> refs1 = g1.context().systemView().view(REFERENCES_VIEW);

            assertEquals(2, refs0.size());
            assertEquals(0, refs1.size());

            for (AtomicReferenceView r : refs0) {
                if ("ref-1".equals(r.name())) {
                    assertEquals("str1", r.value());
                    assertEquals(DEFAULT_DS_GROUP_NAME, r.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), r.groupId());

                    l1.set("str2");

                    assertEquals("str2", r.value());
                    assertFalse(r.removed());
                }
                else {
                    assertEquals("ref-2", r.name());
                    assertEquals("43", r.value());
                    assertEquals("my-group", r.groupName());
                    assertEquals(CU.cacheId("my-group"), r.groupId());
                    assertFalse(r.removed());

                    l2.close();

                    assertTrue(waitForCondition(r::removed, getTestTimeout()));
                }
            }

            g1.atomicReference("ref-1", "str3", true);

            assertEquals(1, refs1.size());

            AtomicReferenceView l = refs1.iterator().next();

            assertEquals("str2", l.value());
            assertEquals(DEFAULT_DS_GROUP_NAME, l.groupName());
            assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), l.groupId());
            assertFalse(l.removed());

            l1.close();

            assertTrue(waitForCondition(l::removed, getTestTimeout()));

            assertEquals(0, refs0.size());
            assertEquals(0, refs1.size());
        }
    }

    /** */
    @Test
    public void testAtomicStamped() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteAtomicStamped<String, Integer> s1 = g0.atomicStamped("s-1", "str0", 1, true);
            IgniteAtomicStamped<String, Integer> s2 = g0.atomicStamped("s-2",
                new AtomicConfiguration().setBackups(1).setGroupName("my-group"), "str1", 43, true);

            SystemView<AtomicStampedView> stamps0 = g0.context().systemView().view(STAMPED_VIEW);
            SystemView<AtomicStampedView> stamps1 = g1.context().systemView().view(STAMPED_VIEW);

            assertEquals(2, stamps0.size());
            assertEquals(0, stamps1.size());

            for (AtomicStampedView s : stamps0) {
                if ("s-1".equals(s.name())) {
                    assertEquals("str0", s.value());
                    assertEquals("1", s.stamp());
                    assertEquals(DEFAULT_DS_GROUP_NAME, s.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), s.groupId());

                    s1.set("str2", 2);

                    assertEquals("str2", s.value());
                    assertEquals("2", s.stamp());
                    assertFalse(s.removed());
                }
                else {
                    assertEquals("s-2", s.name());
                    assertEquals("str1", s.value());
                    assertEquals("43", s.stamp());
                    assertEquals("my-group", s.groupName());
                    assertEquals(CU.cacheId("my-group"), s.groupId());
                    assertFalse(s.removed());

                    s2.close();

                    assertTrue(waitForCondition(s::removed, getTestTimeout()));
                }
            }

            g1.atomicStamped("s-1", "str3", 3, true);

            assertEquals(1, stamps1.size());

            AtomicStampedView l = stamps1.iterator().next();

            assertEquals("str2", l.value());
            assertEquals("2", l.stamp());
            assertEquals(DEFAULT_DS_GROUP_NAME, l.groupName());
            assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), l.groupId());
            assertFalse(l.removed());

            s1.close();

            assertTrue(l.removed());

            assertEquals(0, stamps0.size());
            assertEquals(0, stamps1.size());
        }
    }

    /** */
    @Test
    public void testCountDownLatch() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteCountDownLatch l1 = g0.countDownLatch("c1", 3, false, true);
            IgniteCountDownLatch l2 = g0.countDownLatch("c2", 1, true, true);

            SystemView<CountDownLatchView> latches0 = g0.context().systemView().view(LATCHES_VIEW);
            SystemView<CountDownLatchView> latches1 = g1.context().systemView().view(LATCHES_VIEW);

            assertEquals(2, latches0.size());
            assertEquals(0, latches1.size());

            String grpName = DEFAULT_VOLATILE_DS_GROUP_NAME + "@" + VOLATILE_DATA_REGION_NAME;

            for (CountDownLatchView l : latches0) {
                if ("c1".equals(l.name())) {
                    assertEquals(3, l.count());
                    assertEquals(3, l.initialCount());
                    assertFalse(l.autoDelete());

                    l1.countDown();

                    assertEquals(2, l.count());
                    assertEquals(3, l.initialCount());
                    assertFalse(l.removed());
                }
                else {
                    assertEquals("c2", l.name());
                    assertEquals(1, l.count());
                    assertEquals(1, l.initialCount());
                    assertTrue(l.autoDelete());
                    assertFalse(l.removed());

                    l2.countDown();
                    l2.close();

                    assertTrue(waitForCondition(l::removed, getTestTimeout()));
                }

                assertEquals(grpName, l.groupName());
                assertEquals(CU.cacheId(grpName), l.groupId());
            }

            IgniteCountDownLatch l3 = g1.countDownLatch("c1", 10, true, true);

            assertEquals(1, latches1.size());

            CountDownLatchView l = latches1.iterator().next();

            assertEquals(2, l.count());
            assertEquals(3, l.initialCount());
            assertEquals(grpName, l.groupName());
            assertEquals(CU.cacheId(grpName), l.groupId());
            assertFalse(l.removed());
            assertFalse(l.autoDelete());

            l3.countDown();
            l3.countDown();
            l3.close();

            assertTrue(waitForCondition(l::removed, getTestTimeout()));

            assertEquals(0, latches0.size());
            assertEquals(0, latches1.size());
        }
    }

    /** */
    @Test
    public void testSemaphores() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteSemaphore s1 = g0.semaphore("s1", 3, false, true);
            IgniteSemaphore s2 = g0.semaphore("s2", 1, true, true);

            SystemView<SemaphoreView> semaphores0 = g0.context().systemView().view(SEMAPHORES_VIEW);
            SystemView<SemaphoreView> semaphores1 = g1.context().systemView().view(SEMAPHORES_VIEW);

            assertEquals(2, semaphores0.size());
            assertEquals(0, semaphores1.size());

            String grpName = DEFAULT_VOLATILE_DS_GROUP_NAME + "@" + VOLATILE_DATA_REGION_NAME;

            IgniteInternalFuture<?> acquirePermitFut = null;

            for (SemaphoreView s : semaphores0) {
                if ("s1".equals(s.name())) {
                    assertEquals(3, s.availablePermits());
                    assertFalse(s.hasQueuedThreads());
                    assertEquals(0, s.queueLength());
                    assertFalse(s.failoverSafe());
                    assertFalse(s.broken());
                    assertFalse(s.removed());

                    acquirePermitFut = runAsync(() -> {
                        s1.acquire(2);

                        try {
                            Thread.sleep(getTestTimeout());
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        finally {
                            s1.release(2);
                        }
                    });

                    assertTrue(waitForCondition(() -> s.availablePermits() == 1, getTestTimeout()));
                    assertTrue(s.hasQueuedThreads());
                    assertEquals(1, s.queueLength());
                }
                else {
                    assertEquals(1, s.availablePermits());
                    assertFalse(s.hasQueuedThreads());
                    assertEquals(0, s.queueLength());
                    assertTrue(s.failoverSafe());
                    assertFalse(s.broken());
                    assertFalse(s.removed());

                    s2.close();

                    assertTrue(waitForCondition(s::removed, getTestTimeout()));
                }

                assertEquals(grpName, s.groupName());
                assertEquals(CU.cacheId(grpName), s.groupId());
            }

            IgniteSemaphore l3 = g1.semaphore("s1", 10, true, true);

            assertEquals(1, semaphores1.size());

            SemaphoreView s = semaphores1.iterator().next();

            assertEquals(1, s.availablePermits());
            assertTrue(s.hasQueuedThreads());
            assertEquals(1, s.queueLength());
            assertFalse(s.failoverSafe());
            assertFalse(s.broken());
            assertFalse(s.removed());

            acquirePermitFut.cancel();
            assertTrue(waitForCondition(() -> s.availablePermits() == 3, getTestTimeout()));

            l3.close();

            assertTrue(waitForCondition(s::removed, getTestTimeout()));

            assertEquals(0, semaphores0.size());
            assertEquals(0, semaphores1.size());
        }
    }

    /** */
    @Test
    public void testLocks() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteLock l1 = g0.reentrantLock("l1", false, true, true);
            IgniteLock l2 = g0.reentrantLock("l2", true, false, true);

            SystemView<ReentrantLockView> locks0 = g0.context().systemView().view(LOCKS_VIEW);
            SystemView<ReentrantLockView> locks1 = g1.context().systemView().view(LOCKS_VIEW);

            assertEquals(2, locks0.size());
            assertEquals(0, locks1.size());

            String grpName = DEFAULT_VOLATILE_DS_GROUP_NAME + "@" + VOLATILE_DATA_REGION_NAME;

            IgniteInternalFuture<?> lockFut = null;

            Runnable lockNSleep = () -> {
                l1.lock();

                try {
                    Thread.sleep(getTestTimeout());
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    l1.unlock();
                }
            };

            for (ReentrantLockView l : locks0) {
                if ("l1".equals(l.name())) {
                    assertFalse(l.locked());
                    assertFalse(l.hasQueuedThreads());
                    assertFalse(l.failoverSafe());
                    assertTrue(l.fair());
                    assertFalse(l.broken());
                    assertFalse(l.removed());

                    lockFut = runAsync(lockNSleep);

                    assertTrue(waitForCondition(l::locked, getTestTimeout()));
                }
                else {
                    assertFalse(l.hasQueuedThreads());
                    assertTrue(l.failoverSafe());
                    assertFalse(l.fair());
                    assertFalse(l.broken());
                    assertFalse(l.removed());

                    l2.close();

                    assertTrue(waitForCondition(l::removed, getTestTimeout()));
                }

                assertEquals(grpName, l.groupName());
                assertEquals(CU.cacheId(grpName), l.groupId());
            }

            IgniteLock l3 = g1.reentrantLock("l1", true, false, true);

            assertEquals(1, locks1.size());

            ReentrantLockView s = locks1.iterator().next();

            assertTrue(s.locked());
            assertFalse(s.hasQueuedThreads());
            assertFalse(s.failoverSafe());
            assertTrue(s.fair());
            assertFalse(s.broken());
            assertFalse(s.removed());

            lockFut.cancel();

            assertTrue(waitForCondition(() -> !s.locked(), getTestTimeout()));
            assertFalse(s.hasQueuedThreads());

            l3.close();

            assertTrue(waitForCondition(s::removed, getTestTimeout()));

            assertEquals(0, locks0.size());
            assertEquals(0, locks1.size());
        }
    }

    /** */
    @Test
    public void testQueue() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {

            IgniteQueue<String> q0 = g0.queue("queue-1", 42, new CollectionConfiguration()
                .setCollocated(true)
                .setBackups(1)
                .setGroupName("my-group"));
            IgniteQueue<?> q1 = g0.queue("queue-2", 0, new CollectionConfiguration());

            SystemView<QueueView> queues0 = g0.context().systemView().view(QUEUES_VIEW);
            SystemView<QueueView> queues1 = g1.context().systemView().view(QUEUES_VIEW);

            assertEquals(2, queues0.size());
            assertEquals(0, queues1.size());

            for (QueueView q : queues0) {
                if ("queue-1".equals(q.name())) {
                    assertNotNull(q.id());
                    assertEquals("queue-1", q.name());
                    assertEquals(42, q.capacity());
                    assertTrue(q.bounded());
                    assertTrue(q.collocated());
                    assertEquals("my-group", q.groupName());
                    assertEquals(CU.cacheId("my-group"), q.groupId());
                    assertFalse(q.removed());
                    assertEquals(0, q.size());

                    q0.add("first");

                    assertEquals(1, q.size());
                }
                else {
                    assertNotNull(q.id());
                    assertEquals("queue-2", q.name());
                    assertEquals(Integer.MAX_VALUE, q.capacity());
                    assertFalse(q.bounded());
                    assertFalse(q.collocated());
                    assertEquals(DEFAULT_DS_GROUP_NAME, q.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), q.groupId());
                    assertFalse(q.removed());
                    assertEquals(0, q.size());

                    q1.close();

                    assertTrue(waitForCondition(q::removed, getTestTimeout()));
                }
            }

            IgniteQueue<?> q2 = g1.queue("queue-1", 42, new CollectionConfiguration()
                .setCollocated(true)
                .setBackups(1)
                .setGroupName("my-group"));

            assertEquals(1, queues1.size());

            QueueView q = queues1.iterator().next();

            assertNotNull(q.id());
            assertEquals("queue-1", q.name());
            assertEquals(42, q.capacity());
            assertTrue(q.bounded());
            assertTrue(q.collocated());
            assertEquals("my-group", q.groupName());
            assertEquals(CU.cacheId("my-group"), q.groupId());
            assertFalse(q.removed());
            assertEquals(1, q.size());

            q2.close();

            assertTrue(waitForCondition(q::removed, getTestTimeout()));

            assertEquals(0, queues0.size());
            assertEquals(0, queues1.size());
        }
    }

    /** */
    @Test
    public void testSet() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {

            IgniteSet<String> s0 = g0.set("set-1", new CollectionConfiguration()
                .setCollocated(true)
                .setBackups(1)
                .setGroupName("my-group"));
            IgniteSet<?> s1 = g0.set("set-2", new CollectionConfiguration());

            SystemView<SetView> sets0 = g0.context().systemView().view(SETS_VIEW);
            SystemView<SetView> sets1 = g1.context().systemView().view(SETS_VIEW);

            assertEquals(2, sets0.size());
            assertEquals(0, sets1.size());

            for (SetView q : sets0) {
                if ("set-1".equals(q.name())) {
                    assertNotNull(q.id());
                    assertEquals("set-1", q.name());
                    assertTrue(q.collocated());
                    assertEquals("my-group", q.groupName());
                    assertEquals(CU.cacheId("my-group"), q.groupId());
                    assertFalse(q.removed());
                    assertEquals(0, q.size());

                    s0.add("first");

                    assertEquals(1, q.size());
                }
                else {
                    assertNotNull(q.id());
                    assertEquals("set-2", q.name());
                    assertFalse(q.collocated());
                    assertEquals(DEFAULT_DS_GROUP_NAME, q.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), q.groupId());
                    assertFalse(q.removed());
                    assertEquals(0, q.size());

                    s1.close();

                    assertTrue(waitForCondition(q::removed, getTestTimeout()));
                }
            }

            IgniteSet<?> s2 = g1.set("set-1", new CollectionConfiguration()
                .setCollocated(true)
                .setBackups(1)
                .setGroupName("my-group"));

            assertEquals(1, sets1.size());

            SetView s = sets1.iterator().next();

            assertNotNull(s.id());
            assertEquals("set-1", s.name());
            assertTrue(s.collocated());
            assertEquals("my-group", s.groupName());
            assertEquals(CU.cacheId("my-group"), s.groupId());
            assertFalse(s.removed());
            assertEquals(1, s.size());

            s2.close();

            assertTrue(waitForCondition(s::removed, getTestTimeout()));

            assertEquals(0, sets0.size());
            assertEquals(0, sets1.size());
        }
    }
}
