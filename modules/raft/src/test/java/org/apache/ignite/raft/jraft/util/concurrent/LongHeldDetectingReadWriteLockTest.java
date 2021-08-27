/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util.concurrent;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.lang.IgniteLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class LongHeldDetectingReadWriteLockTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(LongHeldDetectingReadWriteLockTest.class);

    @Test
    public void testLongHeldWriteLock() throws InterruptedException {
        final ReadWriteLock readWriteLock = new LongHeldDetectingReadWriteLock(100, TimeUnit.MILLISECONDS) {

            @Override
            public void report(AcquireMode acquireMode, Thread owner, Collection<Thread> queuedThreads,
                long blockedNanos) {
                System.out.println("currentThread=" + Thread.currentThread() +
                    " acquireMode=" + acquireMode +
                    " lockOwner=" + owner +
                    " queuedThreads= " + queuedThreads +
                    " blockedMs=" + TimeUnit.NANOSECONDS.toMillis(blockedNanos));

                assertTrue(Thread.currentThread().getName().contains("read-lock-thread"));
                assertSame(AcquireMode.Read, acquireMode);
                assertEquals("write-lock-thread", owner.getName());
                assertEquals(2000, TimeUnit.NANOSECONDS.toMillis(blockedNanos), 100);
            }
        };

        final CountDownLatch latch = new CountDownLatch(1);
        Thread t1 = null;
        Thread t2 = null;
        Thread t3 = null;
        try {
            t1 = new Thread(() -> {
                readWriteLock.writeLock().lock();
                latch.countDown();
                try {
                    Thread.sleep(2000);
                }
                catch (final InterruptedException e) {
                    LOG.error("Thread was interrupted", e);
                }
                finally {
                    readWriteLock.writeLock().unlock();
                }
            }, "write-lock-thread");

            t1.start();

            latch.await();

            final CountDownLatch latch1 = new CountDownLatch(2);
            t2 = new Thread(() -> {
                readWriteLock.readLock().lock();
                readWriteLock.readLock().unlock();
                latch1.countDown();
            }, "read-lock-thread-1");

            t2.start();

            t3 = new Thread(() -> {
                readWriteLock.readLock().lock();
                readWriteLock.readLock().unlock();
                latch1.countDown();
            }, "read-lock-thread-2");

            t3.start();

            latch1.await();
        } finally {
            if (t1 != null)
                t1.join();

            if (t2 != null)
                t2.join();

            if (t3 != null)
                t3.join();
        }
    }

    @Test
    public void testLongHeldReadLock() throws InterruptedException {
        final ReadWriteLock readWriteLock = new LongHeldDetectingReadWriteLock(100, TimeUnit.MILLISECONDS) {

            @Override
            public void report(AcquireMode acquireMode, Thread owner, Collection<Thread> queuedThreads,
                long blockedNanos) {
                System.out.println("currentThread=" + Thread.currentThread() +
                    " acquireMode=" + acquireMode +
                    " lockOwner=" + owner +
                    " queuedThreads= " + queuedThreads +
                    " blockedMs=" + TimeUnit.NANOSECONDS.toMillis(blockedNanos));

                assertTrue(Thread.currentThread().getName().contains("write-lock-thread"));
                assertSame(AcquireMode.Write, acquireMode);
                assertNull(owner);
                assertEquals(2000, TimeUnit.NANOSECONDS.toMillis(blockedNanos), 100);
            }
        };

        final CountDownLatch latch = new CountDownLatch(1);
        Thread t1 = null;
        Thread t2 = null;
        Thread t3 = null;
        try {
            t1 = new Thread(() -> {
                readWriteLock.readLock().lock();
                latch.countDown();
                try {
                    Thread.sleep(2000);
                }
                catch (final InterruptedException e) {
                    LOG.error("Thread was interrupted", e);
                }
                finally {
                    readWriteLock.readLock().unlock();
                }
            }, "read-lock-thread");

            t1.start();

            latch.await();

            final CountDownLatch latch1 = new CountDownLatch(2);
            t2 = new Thread(() -> {
                readWriteLock.writeLock().lock();
                readWriteLock.writeLock().unlock();
                latch1.countDown();
            }, "write-lock-thread-1");

            t2.start();

            t3 = new Thread(() -> {
                readWriteLock.writeLock().lock();
                readWriteLock.writeLock().unlock();
                latch1.countDown();
            }, "write-lock-thread-2");

            t3.start();

            latch1.await();
        } finally {
            if (t1 != null)
                t1.join();

            if (t2 != null)
                t2.join();

            if (t3 != null)
                t3.join();
        }
    }
}
