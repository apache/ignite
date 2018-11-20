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

package org.apache.ignite.jvmtest;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import junit.framework.TestCase;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Test to check strange assertion in eviction manager.
 */
public class QueueSizeCounterMultiThreadedTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void testQueueSizeCounter() throws Exception {
        final ConcurrentLinkedQueue<Integer> q = new ConcurrentLinkedQueue<>();

        final AtomicInteger sizeCnt = new AtomicInteger();

        final AtomicBoolean done = new AtomicBoolean();

        final AtomicBoolean guard = new AtomicBoolean();

        final ReadWriteLock lock = new ReentrantReadWriteLock();

        IgniteInternalFuture fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @SuppressWarnings( {"BusyWait"})
                @Nullable @Override public Object call() throws Exception {
                    int cleanUps = 0;

                    while (!done.get()) {
                        lock.readLock().lock();

                        try {
                            q.add(1);

                            sizeCnt.incrementAndGet();
                        }
                        finally {
                            lock.readLock().unlock();
                        }

                        if (sizeCnt.get() > 100 && guard.compareAndSet(false, true)) {
                            lock.writeLock().lock();

                            try {
                                for (Integer i = q.poll(); i != null; i = q.poll())
                                    sizeCnt.decrementAndGet();

                                cleanUps++;

                                assert sizeCnt.get() == 0 : "Invalid count [cnt=" + sizeCnt.get() +
                                    ", size=" + q.size() + ", entries=" + q + ']';
                            }
                            finally {
                                lock.writeLock().unlock();

                                guard.set(false);
                            }
                        }
                    }

                    X.println("Cleanups count (per thread): " + cleanUps);

                    return null;
                }
            },
            100,
            "test-thread"
        );

        Thread.sleep(3 * 60 * 1000);

        done.set(true);

        fut1.get();
    }
}