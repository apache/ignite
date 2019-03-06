/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.jvmtest;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Test to check strange assertion in eviction manager.
 */
public class QueueSizeCounterMultiThreadedTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueSizeCounter() throws Exception {
        final ConcurrentLinkedQueue<Integer> q = new ConcurrentLinkedQueue<>();

        final AtomicInteger sizeCnt = new AtomicInteger();

        final AtomicBoolean done = new AtomicBoolean();

        final AtomicBoolean guard = new AtomicBoolean();

        final ReadWriteLock lock = new ReentrantReadWriteLock();

        IgniteInternalFuture fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
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
