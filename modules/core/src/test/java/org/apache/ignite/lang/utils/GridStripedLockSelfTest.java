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

package org.apache.ignite.lang.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridStripedLockSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int STRIPE_COUNT = 16;

    /** */
    private static final long ITERATION_COUNT = STRIPE_COUNT * 10000;

    /** */
    private static final int THREAD_COUNT = STRIPE_COUNT * 4;

    /** */
    private GridStripedLock lock;

    /** */
    private CyclicBarrier barrier = new CyclicBarrier(THREAD_COUNT);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        lock = new GridStripedLock(STRIPE_COUNT);

        barrier = new CyclicBarrier(THREAD_COUNT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIntLocking() throws Exception {
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    barrier.await();
                }
                catch (Exception e) {
                    fail("Failed to await other threads: " + e.getMessage());
                }

                for (int i = 0; i < STRIPE_COUNT * 10000; i++) {
                    lock.lock(i);

                    try {
                        int holdCnt = 0;

                        for (int lockNum = 0; lockNum < STRIPE_COUNT; lockNum++)
                            if (((ReentrantLock)lock.getLock(lockNum)).isHeldByCurrentThread())
                                holdCnt++;

                        assertEquals(1, holdCnt);
                    }
                    finally {
                        lock.unlock(i);
                    }
                }
            }
        }, THREAD_COUNT, "GridStripedLock-test");
    }

    /**
     * @throws Exception If failed.
     */
    public void testLongLocking() throws Exception {
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    barrier.await();
                }
                catch (Exception e) {
                    fail("Failed to await other threads: " + e.getMessage());
                }

                for (long i = Integer.MAX_VALUE; i < ITERATION_COUNT + Integer.MAX_VALUE; i++) {
                    lock.lock(i);

                    try {
                        int holdCnt = 0;

                        for (long lockNum = 0; lockNum < STRIPE_COUNT; lockNum++)
                            if (((ReentrantLock)lock.getLock(lockNum)).isHeldByCurrentThread())
                                holdCnt++;

                        assertEquals(1, holdCnt);
                    }
                    finally {
                        lock.unlock(i);
                    }
                }
            }
        }, THREAD_COUNT, "GridStripedLock-test");
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectLocking() throws Exception {
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    barrier.await();
                }
                catch (Exception e) {
                    fail("Failed to await other threads: " + e.getMessage());
                }

                for (Object o : testObjects((int)ITERATION_COUNT)) {
                    lock.lock(o);

                    try {
                        int holdCnt = 0;

                        for (Object lockObject : testObjects(STRIPE_COUNT))
                            if (((ReentrantLock)lock.getLock(lockObject)).isHeldByCurrentThread())
                                holdCnt++;

                        // null object considered 0-hash code, so they should appear twicely.
                        if (o == null || o.hashCode() % STRIPE_COUNT == 0)
                            assertEquals(2, holdCnt);
                        else
                            assertEquals("Test object " + o.hashCode(), 1, holdCnt);
                    }
                    finally {
                        lock.unlock(o);
                    }
                }
            }
        }, THREAD_COUNT, "GridStripedLock-test");
    }

    /**
     * Returns iterable containing required number of objects with consequent hashCodes starting from 0
     * and one {@code null} element.
     *
     * @param cnt Objects count.
     * @return Iterable instance.
     */
    private Iterable<Object> testObjects(final int cnt) {
        assert cnt >= 2;

        return new Iterable<Object>() {
            @Override public Iterator<Object> iterator() {
                return new Iterator<Object>() {
                    private int curr = -1;

                    @Override public boolean hasNext() {
                        return curr < cnt;
                    }

                    @Override public Object next() {
                        curr++;

                        if (curr > cnt)
                            throw new NoSuchElementException();

                        return curr == 0 ?
                            null :
                            new Object() {
                                private final int code = curr - 1;

                                @Override public int hashCode() {
                                    return code;
                                }
                            };
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}