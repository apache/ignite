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

package org.apache.ignite.util;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.GridReentrantRWLock;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
public class GridReentrantRWLockSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testLock() throws Exception {
        final GridReentrantRWLock l = new GridReentrantRWLock();

        final long[] f = {1, 1, 2};

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                GridRandom rnd = new GridRandom();

                while (!stop.get()) {
                    boolean update = rnd.nextInt(16) < 5;

                    try {
                        if (update)
                            l.writeLock();
                        else
                            l.readLock();
                    }
                    catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }

                    long a = f[0];
                    long b = f[1];
                    long c = f[2];

                    assertTrue(a + b == c);

                    if (update) {
                        long d = b + c;

                        if (d > 0) {
                            f[0] = b;
                            f[1] = c;
                            f[2] = d;
                        }
                        else {
                            f[0] = 1;
                            f[1] = 1;
                            f[2] = 2;
                        }

                        l.writeUnlock();
                    }
                    else
                        l.readUnlock();
                }
            }
        }, 16);

        Thread.sleep(1000);

        stop.set(true);

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleaseFromOtherThread() throws Exception {
        final GridReentrantRWLock l = new GridReentrantRWLock();

        assertTrue(l.tryReadLock());
        assertTrue(l.tryReadLock());

        assertFalse(l.tryWriteLock());

        multithreaded(new Runnable() {
            @Override public void run() {
                l.readUnlock();
                l.readUnlock();
            }
        }, 1);

        assertTrue(l.tryWriteLock());
        assertFalse(l.tryReadLock());
        assertTrue(l.tryWriteLock());

        multithreaded(new Runnable() {
            @Override public void run() {
                l.writeUnlock();
                l.writeUnlock();
            }
        }, 1);

        assertTrue(l.tryReadLock());

        multithreaded(new Runnable() {
            @Override public void run() {
                assertFalse(l.tryWriteLock());

                l.readUnlock();

                assertTrue(l.tryWriteLock());
            }
        }, 1);

        l.writeUnlock();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriterPreference() throws Exception {
        final GridReentrantRWLock l = new GridReentrantRWLock();

        assertTrue(l.tryReadLock());
        assertTrue(l.tryReadLock());

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertFalse(l.tryWriteLock());

                l.writeLock();

                return null;
            }
        }, 1);

        Thread.sleep(100);

        assertFalse(l.tryReadLock());

        l.readUnlock();
        l.readUnlock();

        fut.get();

        l.writeUnlock();

        assertTrue(l.tryReadLock());
        l.readUnlock();
    }
}
