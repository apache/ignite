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
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridSpinReadWriteLockSelfTest extends GridCommonAbstractTest {
    /** Constructor. */
    public GridSpinReadWriteLockSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testWriteLockReentry() throws Exception {
        GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

        lock.writeLock();

        lock.writeLock();

        boolean b = lock.tryWriteLock();

        assert b;
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testReadLockReentry() throws Exception {
        final GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

        lock.readLock();

        final CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture<?> f = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assert !lock.tryWriteLock();

                    info("Before write lock.");

                    latch.countDown();

                    lock.writeLock();

                    info("After write lock.");

                    return null;
                }
            }, 1);

        latch.await();

        U.sleep(100);

        lock.readLock();

        assert lock.tryReadLock();

        lock.readUnlock();
        lock.readUnlock();
        lock.readUnlock();

        f.get();
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testLockDowngrade() throws Exception {
        GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

        // Read lock while holding write lock.
        lock.writeLock();

        lock.readLock();

        lock.readUnlock();

        lock.writeUnlock();

        // Downgrade from write to read lock.
        lock.writeLock();

        lock.readLock();

        lock.writeUnlock();

        assert !lock.tryWriteLock();

        lock.readUnlock();

        // Test that we can operate with write locks now.
        lock.writeLock();
        lock.writeUnlock();
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testMonitorState() throws Exception {
        GridSpinReadWriteLock lock = new GridSpinReadWriteLock();

        try {
            lock.readUnlock();
        }
        catch (IllegalMonitorStateException e) {
            info("Caught expected exception: " + e);
        }

        try {
            lock.writeUnlock();
        }
        catch (IllegalMonitorStateException e) {
            info("Caught expected exception: " + e);
        }
    }
}