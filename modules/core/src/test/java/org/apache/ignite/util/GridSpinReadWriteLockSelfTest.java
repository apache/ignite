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

package org.apache.ignite.util;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
