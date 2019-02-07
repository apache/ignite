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

package org.apache.ignite.internal.processors.cache.datastructures.local;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteLockAbstractSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 *
 */
public class IgniteLocalLockSelfTest extends IgniteLockAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode atomicsCacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testReentrantLock() throws Exception {
        // Test main functionality.
        IgniteLock lock = grid(0).reentrantLock("lock", true, false, true);

        assertNotNull(lock);

        assertEquals(0, lock.getHoldCount());

        lock.lock();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    IgniteLock lock = grid(0).reentrantLock("lock", true, false, true);

                    assert lock != null;

                    info("Thread is going to wait on lock: " + Thread.currentThread().getName());

                    assert lock.tryLock(1, MINUTES);

                    info("Thread is again runnable: " + Thread.currentThread().getName());

                    lock.unlock();

                    return null;
                }
            },
            THREADS_CNT,
            "test-thread"
        );

        Thread.sleep(3000);

        assert lock.isLocked();

        assert lock.getHoldCount() == 1;

        lock.lock();

        assert lock.isLocked();

        assert lock.getHoldCount() == 2;

        lock.unlock();

        assert lock.isLocked();

        assert lock.getHoldCount() == 1;

        lock.unlock();

        // Ensure there are no hangs.
        fut.get();

        // Test operations on removed lock.
        IgniteLock lock0 = grid(0).reentrantLock("lock", true, false, false);

        assertNotNull(lock0);

        lock0.close();

        checkRemovedReentrantLock(lock0);
    }
}
