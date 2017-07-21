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
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteCountDownLatchAbstractSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 *
 */
public class IgniteLocalCountDownLatchSelfTest extends IgniteCountDownLatchAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode atomicsCacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void testLatch() throws Exception {
        // Test main functionality.
        IgniteCountDownLatch latch = grid(0).countDownLatch("latch", 2, false, true);

        assertNotNull(latch);

        assertEquals(2, latch.count());

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    IgniteCountDownLatch latch = grid(0).countDownLatch("latch", 2, false, true);

                    assert latch != null && latch.count() == 2;

                    info("Thread is going to wait on latch: " + Thread.currentThread().getName());

                    assert latch.await(1, MINUTES);

                    info("Thread is again runnable: " + Thread.currentThread().getName());

                    return null;
                }
            },
            THREADS_CNT,
            "test-thread"
        );

        Thread.sleep(3000);

        assert latch.countDown()  == 1;

        assert latch.countDown() == 0;

        assert latch.await(1, SECONDS);

        // Ensure there are no hangs.
        fut.get();

        // Test operations on removed latch.
        IgniteCountDownLatch latch0 = grid(0).countDownLatch("latch", 0, false, false);

        assertNotNull(latch0);

        latch0.close();

        checkRemovedLatch(latch);
    }
}