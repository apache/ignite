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

package org.apache.ignite.internal.processors.cache.datastructures.local;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteCountDownLatchAbstractSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

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
    @Test
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
