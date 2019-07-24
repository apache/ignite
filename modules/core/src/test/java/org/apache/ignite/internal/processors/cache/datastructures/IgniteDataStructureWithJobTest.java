/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteDataStructureWithJobTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJobWithRestart() throws Exception {
        Ignite ignite = startGrid(0);

        final AtomicBoolean stop = new AtomicBoolean();

        final long endTime = System.currentTimeMillis() + GridTestUtils.SF.applyLB(60_000, 20_000);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!Thread.currentThread().isInterrupted() && !stop.get() && U.currentTimeMillis() < endTime) {
                    try (Ignite ignored = startGrid(1)) {
                        Thread.sleep(500);
                    }
                }

                return null;
            }
        });

        try {
            int iter = 0;

            while (System.currentTimeMillis() < endTime) {
                try {
                    ignite.compute().broadcast(new IgniteClosure<IgniteQueue, Integer>() {
                        @Override public Integer apply(IgniteQueue queue) {
                            assertNotNull(queue);

                            return 1;
                        }
                    }, ignite.queue("queue", 0, new CollectionConfiguration()));
                }
                catch (IgniteException ignore) {
                    // No-op.
                }

                if (iter++ % 1000 == 0)
                    log.info("Iteration: " + iter);
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();
    }
}
