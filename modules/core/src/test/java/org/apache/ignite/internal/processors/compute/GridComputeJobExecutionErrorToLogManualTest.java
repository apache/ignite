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

package org.apache.ignite.internal.processors.compute;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Manual test to reproduce IGNITE-4053
 */
public class GridComputeJobExecutionErrorToLogManualTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT, true);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testRuntimeException() throws Exception {
        Ignite ignite = grid(0);

        ignite.compute().runAsync(new IgniteRunnable() {
            @Override public void run() {
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
            }
        }).listen(new IgniteInClosure<IgniteFuture<Void>>() {
            @Override public void apply(IgniteFuture<Void> future) {
                throw new RuntimeException();
            }
        });
    }
}
