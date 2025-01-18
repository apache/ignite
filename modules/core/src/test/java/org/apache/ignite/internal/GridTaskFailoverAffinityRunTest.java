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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 *
 */
public class GridTaskFailoverAffinityRunTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(clientMode && igniteInstanceName.equals(getTestIgniteInstanceName(GRID_CNT)));

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setRebalanceMode(SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRestart() throws Exception {
        clientMode = false;

        nodeRestart();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRestartClient() throws Exception {
        clientMode = true;

        nodeRestart();
    }

    /**
     * @throws Exception If failed.
     */
    private void nodeRestart() throws Exception {
        // +1 for extra client node.
        startGridsMultiThreaded(GRID_CNT + 1);

        assertEquals((Boolean)clientMode, client().configuration().isClientMode());

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicInteger gridIdx = new AtomicInteger(0);

        final long stopTime = System.currentTimeMillis() + SF.applyLB(30_000, 10_000);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int grid = gridIdx.getAndIncrement();

                // Don't stop client node.
                if (grid == GRID_CNT)
                    grid = gridIdx.getAndIncrement();

                while (!stop.get() && System.currentTimeMillis() < stopTime) {
                    stopGrid(grid);

                    startGrid(grid);
                }

                return null;
            }
        }, 2, "restart-thread");

        try {
            while (System.currentTimeMillis() < stopTime) {
                Collection<IgniteFuture<?>> futs = new ArrayList<>(1000);

                for (int i = 0; i < 1000; i++) {
                    IgniteFuture<?> fut0 = client().compute().affinityCallAsync(DEFAULT_CACHE_NAME, i, new TestJob());

                    assertNotNull(fut0);

                    futs.add(fut0);
                }

                for (IgniteFuture<?> fut0 : futs) {
                    try {
                        fut0.get();
                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            }
        }
        finally {
            stop.set(true);

            fut.get();
        }
    }

    /** */
    private IgniteEx client() {
        return grid(GRID_CNT);
    }

    /**
     *
     */
    private static class TestJob implements IgniteCallable<Object> {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            Thread.sleep(1000);

            return null;
        }
    }
}
