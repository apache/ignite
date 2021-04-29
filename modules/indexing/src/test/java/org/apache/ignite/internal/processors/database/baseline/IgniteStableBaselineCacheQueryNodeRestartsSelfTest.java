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
package org.apache.ignite.internal.processors.database.baseline;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest;

/**
 *
 */
public class IgniteStableBaselineCacheQueryNodeRestartsSelfTest extends IgniteCacheQueryNodeRestartSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(200L * 1024 * 1024)
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGrids(gridCount() + 1);

        initStoreStrategy();

        grid(0).cluster().active(true);

        stopGrid(gridCount());

        startGrid(gridCount() + 1);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture createRestartAction(final AtomicBoolean done, final AtomicInteger restartCnt) throws Exception {
        return multithreadedAsync(new Callable<Object>() {
            /** */
            private final int logFreq = 50;

            @SuppressWarnings({"BusyWait"})
            @Override public Object call() throws Exception {
                while (!done.get()) {
                    int idx = gridCount();

                    startGrid(idx);

                    stopGrid(idx);

                    int c = restartCnt.incrementAndGet();

                    if (c % logFreq == 0)
                        info("Node restarts: " + c);
                }

                return true;
            }
        }, 1, "restart-thread");
    }

    @Override protected int countRebalances(int nodes, int restarts) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }
}
