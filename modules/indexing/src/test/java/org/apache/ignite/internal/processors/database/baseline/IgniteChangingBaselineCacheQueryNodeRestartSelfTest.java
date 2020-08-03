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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest;

/**
 *
 */
public class IgniteChangingBaselineCacheQueryNodeRestartSelfTest extends IgniteCacheQueryNodeRestartSelfTest {
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

        startGrids(gridCount());

        initStoreStrategy();

        grid(0).cluster().baselineAutoAdjustEnabled(false);
        grid(0).cluster().active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture createRestartAction(final AtomicBoolean done, final AtomicInteger restartCnt) throws Exception {
        return multithreadedAsync(new Callable<Object>() {
            /** */
            private final long baselineTopChangeInterval = 10 * 1000;

            /** */
            private final int logFreq = 50;

            /** flag to indicate that last operation was changing BaselineTopology up (add node) */
            private boolean lastOpChangeUp;

            @SuppressWarnings({"BusyWait"})
            @Override public Object call() throws Exception {
                while (!done.get()) {
                    if (lastOpChangeUp) {
                        //need to do change down: stop node, set new BLT without it
                        stopGrid(gridCount());

                        lastOpChangeUp = false;
                    }
                    else {
                        startGrid(gridCount());

                        lastOpChangeUp = true;
                    }

                    resetBaselineTopology();

                    Thread.sleep(baselineTopChangeInterval);

                    //Only stopping node triggers Rebalance.
                    int c = lastOpChangeUp ? restartCnt.get() : restartCnt.incrementAndGet();

                    if (c % logFreq == 0)
                        info("BaselineTopology changes: " + c);
                }

                return true;
            }
        }, 1, "restart-thread");
    }

    /** */
    private Collection<BaselineNode> baselineNodes(Collection<ClusterNode> clNodes) {
        Collection<BaselineNode> res = new ArrayList<>(clNodes.size());

        for (ClusterNode clN : clNodes)
            res.add(clN);

        return res;
    }
}
