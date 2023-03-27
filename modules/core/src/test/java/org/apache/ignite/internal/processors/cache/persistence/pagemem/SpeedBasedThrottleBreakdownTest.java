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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Tests for the cases when high load might break down speed-based throttling protection.
 *
 * @see PagesWriteSpeedBasedThrottle
 */
@RunWith(Parameterized.class)
public class SpeedBasedThrottleBreakdownTest extends GridCommonAbstractTest {
    /***/
    @Parameterized.Parameter
    public boolean useSpeedBasedThrottling;

    /** Parameters. */
    @Parameterized.Parameters(name = "Use speed-based throttling: {0}")
    public static Iterable<Boolean[]> data() {
        return Arrays.asList(
            new Boolean[] {true},
            new Boolean[] {false}
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        // tiny CP buffer is required to reproduce this problem easily
                        .setCheckpointPageBufferSize(3_000_000)
                        .setPersistenceEnabled(true))
                .setCheckpointFrequency(200)
                .setWriteThrottlingEnabled(useSpeedBasedThrottling);

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3L * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /**
     * There was the following bug: at the very start of a checkpoint, the checkpoint progress information was not yet
     * available to the speed-based throttler, in which case it did not apply throttling. If during that short period
     * there was high pressure on the Checkpoint Buffer, the buffer would be exhausted causing node failure.
     *
     * It was easy to reproduce with a small Checkpoint Buffer. This test does exactly this: with a small
     * Checkpoint Buffer, it creates a spike of load on it. The test is successful if the CP Buffer protection
     * does not break down and no exception gets thrown as a result.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void speedBasedThrottleShouldNotAllowCPBufferBreakdownWhenCPBufferIsSmall() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().state(ACTIVE);
        IgniteCache<Object, Object> cache = ignite.createCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100_000; i++) {
            cache.put("key" + i, ThreadLocalRandom.current().nextDouble());
        }

        assertFalse(G.allGrids().isEmpty());
    }
}
