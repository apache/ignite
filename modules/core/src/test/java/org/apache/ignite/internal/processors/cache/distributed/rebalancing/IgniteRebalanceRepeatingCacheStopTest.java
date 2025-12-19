/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test rebalance during repeating caches stop (due to deactivation or explicit call).
 */
@ParameterizedClass(name = "persistence={0}")
@ValueSource(booleans = {true, false})
public class IgniteRebalanceRepeatingCacheStopTest extends GridCommonAbstractTest {
    /** */
    @Parameter(0)
    public boolean pds = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(pds)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @ParameterizedTest(name = "sharedGroup={1}")
    @ValueSource(booleans = {true, false})
    public void testRebalanceOnDeactivate(boolean sharedGrp) throws Exception {
        doTest(ignite -> {
            ignite.cluster().state(ClusterState.INACTIVE);

            ignite.cluster().state(ClusterState.ACTIVE);
        }, sharedGrp);
    }

    /** */
    @ParameterizedTest(name = "sharedGroup={1}")
    @ValueSource(booleans = {true, false})
    public void testRebalanceOnCacheStop(boolean sharedGrp) throws Exception {
        doTest(ignite -> ignite.cache(DEFAULT_CACHE_NAME).destroy(), sharedGrp);
    }

    /** */
    private void doTest(Consumer<Ignite> action, boolean sharedGrp) throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        ignite0.cluster().state(ClusterState.ACTIVE);
        ignite0.cluster().baselineAutoAdjustEnabled(false);

        for (int i = 0; i < 10; i++) {
            IgniteCache<Integer, Integer> cache = ignite0.getOrCreateCache(
                new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                    .setBackups(1)
                    .setGroupName(sharedGrp ? "grp" : null)
                    .setAffinity(new RendezvousAffinityFunction(false, 1)));

            cache.clear();

            stopGrid(0);

            try (IgniteDataStreamer<Integer, Integer> streamer = ignite1.dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int j = 0; j < 100_000; j++)
                    streamer.addData(j, j);
            }

            ignite0 = startGrid(0);

            action.accept(ignite0);
        }
    }
}
