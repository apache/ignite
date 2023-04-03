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

package org.apache.ignite.internal.processors.cache.persistence;

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
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test PDS consistency after checkpoint, triggered by timeout, after cluster deactivation.
 */
public class IgnitePdsCheckpointAfterDeactivateTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
                .setCheckpointFrequency(1_000L))
            .setFailureHandler(new StopNodeFailureHandler());
    }

    /** */
    @Test
    public void testCpAfterClusterDeactivate() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().state(ClusterState.ACTIVE);

        ignite0.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100_000; i++)
                streamer.addData(i, i);
        }

        stopGrid(0);

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite1.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);
            for (int i = 0; i < 100_000; i++)
                streamer.addData(i, i + 1);
        }

        ignite0 = startGrid(0);

        ((GridCacheDatabaseSharedManager)ignite0.context().cache().context().database()).addCheckpointListener(new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) {
                // No-op.
            }

            @Override public void onCheckpointBegin(Context ctx) {
                // Delay last checkpoint to wait for other processes to mark some pages as dirty before actual 
                // caches stop. 
                if ("caches stop".equals(ctx.progress().reason()))
                    doSleep(1_000L);   
            }

            @Override public void beforeCheckpointBegin(Context ctx) {
                // No-op.
            }
        });

        ignite0.cluster().state(ClusterState.INACTIVE);

        doSleep(2_000L);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100_000; i++)
            assertEquals((Integer)(i + 1), cache.get(i));
    }
}
