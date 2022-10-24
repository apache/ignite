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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/** */
public class HeapConsumptionDataStreamerTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            CacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(PARTITIONED);
            cc.setAtomicityMode(ATOMIC);
            cc.setWriteSynchronizationMode(PRIMARY_SYNC);
            cc.setBackups(2);

            cfg.setCacheConfiguration(cc);

            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
        }
        else
            cfg.setClientMode(true);

        return cfg;
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * Tested with 1G heap / 10 bil records and 4g heap / 10bils records.
     * Larger heap stays longer. But it is still a race.
     */
    @Test
    public void testHeap() throws Exception {
        int entriesToLoad = 1_000_000;

        startGrids(3);

        grid(0).cluster().state(ClusterState.ACTIVE);

        Ignite ldr = startClientGrid(3);

        try (IgniteDataStreamer<Integer, Object> ds = ldr.dataStreamer(DEFAULT_CACHE_NAME)) {
            ds.allowOverwrite(true);

            for (int e = 0; e < entriesToLoad; ++e)
                ds.addData(e, e);
        }

        assertEquals(grid(0).cache(DEFAULT_CACHE_NAME).size(), entriesToLoad);
    }
}
