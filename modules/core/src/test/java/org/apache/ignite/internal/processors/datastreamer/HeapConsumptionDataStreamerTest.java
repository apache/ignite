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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Testes heap issues, JVM pauses, lost dht updates when streaming with PDS.
 * Tested with 1G heap. Larger heap stays longer.
 */
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
     * Putting just trivial value.
     * Tested with 1G heap. Larger heap stays longer.
     */
    @Test
    public void testLoadSimpleValue() throws Exception {
        dataStreamerLoad(1_000_000, i -> i);
    }

    /**
     * Putting values comparable to page size.
     * Tested with 1G heap. Larger heap stays longer.
     */
    @Test
    public void testLoad() throws Exception {
        int avgDataLen = 500;

        int entriesToLoad = 1_000_000;

        Object[] values = loadData(2000, avgDataLen);

        dataStreamerLoad(entriesToLoad, i -> values[i % values.length]);
    }

    /** */
    @Test
    public void testLoadWithoutStreamerBatched() throws Exception {
        int avgDataLen = 500;

        int entriesToLoad = 1_000_000;

        Object[] values = loadData(2000, avgDataLen);

        cacheLoad(entriesToLoad, i -> values[i % values.length], 512);
    }

    /** */
    @Test
    public void testLoadWithoutStreamer() throws Exception {
        int avgDataLen = 500;

        int entriesToLoad = 1_000_000;

        Object[] values = loadData(2000, avgDataLen);

        cacheLoad(entriesToLoad, i -> values[i % values.length], 0);
    }

    /** */
    @Test
    public void testLoadWithoutStreamer2Batched() throws Exception {
        int avgDataLen = 500;

        int entriesToLoad = 1_000_000;

        cacheLoad(entriesToLoad, i -> i, 512);
    }

    /** */
    @Test
    public void testLoadWithoutStreamer2() throws Exception {
        int avgDataLen = 500;

        int entriesToLoad = 1_000_000;

        cacheLoad(entriesToLoad, i -> i, 0);
    }

    /** */
    public void dataStreamerLoad(int entriesToLoad, Function<Integer, Object> val) throws Exception {
        try {
            startGrids(3);

            grid(0).cluster().state(ClusterState.ACTIVE);

            Ignite ldr = startClientGrid(G.allGrids().size());

            try (IgniteDataStreamer<Object, Object> ds = ldr.dataStreamer(DEFAULT_CACHE_NAME)) {
                    ds.allowOverwrite(true);

                for (int e = 0; e < entriesToLoad; ++e)
                    ds.addData(e, val.apply(e));
            }

            assertEquals(grid(0).cache(DEFAULT_CACHE_NAME).size(), entriesToLoad);
        }
        finally {
            G.stopAll(true);
        }
    }

    /** */
    public void cacheLoad(int entriesToLoad, Function<Integer, Object> val, int batchSize) throws Exception {
        try {
            startGrids(3);

            grid(0).cluster().state(ClusterState.ACTIVE);

            Ignite ldr = startClientGrid(G.allGrids().size());

            IgniteCache<Object, Object> cache = ldr.cache(DEFAULT_CACHE_NAME);

            Map<Integer, Object> batch = new HashMap<>();

            for (int e = 0; e < entriesToLoad; ++e) {
                if (batchSize > 0) {
                    batch.put(e, val.apply(e));

                    if (batch.size() == 512) {
                        cache.putAll(batch);

                        batch.clear();
                    }
                }
                else
                    cache.put(e, val.apply(e));
            }

            if (batchSize > 0)
                cache.putAll(batch);

            assertEquals(grid(0).cache(DEFAULT_CACHE_NAME).size(), entriesToLoad);
        }
        finally {
            G.stopAll(true);
        }
    }

    /** */
    private Object[] loadData(int len, int entryLen) {
        Random rnd = new Random();

        Object[] values = new Object[len];

        for (int v = 0; v < len; v++) {
            StringBuilder sb = new StringBuilder();

            for (int ch = 0; ch < entryLen; ++ch)
                sb.append((char)((int)'a' + rnd.nextInt(20)));

            values[v] = sb.toString();
        }

        return values;
    }
}
