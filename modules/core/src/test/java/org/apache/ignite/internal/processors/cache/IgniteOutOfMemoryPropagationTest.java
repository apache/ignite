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

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteOutOfMemoryPropagationTest extends GridCommonAbstractTest {

    public static final int NODES = 3;
    public static final String DEFAULT_CACHE_NAME = "default";
    private CacheAtomicityMode atomicityMode;
    private CacheMode mode;
    private int backupdsCount;
    private CacheWriteSynchronizationMode writeSyncMode;
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        assert G.allGrids().isEmpty();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    public void testPutOOMPropagation() throws Exception {
        testOOMPropagation(false);
    }

    public void testStreamerOOMPropagation() throws Exception {
       testOOMPropagation(true);
    }

    /**
     *
     */
    private void testOOMPropagation(boolean useStreamer) throws Exception {
        Throwable t = null;
        for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
            for (CacheMode cacheMode : CacheMode.values()) {
                for (CacheWriteSynchronizationMode writeSyncMode : CacheWriteSynchronizationMode.values()) {
                    for (int backupsCount = 0; backupsCount < 1; backupsCount++) {
                        if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC
                            || cacheMode == CacheMode.REPLICATED)
                            continue;

                        initGrid(atomicityMode, cacheMode, writeSyncMode, backupsCount);
                        try {
                            forceOOM(useStreamer);
                        }
                        catch (Throwable t0) {
                            t = t0;
                            assertTrue(X.hasCause(t, IgniteOutOfMemoryException.class));
                        }
                        finally {
                            assertNotNull(t);
                            stopAllGrids();
                        }
                    }
                }
            }
        }
    }

    /**
     * Ignite grid of 3 server nodes with passed parameters.
     *
     * @param atomicityMode atomicity mode
     * @param mode cache mode
     * @param writeSyncMode cache write synchronization mode
     * @param backupsCount backups count
     * @throws Exception
     */
    private void initGrid(CacheAtomicityMode atomicityMode, CacheMode mode,
        CacheWriteSynchronizationMode writeSyncMode, int backupsCount) throws Exception {

        System.out.println("Starting grid: CacheAtomicityMode." + atomicityMode +
            " CacheMode." + mode + " CacheWriteSynchronizationMode." + writeSyncMode + " backupsCount = " + backupsCount);
        this.atomicityMode = atomicityMode;
        this.mode = mode;
        this.backupdsCount = backupsCount;
        this.writeSyncMode = writeSyncMode;

        Ignition.setClientMode(false);

        startGridsMultiThreaded(NODES);

        Ignition.setClientMode(true);

        client = startGrid(NODES + 1);
    }

    public void forceOOM(boolean useStreamer) throws Exception {
        final IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        IgniteDataStreamer<String, String> streamer = client.dataStreamer(DEFAULT_CACHE_NAME);

        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            map.put("k" + i, "v" + i);

            if (map.size() > 1_000) {
                if (useStreamer)
                    streamer.addData(map);
                else
                    cache.putAll(map);

                map.clear();
            }
        }

        if (map.size() > 0)
            if (useStreamer)
                streamer.addData(map);
            else
                cache.putAll(map);

    }

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        memCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(20
                * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Object, Object> baseCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        baseCfg.setAtomicityMode(this.atomicityMode);
        baseCfg.setCacheMode(this.mode);
        baseCfg.setBackups(this.backupdsCount);
        baseCfg.setWriteSynchronizationMode(this.writeSyncMode);

        cfg.setCacheConfiguration(baseCfg);

        return cfg;
    }
}
