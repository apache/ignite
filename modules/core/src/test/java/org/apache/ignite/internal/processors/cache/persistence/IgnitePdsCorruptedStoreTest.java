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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePdsCorruptedStoreTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME1 = "cache1";

    /** */
    private static final String CACHE_NAME2 = "cache2";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(100 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME1), cacheConfiguration(CACHE_NAME2));

        return cfg;
    }

    /**
     * @return File or folder in work directory.
     * @throws IgniteCheckedException If failed to resolve file name.
     */
    private File file(String file) throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), file, false);
    }

    /**
     * Create cache configuration.
     *
     * @param name Cache name.
     */
    private CacheConfiguration cacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(name);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setBackups(2);

        return ccfg;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNodeInvalidatedWhenPersistenceIsCorrupted() throws Exception {
        Ignite ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, String> cache1 = ignite.cache(CACHE_NAME1);

        for (int i = 0; i < 100; ++i)
            cache1.put(i, String.valueOf(i));

        forceCheckpoint();

        cache1.put(2, "test");

        String nodeName = ignite.name().replaceAll("\\.", "_");

        stopAllGrids();

        U.delete(file(String.format("db/%s/cache-%s/part-2.bin", nodeName, CACHE_NAME1)));

        ignite = startGrid(1);

        try {
            startGrid(0);
        }
        catch (IgniteCheckedException ex) {
            // expected

            throw ex;
        }

        IgniteCache<Integer, Integer> cache2 = ignite.cache(CACHE_NAME2);

        // check that partition map exchange had been finished
        cache2.put(1, 1);

        assertEquals(1, ignite.cluster().nodes().size());
    }
}
