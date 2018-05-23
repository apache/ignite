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

import java.io.File;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;

/**
 *
 */
public class IgnitePdsCorruptedCacheDataTest extends GridCommonAbstractTest {
    /** Test cache name. */
    private static final String TEST_CACHE = "test_cache";

    /** Start grid with known cache factory. */
    private boolean withFactory;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setPersistenceEnabled(true)
        );

        cfg.setDataStorageConfiguration(dsCfg);
        cfg.setCacheConfiguration(getCacheConfiguration());
        cfg.setClassLoader(withFactory ? getExternalClassLoader() : U.gridClassLoader());

        return cfg;
    }

    /**
     * @return Cache configuration.
     * @throws Exception if failed.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration getCacheConfiguration() throws Exception {
        CacheConfiguration cacheCfg = new CacheConfiguration(TEST_CACHE);

        if (withFactory) {
            Factory storeFactory = (Factory)getExternalClassLoader()
                .loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestStoreFactory")
                .newInstance();

            cacheCfg.setCacheStoreFactory(storeFactory);
        }

        return cacheCfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testFilePageStoreManagerShouldThrowExceptionWhenFactoryClassCannotBeLoaded() throws Exception {
        withFactory = true;

        IgniteEx ignite = (IgniteEx)startGrid();

        ignite.cluster()
            .active(true);

        IgniteCache<Integer, String> cache = ignite.cache(TEST_CACHE);

        cache.put(1, "test value");

        GridCacheSharedContext sharedCtx = ignite.context().cache().context();
        FilePageStoreManager pageStore = (FilePageStoreManager)sharedCtx.pageStore();

        assertNotNull(pageStore);

        File cacheData = new File(
            pageStore.cacheWorkDir(ignite.context().cache().cacheConfiguration(TEST_CACHE)),
            CACHE_DATA_FILENAME
        );

        assertTrue(cacheData.exists());

        stopGrid();

        withFactory = false;

        GridTestUtils.assertThrowsAnyCause(
            new NullLogger(),
            this::startGrid,
            IgniteCheckedException.class,
            "An error occurred during cache configuration loading from file"
        );

        assertTrue(cacheData.delete());

        ignite = (IgniteEx)startGrid();

        ignite.cluster()
            .active(true);

        cache = ignite.cache(TEST_CACHE);

        assertEquals("test value", cache.get(1));
    }
}