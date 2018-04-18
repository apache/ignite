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

import java.util.concurrent.Callable;
import javax.cache.configuration.Factory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePdsCorruptedCacheDataTest extends GridCommonAbstractTest {
    /** Error message. */
    private static final String ERR_MESSAGE = "An error occurred during cache configuration loading from given file. " +
        "Make sure that user library containing required class is valid. " +
        "If library is valid then delete cache configuration file and restart cache";

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
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setPersistenceEnabled(true)
        );

        cfg.setDataStorageConfiguration(dsCfg);
        cfg.setClassLoader(getExternalClassLoader());
        cfg.setCacheConfiguration(getCacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     * @throws Exception if failed.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration getCacheConfiguration() throws Exception {
        CacheConfiguration cacheCfg = new CacheConfiguration("test_cache");

        Factory storeFactory = (Factory)getExternalClassLoader()
            .loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestStoreFactory")
            .newInstance();

        cacheCfg.setCacheStoreFactory(storeFactory);

        return cacheCfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testFilePageStoreManagerShouldThrowExceptionWhenFactoryClassCannotBeLoaded() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrid();

        ignite.cluster().active(true);

        ignite.context().config().setClassLoader(U.gridClassLoader());

        GridCacheSharedContext sharedCtx = ignite.context().cache().context();
        FilePageStoreManager pageStore = (FilePageStoreManager)sharedCtx.pageStore();

        assertNotNull(pageStore);

        Throwable e = GridTestUtils.assertThrowsWithCause(() -> {
                pageStore.readCacheConfigurations();

                return null;
            },
            ClassNotFoundException.class);

        assertNotNull(e);

        assertTrue(e.getMessage().startsWith(ERR_MESSAGE));
    }
}
