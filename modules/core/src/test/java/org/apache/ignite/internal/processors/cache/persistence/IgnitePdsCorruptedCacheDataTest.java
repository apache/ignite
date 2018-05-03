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

import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePdsCorruptedCacheDataTest extends GridCommonAbstractTest {
    /** Start grid with known cache factory. */
    private boolean withFactory = true;

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
        CacheConfiguration cacheCfg = new CacheConfiguration("test_cache");

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
        startGrid()
            .cluster()
            .active(true);

        stopGrid();

        withFactory = false;

        GridTestUtils.assertThrowsAnyCause(
            new NullLogger(),
            () -> {
                startGrid();
                return null;
            },
            IgniteCheckedException.class,
            "An error occurred during cache configuration loading from file"
        );
    }
}
