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

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS;

/**
 * Test cache destroy.
 */
public class DestroyCacheTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(256L * 1024 * 1024)
            ));

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME).setGroupName("grp"),
            new CacheConfiguration("another_cache").setGroupName("grp")
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS, value = "true")
    public void testDestroyCache() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        try (IgniteDataStreamer<Object, Object> streamer2 = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            PageMemoryEx pageMemory = (PageMemoryEx)ignite.cachex(DEFAULT_CACHE_NAME).context().dataRegion().pageMemory();

            long totalPages = pageMemory.totalPages();

            for (int i = 0; i <= totalPages; i++)
                streamer2.addData(i, new byte[pageMemory.pageSize() / 2]);
        }

        ignite.destroyCache(DEFAULT_CACHE_NAME);
    }
}
