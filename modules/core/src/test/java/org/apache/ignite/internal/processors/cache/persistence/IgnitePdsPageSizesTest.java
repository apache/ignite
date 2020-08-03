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

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DISK_PAGE_COMPRESSION;

/**
 *
 */
public class IgnitePdsPageSizesTest extends GridCommonAbstractTest {
    /** Cache name. */
    private final String cacheName = "cache";

    /** */
    private int pageSize;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(pageSize);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setCacheConfiguration(
            new CacheConfiguration(cacheName)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        cfg.setFailureDetectionTimeout(20_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_1k() throws Exception {
        checkPageSize(1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_2k() throws Exception {
        checkPageSize(2 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_4k() throws Exception {
        checkPageSize(4 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_8k() throws Exception {
        checkPageSize(8 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_16k() throws Exception {
        checkPageSize(16 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    private void checkPageSize(int pageSize) throws Exception {
        if (pageSize <= 4 * 1024 &&
            IgniteSystemProperties.getEnum(DiskPageCompression.class, IGNITE_DEFAULT_DISK_PAGE_COMPRESSION) != null)
            return; // Small pages do not work with compression.

        this.pageSize = pageSize;

        IgniteEx ignite = startGrid(0);

        ignite.active(true);

        try {
            final IgniteCache<Object, Object> cache = ignite.cache(cacheName);
            final long endTime = System.currentTimeMillis() + GridTestUtils.SF.applyLB(60_000, 10_000);

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Random rnd = ThreadLocalRandom.current();

                    while (System.currentTimeMillis() < endTime) {
                        for (int i = 0; i < 500; i++)
                            cache.put(rnd.nextInt(100_000), rnd.nextInt());
                    }

                    return null;
                }
            }, 16, "runner");
        }
        finally {
            stopAllGrids();
        }
    }
}
