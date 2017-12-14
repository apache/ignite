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

package org.apache.ignite.internal.processors.cache.persistence.db;

import com.google.common.base.Strings;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test TTL worker with persistence enabled
 */
public class IgnitePdsWithTtlTest extends GridCommonAbstractTest {
/** */
    public static final String CACHE = "expirableCache";

    /** */
    public static final int DURATION_SEC = 10;

    /** */
    public static final int ENTRIES = 7000;

    /**
     * copy of GridCacheSharedTtlCleanupManager.CLEANUP_WORKER_SLEEP_INTERVAL
     */
    private static final long CLEANUP_WORKER_SLEEP_INTERVAL = 500;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        final CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(CACHE);
        ccfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, DURATION_SEC)));
        ccfg.setEagerTtl(true);
        ccfg.setGroupName("group1");

        final DataStorageConfiguration storCfg = new DataStorageConfiguration();
        final DataRegionConfiguration regCfg = new DataRegionConfiguration();
        regCfg.setPersistenceEnabled(true);
        storCfg.setDefaultDataRegionConfiguration(regCfg);
        cfg.setDataStorageConfiguration(storCfg);

        cfg.setCacheConfiguration(ccfg);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testTtlIsApplied() throws Exception {
        final IgniteEx ignite = startGrid(0);

        ignite.active(true);

        for (int i = 0; i < 1; i++)
            loadAndWaitForCleanup(ignite, i*ENTRIES, false);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testTtlIsAppliedAfterRestart() throws Exception {
        final IgniteEx ignite = startGrid(0);

        ignite.active(true);

        for (int i = 0; i < 1; i++)
            loadAndWaitForCleanup(ignite, i*ENTRIES, true);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    private void loadAndWaitForCleanup(IgniteEx ignite, final int start, boolean restartGrid) throws Exception {
        {
            final IgniteCache<Integer, String> cache = ignite.cache(CACHE);

            putEntriesBatch(cache, start);

            assertEquals(ENTRIES, cache.size());

            System.out.println("After cache puts {{");
            ((IgniteCacheProxy)cache).context().printMemoryStats();
            System.out.println("}} ");
        }
        final long ms = System.currentTimeMillis();
        {
            if(restartGrid) {
                Ignition.stop(ignite.name(), false);
                ignite = startGrid(0);
                ignite.active(true);
            }

            final IgniteCache<Integer, String> cache = ignite.cache(CACHE);
            System.out.println("After restart from LFS {{");
            ((IgniteCacheProxy)cache).context().printMemoryStats();
            System.out.println("}} After restart from LFS");

            if (restartGrid)
                for (int i = 0; i < 50; i++)
                    cache.get(i); // touch entries

            final long timeout = DURATION_SEC * 1000 + CLEANUP_WORKER_SLEEP_INTERVAL;
            Thread.sleep(timeout);

            System.out.println("After sleep {{");
            ((IgniteCacheProxy)cache).context().printMemoryStats();
            System.out.println("}} After sleep");

            assertEquals(0, cache.size());
        }
    }

    //it is possible test will be passing if SQL indexes are enabled
    /**
     * @throws Exception if failed.
     */
    public void testRebalancingWithTtlExpirable() throws Exception {
        final IgniteEx srv1 = startGrid(0);

        srv1.active(true);

        {
            final IgniteCache<Integer, String> cache = srv1.cache(CACHE);

            putEntriesBatch(cache, 0);

            assertEquals(ENTRIES, cache.size());

            System.out.println("After cache puts {{");
            ((IgniteCacheProxy)cache).context().printMemoryStats();
            System.out.println("}} ");
        }


        //causes rebalancing start
        final IgniteEx srv2 = startGrid(1);
        {
            final IgniteCache<Integer, String> cacheOn2 = srv2.cache(CACHE);
            System.out.println("After rebalancing start  {{");
            ((IgniteCacheProxy)cacheOn2).context().printMemoryStats();
            System.out.println("}} After rebalancing start");


            final long timeout = DURATION_SEC * 1000 + CLEANUP_WORKER_SLEEP_INTERVAL;
            Thread.sleep(timeout);


            System.out.println("After timeout {{");
            ((IgniteCacheProxy)cacheOn2).context().printMemoryStats();
            System.out.println("}} After timeout");

            assertEquals(0, cacheOn2.size());
        }

        stopAllGrids();
    }

    /** */
    private void putEntriesBatch(IgniteCache<Integer, String> cache, final int startKey) {
        cache.putAll(new TreeMap<Integer, String>() {{
            for (int i = startKey; i < startKey + ENTRIES; i++)
                put(i, Strings.repeat("Some value " + i, 125));
        }});
    }
}