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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheDataRegionConfigurationTest extends GridCommonAbstractTest {
    /** */
    private volatile CacheConfiguration ccfg;

    /** */
    private volatile DataStorageConfiguration memCfg;

    /** */
    private static final long DFLT_MEM_PLC_SIZE = 10 * 1024 * 1024;

    /** */
    private static final long BIG_MEM_PLC_SIZE = 1024 * 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (memCfg != null)
            cfg.setDataStorageConfiguration(memCfg);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Verifies that proper exception is thrown when DataRegion is misconfigured for cache.
     */
    public void testMissingDataRegion() throws Exception {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setDataRegionName("nonExistingMemPlc");

        try {
            startGrid(0);
        }
        catch (IgniteCheckedException e) {
            String msg = e.getMessage();

            assertTrue("Not expected exception was thrown: " + e, msg.contains("Requested DataRegion is not configured"));

            return;
        }

        fail("Expected exception was not thrown: missing DataRegion");
    }

    /**
     * Verifies that {@link IgniteOutOfMemoryException} is thrown when cache is configured with too small DataRegion.
     */
    public void testTooSmallDataRegion() throws Exception {
        memCfg = new DataStorageConfiguration();

        DataRegionConfiguration dfltPlcCfg = new DataRegionConfiguration();
        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(10 * 1024 * 1024);
        dfltPlcCfg.setMaxSize(10 * 1024 * 1024);

        DataRegionConfiguration bigPlcCfg = new DataRegionConfiguration();
        bigPlcCfg.setName("bigPlc");
        bigPlcCfg.setMaxSize(1024 * 1024 * 1024);

        memCfg.setDataRegionConfigurations(bigPlcCfg);
        memCfg.setDefaultDataRegionConfiguration(dfltPlcCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        IgniteEx ignite0 = startGrid(0);

        IgniteCache<Object, Object> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        boolean oomeThrown = false;

        try {
            for (int i = 0; i < 500_000; i++)
                cache.put(i, "abc");
        }
        catch (Exception e) {
            Throwable cause = e;

            do {
                if (cause instanceof IgniteOutOfMemoryException) {
                    oomeThrown = true;
                    break;
                }

                if (cause == null)
                    break;

                if (cause.getSuppressed() == null || cause.getSuppressed().length == 0)
                    cause = cause.getCause();
                else
                    cause = cause.getSuppressed()[0];
            }
            while (true);
        }

        if (!oomeThrown)
            fail("OutOfMemoryException hasn't been thrown");
    }

    /**
     * Verifies that with enough memory allocated adding values to cache doesn't cause any exceptions.
     */
    public void testProperlySizedMemoryPolicy() throws Exception {
        memCfg = new DataStorageConfiguration();

        DataRegionConfiguration dfltPlcCfg = new DataRegionConfiguration();
        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(DFLT_MEM_PLC_SIZE);
        dfltPlcCfg.setMaxSize(DFLT_MEM_PLC_SIZE);

        DataRegionConfiguration bigPlcCfg = new DataRegionConfiguration();
        bigPlcCfg.setName("bigPlc");
        bigPlcCfg.setMaxSize(BIG_MEM_PLC_SIZE);

        memCfg.setDataRegionConfigurations(bigPlcCfg);
        memCfg.setDefaultDataRegionConfiguration(dfltPlcCfg);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setDataRegionName("bigPlc");

        IgniteEx ignite0 = startGrid(0);

        IgniteCache<Object, Object> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        try {
            for (int i = 0; i < 500_000; i++)
                cache.put(i, "abc");
        }
        catch (Exception e) {
            fail("With properly sized DataRegion no exceptions are expected to be thrown.");
        }
    }
}
