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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheMemoryPolicyConfigurationTest extends GridCommonAbstractTest {
    /** */
    private volatile CacheConfiguration ccfg;

    /** */
    private volatile MemoryConfiguration memCfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (memCfg != null)
            cfg.setMemoryConfiguration(memCfg);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Verifies that proper exception is thrown when MemoryPolicy is misconfigured for cache.
     */
    public void testMissingMemoryPolicy() throws Exception {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setMemoryPolicyName("nonExistingMemPlc");

        try {
            startGrid(0);
        }
        catch (IgniteCheckedException e) {
            String msg = e.getMessage();

            assertTrue("Not expected exception was thrown: " + e, msg.contains("Requested MemoryPolicy is not configured"));

            return;
        }

        fail("Expected exception was not thrown: missing MemoryPolicy");
    }

    /**
     * Verifies that {@link IgniteOutOfMemoryException} is thrown when cache is configured with too small MemoryPolicy.
     */
    public void testTooSmallMemoryPolicy() throws Exception {
        memCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration dfltPlcCfg = new MemoryPolicyConfiguration();
        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(10 * 1024 * 1024);
        dfltPlcCfg.setMaxSize(10 * 1024 * 1024);

        MemoryPolicyConfiguration bigPlcCfg = new MemoryPolicyConfiguration();
        bigPlcCfg.setName("bigPlc");
        bigPlcCfg.setMaxSize(1024 * 1024 * 1024);

        memCfg.setMemoryPolicies(dfltPlcCfg, bigPlcCfg);
        memCfg.setDefaultMemoryPolicyName("dfltPlc");

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
        memCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration dfltPlcCfg = new MemoryPolicyConfiguration();
        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(1024 * 1024);
        dfltPlcCfg.setMaxSize(1024 * 1024);

        MemoryPolicyConfiguration bigPlcCfg = new MemoryPolicyConfiguration();
        bigPlcCfg.setName("bigPlc");
        bigPlcCfg.setMaxSize(1024 * 1024 * 1024);

        memCfg.setMemoryPolicies(dfltPlcCfg, bigPlcCfg);
        memCfg.setDefaultMemoryPolicyName("dfltPlc");

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setMemoryPolicyName("bigPlc");

        IgniteEx ignite0 = startGrid(0);

        IgniteCache<Object, Object> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        try {
            for (int i = 0; i < 500_000; i++)
                cache.put(i, "abc");
        }
        catch (Exception e) {
            fail("With properly sized MemoryPolicy no exceptions are expected to be thrown.");
        }
    }
}
