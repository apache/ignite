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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class IgniteClusterActivateDeactivateTestWithPersistence extends IgniteClusterActivateDeactivateTest {
    /** {@inheritDoc} */
    @Override protected boolean persistenceEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        GridTestUtils.deleteDbFiles();
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_SingleNode() throws Exception {
        activateCachesRestore(1, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_SingleNode_WithNewCaches() throws Exception {
        activateCachesRestore(1, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_5_Servers() throws Exception {
        activateCachesRestore(5, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_5_Servers_WithNewCaches() throws Exception {
        activateCachesRestore(5, false);
    }

    /**
     * @param srvs Number of server nodes.
     * @param withNewCaches If {@code true} then after restart has new caches in configuration.
     * @throws Exception If failed.
     */
    private void activateCachesRestore(int srvs, boolean withNewCaches) throws Exception {
        Ignite srv = startGrids(srvs);

        srv.active(true);

        srv.createCaches(Arrays.asList(cacheConfigurations1()));

        Map<Integer, Integer> cacheData = new LinkedHashMap<>();

        for (int i = 1; i <= 100; i++) {
            for (CacheConfiguration ccfg : cacheConfigurations1()) {
                srv.cache(ccfg.getName()).put(-i, i);

                cacheData.put(-i, i);
            }
        }

        stopAllGrids();

        for (int i = 0; i < srvs; i++) {
            if (withNewCaches)
                ccfgs = cacheConfigurations2();

            startGrid(i);
        }

        srv = ignite(0);

        checkNoCaches(srvs);

        srv.active(true);

        final int CACHES = withNewCaches ? 4 : 2;

        for (int i = 0; i < srvs; i++) {
            for (int c = 0; c < CACHES; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
        }

        for (CacheConfiguration ccfg : cacheConfigurations1())
            checkCacheData(cacheData, ccfg.getName());

        checkCaches(srvs, CACHES);

        int nodes = srvs;

        client = false;

        startGrid(nodes++);

        for (int i = 0; i < nodes; i++) {
            for (int c = 0; c < CACHES; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
        }

        checkCaches(nodes, CACHES);

        client = true;

        startGrid(nodes++);

        for (int c = 0; c < CACHES; c++)
            checkCache(ignite(nodes - 1), CACHE_NAME_PREFIX + c, false);

        checkCaches(nodes, CACHES);

        for (int i = 0; i < nodes; i++) {
            for (int c = 0; c < CACHES; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
        }

        for (CacheConfiguration ccfg : cacheConfigurations1())
            checkCacheData(cacheData, ccfg.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCacheRestoreConfigurationConflict() throws Exception {
        final int SRVS = 3;

        Ignite srv = startGrids(SRVS);

        srv.active(true);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        srv.createCache(ccfg);

        stopAllGrids();

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME + 1);

        ccfg.setGroupName(DEFAULT_CACHE_NAME);

        ccfgs = new CacheConfiguration[]{ccfg};

        startGrids(SRVS);

        try {
            ignite(0).active(true);

            fail();
        }
        catch (IgniteException e) {
            // Expected error.
        }

        for (int i = 0; i < SRVS; i++)
            assertFalse(ignite(i).active());

        checkNoCaches(SRVS);
    }
}
