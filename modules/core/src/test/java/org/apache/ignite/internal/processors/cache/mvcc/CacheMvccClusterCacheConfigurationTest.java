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
package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessorImpl.DISABLE_MVCC_SUPPORT_SYSTEM_PROPERTY;

/**
 * Tests for cluster-wide MVCC cache validation.
 */
@SuppressWarnings("unchecked")
public class CacheMvccClusterCacheConfigurationTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreconfiguredCacheMvccNotStarted() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        IgniteConfiguration cfg = getConfiguration();
        cfg.setCacheConfiguration(ccfg);

        IgniteEx node = startGrid(cfg);

        IgniteCache cache = node.cache(ccfg.getName());

        cache.put(1, 1);
        cache.put(1, 2);

        assertFalse(mvccEnabled(node));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreconfiguredCacheMvccStarted() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);

        IgniteConfiguration cfg = getConfiguration();
        cfg.setCacheConfiguration(ccfg);

        IgniteEx node = startGrid(cfg);

        IgniteCache cache = node.cache(ccfg.getName());

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPersistedMvccStartedWithDynamicCache() throws Exception {
        persistence = true;

        IgniteEx node = startGrid(1);

        assertFalse(mvccEnabled(node));

        node.cluster().active(true);

        assertFalse(mvccEnabled(node));

        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);

        IgniteCache cache = node.createCache(ccfg);

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));

        stopGrid(1);

        node = startGrid(1);

        // Should be started before cluster activation
        assertTrue(mvccEnabled(node));

        node.cluster().active(true);

        cache = node.cache(ccfg.getName());

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccStartedWithDynamicCache() throws Exception {
        IgniteEx node = startGrid(1);

        assertFalse(mvccEnabled(node));

        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);

        IgniteCache cache = node.createCache(ccfg);

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));

        stopGrid(1);

        node = startGrid(1);

        // Should not be started because we do not have persistence enabled
        assertFalse(mvccEnabled(node));

        cache = node.createCache(ccfg);

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldNodeConnectsToNewClusterMvccEnabled() throws Exception  {
        IgniteEx newNode1 = startGrid(1);
        IgniteEx newNode2 = startGrid(2);

        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);

        IgniteCache cache = newNode1.createCache(ccfg);

        cache.put(1, 1);
        cache.put(1, 2);

        // Expect old node can not join topology if at least one MVCC cache started.
        IgniteEx oldNode = null;

        System.setProperty(DISABLE_MVCC_SUPPORT_SYSTEM_PROPERTY, "true");

        try {
            oldNode = startGrid(3);

            fail();
        }
        catch (Exception e) {
            IgniteSpiException ex = X.cause(e, IgniteSpiException.class);

            assertNotNull(ex);
            assertTrue(ex.getMessage(),ex.getMessage() != null &&
                ex.getMessage().contains("Failed to add node to topology because MVCC " +
                "is enabled on cluster and the node doesn't support MVCC or MVCC is disabled for the node"));
        }
        finally {
            System.clearProperty(DISABLE_MVCC_SUPPORT_SYSTEM_PROPERTY);
        }

        cache.put(1, 1);
        cache.put(1, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldNodeConnectsToNewClusterMvccDisabled() throws Exception  {
        IgniteEx newNode1 = startGrid(1);
        IgniteEx newNode2 = startGrid(2);

        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL); // Set not mvcc mode.

        IgniteCache cache = newNode1.createCache(ccfg);

        cache.put(1, 1);
        cache.put(1, 2);

        // Expect old node can join topology if no MVCC caches started.
        IgniteEx oldNode = null;

        System.setProperty(DISABLE_MVCC_SUPPORT_SYSTEM_PROPERTY, "true");

        try {
            oldNode = startGrid(3);
        }
        finally {
            System.clearProperty(DISABLE_MVCC_SUPPORT_SYSTEM_PROPERTY);
        }

        cache.put(1, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldNodeConnectsToNewClusterDynamicMvccCacheStart() throws Exception  {
        IgniteEx newNode1 = startGrid(1);

        IgniteEx oldNode = null;

        System.setProperty(DISABLE_MVCC_SUPPORT_SYSTEM_PROPERTY, "true");

        try {
            oldNode = startGrid(2);
        }
        finally {
            System.clearProperty(DISABLE_MVCC_SUPPORT_SYSTEM_PROPERTY);
        }

        // We can not start MVCC cache when old node are in cluster.
        try {
            CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);

            IgniteCache cache = newNode1.createCache(ccfg);

            cache.put(1, 1);
            cache.put(1, 2);
        }
        catch (Exception e) {
            assertEquals(IgniteException.class, e.getClass());
            assertNotNull(e.getMessage());
            assertEquals("Cannot start cache with MVCC transactional snapshot when there some nodes " +
                "in cluster do not support it.", e.getMessage());
        }

        // But we might do it when all old nodes left the cluster
        stopGrid(2);

        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);

        IgniteCache cache = newNode1.createCache(ccfg);

        cache.put(1, 1);
        cache.put(1, 2);
    }

    /**
     * @param node Node.
     * @return {@code True} if {@link MvccProcessor} is started.
     */
    private boolean mvccEnabled(IgniteEx node) {
        return node.context().coordinators().mvccEnabled();
    }
}
