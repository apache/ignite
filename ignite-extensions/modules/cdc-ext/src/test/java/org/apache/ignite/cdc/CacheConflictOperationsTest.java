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

package org.apache.ignite.cdc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Cache conflict operations test.
 */
@RunWith(Parameterized.class)
public class CacheConflictOperationsTest extends GridCommonAbstractTest {
    /** Cache mode. */
    @Parameterized.Parameter
    public CacheAtomicityMode cacheMode;

    /** Other cluster id. */
    @Parameterized.Parameter(1)
    public byte otherClusterId;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "cacheMode={0}, otherClusterId={1}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode mode : EnumSet.of(ATOMIC, TRANSACTIONAL))
            for (byte otherClusterId : new byte[] {FIRST_CLUSTER_ID, THIRD_CLUSTER_ID})
                params.add(new Object[] {mode, otherClusterId});

        return params;
    }

    /** */
    private static IgniteCache<String, ConflictResolvableTestData> cache;

    /** */
    private static IgniteInternalCache<BinaryObject, BinaryObject> cachex;

    /** */
    private static IgniteEx client;

    /** */
    private static final byte FIRST_CLUSTER_ID = 1;

    /** */
    private static final byte SECOND_CLUSTER_ID = 2;

    /** */
    private static final byte THIRD_CLUSTER_ID = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheVersionConflictResolverPluginProvider<?> pluginCfg = new CacheVersionConflictResolverPluginProvider<>();

        pluginCfg.setClusterId(SECOND_CLUSTER_ID);
        pluginCfg.setCaches(new HashSet<>(Collections.singleton(DEFAULT_CACHE_NAME)));
        pluginCfg.setConflictResolveField(conflictResolveField());

        return super.getConfiguration(igniteInstanceName).setPluginProviders(pluginCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);

        client = startClientGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        cache = null;
        cachex = null;
        client = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (cachex == null || cachex.configuration().getAtomicityMode() != cacheMode) {
            if (cachex != null)
                client.cache(DEFAULT_CACHE_NAME).destroy();

            cache = client.createCache(new CacheConfiguration<String, ConflictResolvableTestData>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(cacheMode));

            cachex = client.cachex(DEFAULT_CACHE_NAME);
        }
    }

    /** Tests that regular cache operations works with the conflict resolver when there is no update conflicts. */
    @Test
    public void testSimpleUpdates() {
        String key = "UpdatesWithoutConflict";

        put(key);
        put(key);

        remove(key);
    }

    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there is no update conflicts.
     */
    @Test
    public void testUpdatesFromOtherClusterWithoutConflict() throws Exception {
        String key = key("UpdateFromOtherClusterWithoutConflict", otherClusterId);

        putConflict(key, 1, true);

        putConflict(key, 2, true);

        removeConflict(key, 3, true);
    }

    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there are update conflicts.
     */
    @Test
    public void testUpdatesReorderFromOtherCluster() throws Exception {
        String key = key("UpdateClusterUpdateReorder", otherClusterId);

        putConflict(key, 2, true);

        // Update with the equal or lower order should ignored.
        putConflict(key, 2, false);
        putConflict(key, 1, false);

        // Remove with the equal or lower order should ignored.
        removeConflict(key, 2, false);
        removeConflict(key, 1, false);

        // Remove with the higher order should succeed.
        putConflict(key, 3, true);

        key = key("UpdateClusterUpdateReorder2", otherClusterId);

        int order = 1;

        putConflict(key, new GridCacheVersion(2, order, 1, otherClusterId), true);

        // Update with the equal or lower topVer should ignored.
        putConflict(key, new GridCacheVersion(2, order, 1, otherClusterId), false);
        putConflict(key, new GridCacheVersion(1, order, 1, otherClusterId), false);

        // Remove with the equal or lower topVer should ignored.
        removeConflict(key, new GridCacheVersion(2, order, 1, otherClusterId), false);
        removeConflict(key, new GridCacheVersion(1, order, 1, otherClusterId), false);

        // Remove with the higher topVer should succeed.
        putConflict(key, new GridCacheVersion(3, order, 1, otherClusterId), true);

        key = key("UpdateClusterUpdateReorder3", otherClusterId);

        int topVer = 1;

        putConflict(key, new GridCacheVersion(topVer, order, 2, otherClusterId), true);

        // Update with the equal or lower nodeOrder should ignored.
        putConflict(key, new GridCacheVersion(topVer, order, 2, otherClusterId), false);
        putConflict(key, new GridCacheVersion(topVer, order, 1, otherClusterId), false);

        // Remove with the equal or lower nodeOrder should ignored.
        removeConflict(key, new GridCacheVersion(topVer, order, 2, otherClusterId), false);
        removeConflict(key, new GridCacheVersion(topVer, order, 1, otherClusterId), false);

        // Remove with the higher nodeOrder should succeed.
        putConflict(key, new GridCacheVersion(topVer, order, 3, otherClusterId), true);
    }

    /** Tests cache operations for entry replicated from another cluster. */
    @Test
    public void testUpdatesConflict() throws Exception {
        String key = key("UpdateThisClusterConflict0", otherClusterId);

        putConflict(key, 1, true);

        // Local remove for other cluster entry should succeed.
        remove(key);

        // Conflict replicated update should ignored.
        // Resolve by field value not applicable because after remove operation "old" value doesn't exists.
        putConflict(key, 2, false);

        key = key("UpdateThisDCConflict1", otherClusterId);

        putConflict(key, 3, true);

        // Local update for other cluster entry should succeed.
        put(key);

        key = key("UpdateThisDCConflict2", otherClusterId);

        put(key);

        // Conflict replicated remove should ignored.
        removeConflict(key, 4, false);

        key = key("UpdateThisDCConflict3", otherClusterId);

        put(key);

        // Conflict replicated update succeed only if resolved by field.
        putConflict(key, 5, conflictResolveField() != null);
    }

    /** */
    private void put(String key) {
        ConflictResolvableTestData newVal = ConflictResolvableTestData.create();

        CacheEntry<String, ConflictResolvableTestData> oldEntry = cache.getEntry(key);

        cache.put(key, newVal);

        CacheEntry<String, ConflictResolvableTestData> newEntry = cache.getEntry(key);

        assertNull(((CacheEntryVersion)newEntry.version()).otherClusterVersion());
        assertEquals(newVal, cache.get(key));

        if (oldEntry != null)
            assertTrue(((CacheEntryVersion)oldEntry.version()).order() < ((CacheEntryVersion)newEntry.version()).order());
    }

    /** Puts entry via {@link IgniteInternalCache#putAllConflict(Map)}. */
    private void putConflict(String k, long order, boolean success) throws IgniteCheckedException {
        putConflict(k, new GridCacheVersion(1, order, 1, otherClusterId), success);
    }

    /** Puts entry via {@link IgniteInternalCache#putAllConflict(Map)}. */
    private void putConflict(String k, GridCacheVersion newVer, boolean success) throws IgniteCheckedException {
        CacheEntry<String, ConflictResolvableTestData> oldEntry = cache.getEntry(k);
        ConflictResolvableTestData newVal = ConflictResolvableTestData.create();

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));
        CacheObject val = new CacheObjectImpl(client.binary().toBinary(newVal), null);

        cachex.putAllConflict(singletonMap(key, new GridCacheDrInfo(val, newVer)));

        if (success) {
            assertEquals(newVer, ((GridCacheVersion)cache.getEntry(k).version()).conflictVersion());
            assertEquals(newVal, cache.get(k));
        }
        else if (oldEntry != null) {
            assertEquals(oldEntry.getValue(), cache.get(k));
            assertEquals(oldEntry.version(), cache.getEntry(k).version());
        }
    }

    /** */
    private void remove(String key) {
        assertTrue(cache.containsKey(key));

        cache.remove(key);

        assertFalse(cache.containsKey(key));
    }

    /** Removes entry via {@link IgniteInternalCache#removeAllConflict(Map)}. */
    private void removeConflict(String k, long order, boolean success) throws IgniteCheckedException {
        removeConflict(k, new GridCacheVersion(1, order, 1, otherClusterId), success);
    }

    /** Removes entry via {@link IgniteInternalCache#removeAllConflict(Map)}. */
    private void removeConflict(String k, GridCacheVersion ver, boolean success) throws IgniteCheckedException {
        assertTrue(cache.containsKey(k));

        CacheEntry<String, ConflictResolvableTestData> oldEntry = cache.getEntry(k);

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));

        cachex.removeAllConflict(singletonMap(key, ver));

        if (success)
            assertFalse(cache.containsKey(k));
        else if (oldEntry != null) {
            assertEquals(oldEntry.getValue(), cache.get(k));
            assertEquals(oldEntry.version(), cache.getEntry(k).version());
        }
    }

    /** */
    private String key(String key, byte otherClusterId) {
        return key + otherClusterId + cacheMode;
    }

    /** */
    protected String conflictResolveField() {
        return null;
    }
}
