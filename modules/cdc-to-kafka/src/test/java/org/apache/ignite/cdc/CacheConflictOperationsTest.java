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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cdc.CDCReplicationTest.Data;
import org.apache.ignite.cdc.conflictplugin.CDCReplicationConfigurationPluginProvider;
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
import static org.apache.ignite.cdc.CDCReplicationTest.checkCRC;
import static org.apache.ignite.cdc.CDCReplicationTest.generateSingleData;

/**
 * Cache conflict operations test.
 */
@RunWith(Parameterized.class)
public class CacheConflictOperationsTest extends GridCommonAbstractTest {
    /** Cache mode. */
    @Parameterized.Parameter
    public CacheAtomicityMode cacheMode;

    /** DC id. */
    @Parameterized.Parameter(1)
    public byte drId;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "cacheMode={0},drId={1}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            {ATOMIC, LOWER_DC},
            {TRANSACTIONAL, LOWER_DC},
            {ATOMIC, GREATER_DC},
            {TRANSACTIONAL, GREATER_DC},
        });
    }

    /** */
    private static IgniteCache<String, Data> cache;

    /** */
    private static IgniteInternalCache<BinaryObject, BinaryObject> cachex;

    /** */
    private static IgniteEx cli;

    /** This DC have a greater priority that {@link #THIS_DC}. */
    private static final byte GREATER_DC = 1;

    /** */
    private static final byte THIS_DC = 2;

    /** This DC have a lower priority that {@link #THIS_DC}. */
    private static final byte LOWER_DC = 3;

    /** */
    private long order;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CDCReplicationConfigurationPluginProvider cfgPlugin = new CDCReplicationConfigurationPluginProvider();

        cfgPlugin.setDrId(THIS_DC);
        cfgPlugin.setCaches(new HashSet<>(Collections.singleton("cache")));
        cfgPlugin.setConflictResolveField(conflictResolveField());

        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(cfgPlugin);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        cli = startClientGrid(2);

        cache = cli.createCache(new CacheConfiguration<String, Data>("cache").setAtomicityMode(cacheMode));
        cachex = cli.cachex("cache");
    }

    /** Tests that regular cache operations works with the conflict resolver when there is no update conflicts. */
    @Test
    public void testThisDCUpdateWithoutConflict() {
        String key = "testUpdateThisDCWithoutConflict";

        put(key);

        put(key);

        remove(key);
    }

    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there is no update conflicts.
     */
    @Test
    public void testThatDCUpdateWithoutConflict() throws Exception {
        String key = "testUpdateThatDCWithoutConflict";

        putx(key(key, drId), drId, true);

        putx(key(key, drId), drId, true);

        removex(key(key, drId), drId, true);
    }


    /**
     * Tests that {@code IgniteInternalCache#*AllConflict} cache operations works with the conflict resolver
     * when there is no update conflicts.
     */
    @Test
    public void testThatDCUpdateReorder() throws Exception {
        String key = "testThatDCUpdateReorder";

        order += 1;

        putx(key(key, drId), drId, order, true);

        putx(key(key, drId), drId, order, false);

        putx(key(key, drId), drId, order - 1, false);

        removex(key(key, drId), drId, order, false);

        removex(key(key, drId), drId, order - 1, false);
    }

    /** Tests cache operations for entry replicated from another DC. */
    @Test
    public void testUpdateThisDCConflict() throws Exception {
        String key = "testUpdateThisDCConflict0";

        putx(key(key, drId), drId, true);

        remove(key(key, drId));

        // Conflict replicated update succeed only if DC has a greater priority than this DC.
        putx(key(key, drId), drId, drId == GREATER_DC);

        key = "testUpdateThisDCConflict1";

        putx(key(key, drId), drId, true);

        put(key(key, drId));

        key = "testUpdateThisDCConflict2";

        put(key(key, drId));

        // Conflict replicated remove succeed only if DC has a greater priority than this DC.
        removex(key(key, drId), drId, drId == GREATER_DC);

        key = "testUpdateThisDCConflict3";

        put(key(key, drId));

        // Conflict replicated update succeed only if DC has a greater priority than this DC.
        putx(key(key, drId), drId, drId == GREATER_DC || conflictResolveField() != null);
    }

    /** */
    private void put(String key) {
        Data newVal = generateSingleData(1);

        cache.put(key, newVal);

        assertEquals(newVal, cache.get(key));

        checkCRC(cache.get(key), 1);
    }

    /** */
    private void putx(String k, byte drId, boolean expectSuccess) throws IgniteCheckedException {
        putx(k, drId, order++, expectSuccess);
    }

    /** */
    private void putx(String k, byte drId, long order, boolean expectSuccess) throws IgniteCheckedException {
        Data oldVal = cache.get(k);
        Data newVal = generateSingleData(1);

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));

        CacheObject val = new CacheObjectImpl(cli.binary().toBinary(newVal), null);

        cachex.putAllConflict(singletonMap(key,
            new GridCacheDrInfo(val,
                new GridCacheVersion(1, order, 1, drId))));

        if (expectSuccess) {
            assertTrue(cache.containsKey(k));

            assertEquals(newVal, cache.get(k));

            checkCRC(cache.get(k), newVal.getIter());
        }
        else {
            assertTrue(cache.containsKey(k) || oldVal == null);

            if (oldVal != null)
                assertEquals(oldVal, cache.get(k));
        }
    }

    /** */
    private void remove(String key) {
        cache.remove(key);

        assertFalse(cache.containsKey(key));
    }

    /** */
    private void removex(String k, byte drId, boolean expectSuccess) throws IgniteCheckedException {
        removex(k, drId, order++, expectSuccess);
    }

    /** */
    private void removex(String k, byte drId, long order, boolean expectSuccess) throws IgniteCheckedException {
        Data oldVal = cache.get(k);

        KeyCacheObject key = new KeyCacheObjectImpl(k, null, cachex.context().affinity().partition(k));

        cachex.removeAllConflict(singletonMap(key,
                new GridCacheVersion(1, order, 1, drId)));

        if (expectSuccess)
            assertFalse(cache.containsKey(k));
        else {
            assertTrue(cache.containsKey(k) || oldVal == null);

            if (oldVal != null)
                assertEquals(oldVal, cache.get(k));
        }
    }

    /** */
    private String key(String key, byte drId) {
        return key + drId + cacheMode;
    }

    /** */
    protected String conflictResolveField() {
        return null;
    }
}
