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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.AbstractCdcTest;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cdc.AbstractCdcTest.KEYS_CNT;
import static org.apache.ignite.cdc.CdcSelfTest.addData;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.stopThreads;
import static org.apache.ignite.util.CdcCommandTest.CDC;
import static org.apache.ignite.util.CdcCommandTest.RESEND;
import static org.apache.ignite.util.CdcCommandTest.runCdc;
import static org.apache.ignite.util.CdcCommandTest.waitForSize;
import static org.apache.ignite.util.GridCommandHandlerClusterByClassTest.CACHES;

/**
 * CDC resend command tests.
 */
public class CdcResendCommandTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(1000)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setCdcEnabled(true)
                .setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setBackups(1));

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "ConflictResolverProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (cls != CacheConflictResolutionManager.class)
                    return null;

                return (T)new AlwaysNewResolutionManager<>();
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopThreads(log);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testResendCacheDataRestoreFromWal() throws Exception {
        IgniteEx ign = startGrid(0);

        ign.cluster().state(ACTIVE);

        enableCheckpoints(ign, false);

        addData(ign.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        AbstractCdcTest.UserCdcConsumer cnsmr = runCdc(ign);

        waitForSize(cnsmr, KEYS_CNT);

        cnsmr.clear();

        executeCommand(EXIT_CODE_OK, CDC, RESEND, CACHES, DEFAULT_CACHE_NAME);

        waitForSize(cnsmr, KEYS_CNT);

        stopAllGrids();

        ign = startGrid(0);

        assertEquals(KEYS_CNT, ign.cache(DEFAULT_CACHE_NAME).size());
    }

    /** */
    @Test
    public void testResendConflictVersion() throws Exception {
        IgniteEx ign = startGrid(0);

        ign.cluster().state(ACTIVE);

        enableCheckpoints(ign, false);

        IgniteInternalCache<Integer, Integer> cachex = ign.cachex(DEFAULT_CACHE_NAME);

        // Put data.
        cachex.put(0, 0);

        // Override data from clusterId=2.
        KeyCacheObject key = new KeyCacheObjectImpl(0, null, cachex.affinity().partition(0));
        CacheObject val = new CacheObjectImpl(1, null);
        val.prepareMarshal(cachex.context().cacheObjectContext());

        GridCacheVersion conflict = new GridCacheVersion(1, 0, 1, (byte)2);

        Map<KeyCacheObject, GridCacheDrInfo> drMap = new HashMap<>();
        drMap.put(key, new GridCacheDrInfo(val, conflict));

        cachex.putAllConflict(drMap);

        // Resend data.
        executeCommand(EXIT_CODE_OK, CDC, RESEND, CACHES, DEFAULT_CACHE_NAME);

        TestCdcConsumer cnsmr = new TestCdcConsumer();

        CdcConfiguration cfg = new CdcConfiguration();
        cfg.setConsumer(cnsmr);

        CdcMain cdc = new CdcMain(ign.configuration(), null, cfg);
        GridTestUtils.runAsync(cdc);

        assertTrue(GridTestUtils.waitForCondition(() -> cnsmr.events().size() == 3, 10_000, 100));

        CdcEvent ev0 = cnsmr.events().get(0);
        assertEquals(0, ev0.key());
        assertEquals(0, ev0.value());
        assertNull(ev0.version().otherClusterVersion());

        CdcEvent ev1 = cnsmr.events().get(1);
        assertEquals(0, ev1.key());
        assertEquals(1, ev1.value());
        assertEquals(conflict, ev1.version().otherClusterVersion());

        CdcEvent ev2 = cnsmr.events().get(2);
        assertEquals(0, ev2.key());
        assertEquals(1, ev2.value());
        assertNull(ev2.version().otherClusterVersion());
    }

    /** */
    private static class TestCdcConsumer implements CdcConsumer {
        /** */
        private final List<CdcEvent> events = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<CdcEvent> events) {
            synchronized (this) {
                events.forEachRemaining(this.events::add);
            }

            return false;
        }

        /** */
        synchronized List<CdcEvent> events() {
            return events;
        }

        /** {@inheritDoc} */
        @Override public void start(MetricRegistry mreg) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            types.forEachRemaining(t -> {});
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            mappings.forEachRemaining(t -> {});
        }

        /** {@inheritDoc} */
        @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
            cacheEvents.forEachRemaining(t -> {});
        }

        /** {@inheritDoc} */
        @Override public void onCacheDestroy(Iterator<Integer> caches) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            // No-op
        }
    }

    /** */
    private static class AlwaysNewResolutionManager<K, V>
        extends GridCacheManagerAdapter<K, V> implements CacheConflictResolutionManager<K, V> {
        /** */
        private final CacheVersionConflictResolver rslv;

        /** */
        AlwaysNewResolutionManager() {
            rslv = new CacheVersionConflictResolver() {
                @Override public <K1, V1> GridCacheVersionConflictContext<K1, V1> resolve(
                    CacheObjectValueContext ctx,
                    GridCacheVersionedEntryEx<K1, V1> oldEntry,
                    GridCacheVersionedEntryEx<K1, V1> newEntry,
                    boolean atomicVerComparator
                ) {
                    GridCacheVersionConflictContext<K1, V1> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

                    res.useNew();

                    return res;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return rslv;
        }
    }
}
