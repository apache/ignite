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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
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
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.AbstractCachePluginProvider;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CdcCacheVersionTest extends AbstractCdcTest {
    /** */
    public static final String FOR_OTHER_CLUSTER_ID = "for-other-cluster-id";

    /** */
    public static final byte DFLT_CLUSTER_ID = 1;

    /** */
    public static final byte OTHER_CLUSTER_ID = 2;

    /** */
    public static final int KEY_TO_UPD = 42;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setCdcEnabled(true)
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "ConflictResolverProvider";
            }

            @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
                if (!ctx.igniteCacheConfiguration().getName().equals(FOR_OTHER_CLUSTER_ID))
                    return null;

                return new AbstractCachePluginProvider() {
                    @Override public @Nullable Object createComponent(Class cls) {
                        if (cls != CacheConflictResolutionManager.class)
                            return null;

                        return new TestCacheConflictResolutionManager();
                    }
                };
            }
        });

        return cfg;
    }

    /** Simplest CDC test with usage of {@link IgniteInternalCache#putAllConflict(Map)}. */
    @Test
    public void testReadAllKeysFromOtherCluster() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-conflict-resolver");

        IgniteEx ign = startGrid(cfg);

        ign.context().cache().context().versions().dataCenterId(DFLT_CLUSTER_ID);
        ign.cluster().state(ACTIVE);

        UserCdcConsumer cnsmr = new UserCdcConsumer() {
            @Override public void checkEvent(CdcEvent evt) {
                assertEquals(DFLT_CLUSTER_ID, evt.version().clusterId());
                assertEquals(OTHER_CLUSTER_ID, evt.version().otherClusterVersion().clusterId());
            }
        };

        CdcMain cdc = new CdcMain(cfg, null, cdcConfig(cnsmr));

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(FOR_OTHER_CLUSTER_ID);

        addAndWaitForConsumption(cnsmr, cdc, cache, null, this::addConflictData, 0, KEYS_CNT, getTestTimeout());
    }

    /** */
    @Test
    public void testOrderIncrease() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteEx ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        AtomicLong updCntr = new AtomicLong(0);

        CdcConsumer cnsmr = new CdcConsumer() {
            private long order = -1;

            @Override public boolean onEvents(Iterator<CdcEvent> evts) {
                evts.forEachRemaining(evt -> {
                    assertEquals(KEY_TO_UPD, evt.key());

                    assertTrue(evt.version().order() > order);

                    order = evt.version().order();

                    updCntr.incrementAndGet();
                });

                return true;
            }

            @Override public void start() {
                // No-op.
            }

            @Override public void stop() {
                // No-op.
            }
        };

        CdcMain cdc = new CdcMain(cfg, null, cdcConfig(cnsmr));

        IgniteCache<Integer, User> cache = ign.getOrCreateCache("my-cache");

        IgniteInternalFuture<?> fut = runAsync(cdc);

        // Update the same key several time.
        // Expect {@link CacheEntryVersion#order()} will monotically increase.
        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(KEY_TO_UPD, createUser(i));

        assertTrue(waitForCondition(() -> updCntr.get() == KEYS_CNT, getTestTimeout()));

        fut.cancel();
    }

    /** */
    private void addConflictData(IgniteCache<Integer, User> cache, int from, int to) {
        try {
            IgniteEx ign = (IgniteEx)G.allGrids().get(0);

            IgniteInternalCache<Integer, User> intCache = ign.cachex(cache.getName());

            Map<KeyCacheObject, GridCacheDrInfo> drMap = new HashMap<>();

            for (int i = from; i < to; i++) {
                KeyCacheObject key = new KeyCacheObjectImpl(i, null, intCache.affinity().partition(i));
                CacheObject val =
                    new CacheObjectImpl(createUser(i), null);

                val.prepareMarshal(intCache.context().cacheObjectContext());

                drMap.put(key, new GridCacheDrInfo(val, new GridCacheVersion(1, i, 1, OTHER_CLUSTER_ID)));
            }

            intCache.putAllConflict(drMap);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public static class TestCacheConflictResolutionManager<K, V> extends GridCacheManagerAdapter<K, V>
        implements CacheConflictResolutionManager<K, V> {

        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return new CacheVersionConflictResolver() {
                @Override public <K1, V1> GridCacheVersionConflictContext<K1, V1> resolve(
                    CacheObjectValueContext ctx,
                    GridCacheVersionedEntryEx<K1, V1> oldEntry,
                    GridCacheVersionedEntryEx<K1, V1> newEntry,
                    boolean atomicVerComparator
                ) {
                    GridCacheVersionConflictContext<K1, V1> res =
                        new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

                    res.useNew();

                    return res;
                }
            };
        }
    }
}
