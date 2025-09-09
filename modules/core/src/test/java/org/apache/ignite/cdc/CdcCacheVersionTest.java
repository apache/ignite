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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.AbstractCachePluginProvider;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager.DATA_VER_CLUSTER_ID;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class CdcCacheVersionTest extends AbstractCdcTest {
    /** */
    public static final byte DFLT_CLUSTER_ID = 1;

    /** */
    public static final byte OTHER_CLUSTER_ID = 2;

    /** */
    public static final int KEY_TO_UPD = 42;

    /** */
    public static final String NOT_CDC = "not-cdc";

    /** */
    public static final String CDC = "cdc";

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(1)
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(2)
    public int gridCnt;

    /** */
    @Parameterized.Parameter(3)
    public boolean persistenceEnabled;

    /** */
    private final AtomicLong walRecCheckedCntr = new AtomicLong();

    /** */
    private final AtomicLong conflictCheckedCntr = new AtomicLong();

    /** */
    private volatile Function<GridKernalContext, IgniteWriteAheadLogManager> walProvider;

    /** */
    private volatile Supplier<CacheVersionConflictResolver> conflictResolutionMgrSupplier;

    /** */
    @Parameterized.Parameters(name = "atomicity={0}, mode={1}, gridCnt={2}, persistenceEnabled={3}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode atomicity : EnumSet.of(ATOMIC, TRANSACTIONAL))
            for (CacheMode mode : EnumSet.of(PARTITIONED, REPLICATED))
                for (int gridCnt : new int[] {1, 3})
                    for (boolean persistenceEnabled : new boolean[] {false, true})
                        params.add(new Object[] {atomicity, mode, gridCnt, persistenceEnabled});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled))
            .setDataRegionConfigurations(
                new DataRegionConfiguration()
                    .setName(CDC)
                    .setPersistenceEnabled(persistenceEnabled)
                    .setCdcEnabled(true),
                new DataRegionConfiguration()
                    .setName(NOT_CDC)
                    .setPersistenceEnabled(false)
                    .setCdcEnabled(false)));

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "ConflictResolverProvider";
            }

            @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
                if (!ctx.igniteCacheConfiguration().getName().equals(DEFAULT_CACHE_NAME))
                    return null;

                return new AbstractCachePluginProvider() {
                    @Override public @Nullable Object createComponent(Class cls) {
                        if (cls != CacheConflictResolutionManager.class || conflictResolutionMgrSupplier == null)
                            return null;

                        return new TestCacheConflictResolutionManager<>();
                    }
                };
            }

            @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteWriteAheadLogManager.class.equals(cls))
                    return (T)walProvider.apply(((IgniteEx)ctx.grid()).context());

                return null;
            }
        });

        return cfg;
    }

    /** Test that conflict version is writtern to WAL. */
    @Test
    public void testConflictVersionWritten() throws Exception {
        walProvider = (ctx) -> new FileWriteAheadLogManager(ctx) {
            @Override public WALPointer log(WALRecord rec) throws IgniteCheckedException {
                if (rec.type() != DATA_RECORD_V2)
                    return super.log(rec);

                DataRecord dataRec = (DataRecord)rec;

                for (int i = 0; i < dataRec.entryCount(); i++) {
                    DataEntry dataEntry = dataRec.writeEntries().get(i);

                    assertEquals(CU.cacheId(DEFAULT_CACHE_NAME), dataEntry.cacheId());
                    assertEquals(DFLT_CLUSTER_ID, dataEntry.writeVersion().dataCenterId());
                    assertNotNull(dataEntry.writeVersion().conflictVersion());
                    assertEquals(OTHER_CLUSTER_ID, dataEntry.writeVersion().conflictVersion().dataCenterId());

                    walRecCheckedCntr.incrementAndGet();
                }

                return super.log(rec);
            }
        };

        conflictResolutionMgrSupplier = () -> new CacheVersionConflictResolver() {
            @Override public <K1, V1> GridCacheVersionConflictContext<K1, V1> resolve(
                CacheObjectValueContext ctx,
                GridCacheVersionedEntryEx<K1, V1> oldEntry,
                GridCacheVersionedEntryEx<K1, V1> newEntry,
                boolean atomicVerComparator
            ) {
                GridCacheVersionConflictContext<K1, V1> res =
                    new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

                res.useNew();

                assertEquals(OTHER_CLUSTER_ID, newEntry.version().dataCenterId());

                if (!oldEntry.isStartVersion())
                    assertEquals(OTHER_CLUSTER_ID, oldEntry.version().dataCenterId());

                conflictCheckedCntr.incrementAndGet();

                return res;
            }

            @Override public String toString() {
                return "TestCacheConflictResolutionManager";
            }
        };

        startGrids(gridCnt);
        IgniteEx cli = startClientGrid(gridCnt);

        for (int i = 0; i < gridCnt; i++) {
            grid(i).context().cache().context().versions().dataCenterId(DFLT_CLUSTER_ID);

            assertEquals(
                DFLT_CLUSTER_ID,
                grid(i).context().metric().registry(CACHE_METRICS).<IntMetric>findMetric(DATA_VER_CLUSTER_ID).value()
            );
        }

        cli.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = cli.getOrCreateCache(
            new CacheConfiguration<Integer, User>(DEFAULT_CACHE_NAME)
                .setCacheMode(cacheMode)
                .setAtomicityMode(atomicityMode)
                .setDataRegionName(CDC)
                .setBackups(Integer.MAX_VALUE));

        if (atomicityMode == ATOMIC)
            putRemoveCheck(cli, cache, null, null);
        else {
            // Check operations for transaction cache without explicit transaction.
            putRemoveCheck(cli, cache, null, null);

            // Check operations for transaction cache with explicit transaction in all modes.
            for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                for (TransactionIsolation isolation : TransactionIsolation.values())
                    putRemoveCheck(cli, cache, concurrency, isolation);
        }

        for (int i = 0; i < gridCnt; i++) {
            boolean dfltCacheFound = false;

            assertFalse(grid(i).context().clientNode());

            SystemView<CacheView> caches = grid(i).context().systemView().view(CACHES_VIEW);

            for (CacheView v : caches) {
                if (v.cacheName().equals(DEFAULT_CACHE_NAME)) {
                    assertEquals(v.conflictResolver(), "TestCacheConflictResolutionManager");

                    dfltCacheFound = true;
                }
                else
                    assertNull(v.conflictResolver());
            }

            assertTrue(dfltCacheFound);
        }

    }

    /** */
    private void putRemoveCheck(
        IgniteEx cli,
        IgniteCache<Integer, User> cache,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation
    ) throws Exception {
        conflictCheckedCntr.set(0);
        walRecCheckedCntr.set(0);

        // Put data with conflict version.
        // Conflict version will be checked during WAL record and conflict resolution.
        addConflictData(cli, cache, 0, KEYS_CNT, concurrency, isolation);

        checkResolverAndWal();

        // Replacing existing data.
        addConflictData(cli, cache, 0, KEYS_CNT, concurrency, isolation);

        checkResolverAndWal();

        // Removing existing data with conflict version.
        removeConflictData(cli, cache, 0, KEYS_CNT, concurrency, isolation);

        checkResolverAndWal();
    }

    /** */
    private void checkResolverAndWal() throws IgniteInterruptedCheckedException {
        // Conflict resolver for ATOMIC caches invoked only on primary.
        long expConflictResolverCnt = atomicityMode == ATOMIC ? KEYS_CNT : (KEYS_CNT * (long)gridCnt);

        if (!waitForCondition(() -> conflictCheckedCntr.get() == expConflictResolverCnt, WAL_ARCHIVE_TIMEOUT))
            fail("Expected " + expConflictResolverCnt + " but was " + conflictCheckedCntr.get());

        long expWalRecCnt = (long)KEYS_CNT * gridCnt;

        if (!waitForCondition(() -> walRecCheckedCntr.get() == expWalRecCnt, WAL_ARCHIVE_TIMEOUT))
            fail("Expected " + expWalRecCnt + " but was " + walRecCheckedCntr.get());

        conflictCheckedCntr.set(0);
        walRecCheckedCntr.set(0);
    }

    /** */
    @Test
    public void testOrderIncrease() throws Exception {
        walProvider = (ctx) -> new FileWriteAheadLogManager(ctx) {
            /** */
            private long prevOrder = -1;

            @Override public WALPointer log(WALRecord rec) throws IgniteCheckedException {
                if (rec.type() != DATA_RECORD_V2)
                    return super.log(rec);

                DataRecord dataRec = (DataRecord)rec;

                for (int i = 0; i < dataRec.entryCount(); i++) {
                    assertEquals(CU.cacheId(DEFAULT_CACHE_NAME), dataRec.get(i).cacheId());
                    assertEquals(KEY_TO_UPD, (int)dataRec.get(i).key().value(null, false));
                    assertTrue(dataRec.get(i).writeVersion().order() > prevOrder);

                    prevOrder = dataRec.get(i).writeVersion().order();

                    walRecCheckedCntr.incrementAndGet();
                }

                return super.log(rec);
            }
        };

        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteEx ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(
            new CacheConfiguration<Integer, User>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(atomicityMode)
                .setDataRegionName(CDC)
                .setCacheMode(cacheMode));

        IgniteCache<Integer, User> notCdcCache = ign.getOrCreateCache(
                new CacheConfiguration<Integer, User>(NOT_CDC).setDataRegionName(NOT_CDC));

        walRecCheckedCntr.set(0);

        // Update the same key several time.
        // Expect {@link CacheEntryVersion#order()} will monotically increase.
        for (int i = 0; i < KEYS_CNT; i++) {
            cache.put(KEY_TO_UPD, createUser(i));
            notCdcCache.put(KEY_TO_UPD, createUser(i));
        }

        assertTrue(waitForCondition(() -> walRecCheckedCntr.get() == KEYS_CNT, getTestTimeout()));
    }

    /** */
    private void addConflictData(
        IgniteEx cli,
        IgniteCache<Integer, User> cache,
        int from,
        int to,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation
    ) {
        try {
            IgniteInternalCache<Integer, User> intCache = cli.cachex(cache.getName());

            Map<KeyCacheObject, GridCacheDrInfo> drMap = new HashMap<>();

            for (int i = from; i < to; i++) {
                KeyCacheObject key = new KeyCacheObjectImpl(i, null, intCache.affinity().partition(i));
                CacheObject val = new CacheObjectImpl(createUser(i), null);

                val.prepareMarshal(intCache.context().cacheObjectContext());

                drMap.put(key, new GridCacheDrInfo(val, new GridCacheVersion(1, i, 1, OTHER_CLUSTER_ID)));
            }

            if (concurrency != null) {
                try (Transaction tx = cli.transactions().txStart(concurrency, isolation)) {
                    intCache.putAllConflict(drMap);
                    tx.commit();
                }
            }
            else
                intCache.putAllConflict(drMap);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private void removeConflictData(
        IgniteEx cli,
        IgniteCache<Integer, User> cache,
        int from,
        int to,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation
    ) {
        try {
            IgniteInternalCache<Integer, User> intCache = cli.cachex(cache.getName());

            Map<KeyCacheObject, GridCacheVersion> drMap = new HashMap<>();

            for (int i = from; i < to; i++) {
                drMap.put(
                    new KeyCacheObjectImpl(i, null, intCache.affinity().partition(i)),
                    new GridCacheVersion(1, i, 1, OTHER_CLUSTER_ID)
                );
            }

            if (concurrency != null) {
                try (Transaction tx = cli.transactions().txStart(concurrency, isolation)) {
                    intCache.removeAllConflict(drMap);
                    tx.commit();
                }
            }
            else
                intCache.removeAllConflict(drMap);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public class TestCacheConflictResolutionManager<K, V> extends GridCacheManagerAdapter<K, V>
        implements CacheConflictResolutionManager<K, V> {

        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return conflictResolutionMgrSupplier.get();
        }
    }
}
