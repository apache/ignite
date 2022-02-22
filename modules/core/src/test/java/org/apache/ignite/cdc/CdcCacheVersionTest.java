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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.CdcMain;
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
import org.apache.ignite.internal.processors.metric.MetricRegistry;
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

import static java.lang.Boolean.TRUE;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager.DATA_VER_CLUSTER_ID;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class CdcCacheVersionTest extends AbstractCdcTest {
    /** */
    public static final String FOR_OTHER_CLUSTER_ID = "for-other-cluster-id";

    /** */
    public static final byte DFLT_CLUSTER_ID = 1;

    /** */
    public static final byte OTHER_CLUSTER_ID = 2;

    /** */
    public static final int KEY_TO_UPD = 42;

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
    private TransactionConcurrency concurrency;

    /** */
    private TransactionIsolation isolation;

    /** */
    private static final AtomicLong WAL_REC_CHECKED = new AtomicLong();

    /** */
    private static final AtomicLong CONFLICT_CHECKED = new AtomicLong();

    /** */
    @Parameterized.Parameters(name = "atomicity={0}, mode={1}, gridCnt={2}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode atomicity : EnumSet.of(ATOMIC, TRANSACTIONAL))
            for (CacheMode mode : EnumSet.of(PARTITIONED, REPLICATED))
                for (int gridCnt : new int[] {1, 3})
                    params.add(new Object[] {atomicity, mode, gridCnt});

        return params;
    }

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
                if (ctx.igniteConfiguration().isClientMode() == TRUE ||
                    !ctx.igniteCacheConfiguration().getName().equals(FOR_OTHER_CLUSTER_ID))
                    return null;

                return new AbstractCachePluginProvider() {
                    @Override public @Nullable Object createComponent(Class cls) {
                        if (cls != CacheConflictResolutionManager.class)
                            return null;

                        return new TestCacheConflictResolutionManager();
                    }
                };
            }

            @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
                if (ctx.igniteConfiguration().isClientMode() != TRUE &&
                    IgniteWriteAheadLogManager.class.equals(cls))
                    return (T)new TestFileWriteAheadLogManager(((IgniteEx)ctx.grid()).context());

                return null;
            }
        });

        return cfg;
    }

    /** Test that conflict version is writtern to WAL. */
    @Test
    public void testConflictVersionWritten() throws Exception {
        startGrids(gridCnt);
        IgniteEx cli = startClientGrid(gridCnt);

        for (int i = 0; i < gridCnt; i++) {
            grid(i).context().cache().context().versions().dataCenterId(DFLT_CLUSTER_ID);

            assertEquals(
                DFLT_CLUSTER_ID,
                grid(i).context().metric().registry(CACHE_METRICS).<IntMetric>findMetric(DATA_VER_CLUSTER_ID).value()
            );

            assertTrue(
                "Must use test WAL implementation",
                grid(i).context().cache().context().wal() instanceof TestFileWriteAheadLogManager
            );
        }

        cli.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = cli.getOrCreateCache(
            new CacheConfiguration<Integer, User>(FOR_OTHER_CLUSTER_ID)
                .setCacheMode(cacheMode)
                .setAtomicityMode(atomicityMode)
                .setBackups(gridCnt - 1));

        for (int i = 0; i < gridCnt; i++) {
            boolean found = false;

            assertFalse(grid(i).context().clientNode());

            SystemView<CacheView> caches = grid(i).context().systemView().view(CACHES_VIEW);

            for (CacheView v : caches) {
                if (v.cacheName().equals(FOR_OTHER_CLUSTER_ID)) {
                    assertEquals(v.conflictResolver(), "TestCacheConflictResolutionManager");

                    found = true;
                }
                else
                    assertNull(v.conflictResolver());
            }

            assertTrue(found);
        }

        CONFLICT_CHECKED.set(0);
        WAL_REC_CHECKED.set(0);

        if (atomicityMode == ATOMIC)
            doCheck(cli, cache);
        else {
            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    this.concurrency = concurrency;
                    this.isolation = isolation;

                    doCheck(cli, cache);
                }
            }

            concurrency = null;
            isolation = null;
        }

    }

    private void doCheck(IgniteEx cli, IgniteCache<Integer, User> cache) throws IgniteInterruptedCheckedException {
        // Put data with conflict version.
        // Conflict version will be checked during WAL record and conflict resolution.
        addConflictData(cli, cache, 0, KEYS_CNT);

        checkResolverAndWal();

        // Replacing existing data.
        addConflictData(cli, cache, 0, KEYS_CNT);

        checkResolverAndWal();

        // Removing existing data with conflict version.
        removeConflictData(cli, cache, 0, KEYS_CNT);

        checkResolverAndWal();
    }

    /** */
    private void checkResolverAndWal() throws IgniteInterruptedCheckedException {
        // Conflict resolver for ATOMIC caches invoked only on primary.
        long expConflictResolverCnt = atomicityMode == ATOMIC ? KEYS_CNT : (KEYS_CNT * (long)gridCnt);

        if (!waitForCondition(() -> CONFLICT_CHECKED.get() == expConflictResolverCnt, WAL_ARCHIVE_TIMEOUT))
            fail("Expected " + expConflictResolverCnt + " but was " + CONFLICT_CHECKED.get());

        long expWalRecCnt = (long)KEYS_CNT * gridCnt;

        if (!waitForCondition(() -> WAL_REC_CHECKED.get() == expWalRecCnt, WAL_ARCHIVE_TIMEOUT))
            fail("Expected " + expWalRecCnt + " but was " + WAL_REC_CHECKED.get());

        CONFLICT_CHECKED.set(0);
        WAL_REC_CHECKED.set(0);
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

            @Override public void start(MetricRegistry mreg) {
                // No-op.
            }

            @Override public void stop() {
                // No-op.
            }
        };

        CdcMain cdc = createCdc(cnsmr, cfg);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(new CacheConfiguration<Integer, User>("my-cache")
            .setAtomicityMode(atomicityMode)
            .setCacheMode(cacheMode));

        IgniteInternalFuture<?> fut = runAsync(cdc);

        // Update the same key several time.
        // Expect {@link CacheEntryVersion#order()} will monotically increase.
        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(KEY_TO_UPD, createUser(i));

        assertTrue(waitForCondition(() -> updCntr.get() == KEYS_CNT, getTestTimeout()));

        fut.cancel();
    }

    /** */
    private void addConflictData(IgniteEx cli, IgniteCache<Integer, User> cache, int from, int to) {
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
    private void removeConflictData(IgniteEx cli, IgniteCache<Integer, User> cache, int from, int to) {
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

                    assertEquals(OTHER_CLUSTER_ID, newEntry.version().dataCenterId());

                    if (!oldEntry.isStartVersion())
                        assertEquals(OTHER_CLUSTER_ID, oldEntry.version().dataCenterId());

                    CONFLICT_CHECKED.incrementAndGet();

                    return res;
                }

                @Override public String toString() {
                    return "TestCacheConflictResolutionManager";
                }
            };
        }
    }

    /** WAL manager that counts how many times replay has been called. */
    private static class TestFileWriteAheadLogManager extends FileWriteAheadLogManager {
        /** */
        public TestFileWriteAheadLogManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord rec) throws IgniteCheckedException {
            if (rec.type() == DATA_RECORD_V2 && ((DataRecord)rec).get(0).cacheId() == CU.cacheId(FOR_OTHER_CLUSTER_ID)) {
                DataRecord dataRec = (DataRecord)rec;

                for (int i = 0; i < dataRec.entryCount(); i++) {
                    DataEntry dataEntry = dataRec.writeEntries().get(i);

                    assertEquals(DFLT_CLUSTER_ID, dataEntry.writeVersion().dataCenterId());
                    assertNotNull(dataEntry.writeVersion().conflictVersion());
                    assertEquals(OTHER_CLUSTER_ID, dataEntry.writeVersion().conflictVersion().dataCenterId());

                    WAL_REC_CHECKED.incrementAndGet();
                }
            }

            return super.log(rec);
        }
    }
}
