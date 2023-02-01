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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.DELETE;
import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.UPDATE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CDC_WAL_DIRECTORY_MAX_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assume.assumeTrue;

/** */
@RunWith(Parameterized.class)
public class CdcSelfTest extends AbstractCdcTest {
    /** */
    public static final String TX_CACHE_NAME = "tx-cache";

    /** */
    @Parameterized.Parameter
    public boolean specificConsistentId;

    /** */
    @Parameterized.Parameter(1)
    public WALMode walMode;

    /** */
    @Parameterized.Parameter(2)
    public boolean persistenceEnabled;

    /** */
    private long cdcWalDirMaxSize = DFLT_CDC_WAL_DIRECTORY_MAX_SIZE;

    /** */
    @Parameterized.Parameters(name = "consistentId={0}, wal={1}, persistence={2}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (WALMode mode : EnumSet.of(WALMode.FSYNC, WALMode.LOG_ONLY, WALMode.BACKGROUND))
            for (boolean specificConsistentId : new boolean[] {false, true})
                for (boolean persistenceEnabled : new boolean[] {true, false})
                    params.add(new Object[] {specificConsistentId, mode, persistenceEnabled});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (specificConsistentId)
            cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalMode(walMode)
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)
                .setCdcEnabled(true))
            .setWalArchivePath(DFLT_WAL_ARCHIVE_PATH + "/" + U.maskForFileName(igniteInstanceName))
            .setCdcWalDirectoryMaxSize(cdcWalDirMaxSize));

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(TX_CACHE_NAME)
                .setAtomicityMode(TRANSACTIONAL)
                .setBackups(1)
        );

        return cfg;
    }

    /** Simplest CDC test. */
    @Test
    public void testReadAllKeysCommitAll() throws Exception {
        // Read all records from iterator.
        readAll(new UserCdcConsumer(), true);
    }

    /** Simplest CDC test but read one event at a time to check correct iterator work. */
    @Test
    public void testReadAllKeysWithoutCommit() throws Exception {
        // Read one record per call.
        readAll(new UserCdcConsumer() {
            @Override public boolean onEvents(Iterator<CdcEvent> evts) {
                if (evts.hasNext())
                    super.onEvents(Collections.singleton(evts.next()).iterator());

                return false;
            }
        }, false);
    }

    /** Simplest CDC test but commit every event to check correct state restore. */
    @Test
    public void testReadAllKeysCommitEachEvent() throws Exception {
        // Read one record per call and commit.
        readAll(new UserCdcConsumer() {
            @Override public boolean onEvents(Iterator<CdcEvent> evts) {
                if (evts.hasNext())
                    super.onEvents(Collections.singleton(evts.next()).iterator());

                return true;
            }
        }, true);
    }

    /** */
    private void readAll(UserCdcConsumer cnsmr, boolean offsetCommit) throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, User> txCache = ign.getOrCreateCache(TX_CACHE_NAME);

        addAndWaitForConsumption(
            cnsmr,
            cfg,
            cache,
            txCache,
            CdcSelfTest::addData,
            0,
            KEYS_CNT + 3,
            offsetCommit
        );

        removeData(cache, 0, KEYS_CNT);

        CdcMain cdcMain = createCdc(cnsmr, cfg);

        IgniteInternalFuture<?> rmvFut = runAsync(cdcMain);

        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, cnsmr);

        checkMetrics(cdcMain, offsetCommit ? KEYS_CNT : ((KEYS_CNT + 3) * 2 + KEYS_CNT));

        rmvFut.cancel();

        assertTrue(cnsmr.stopped());

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testReadOneByOneForBackup() throws Exception {
        assumeTrue("CDC with 2 local nodes can't determine correct PDS directory without specificConsistentId.",
            specificConsistentId);

        IgniteEx ign = startGrids(2);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> txCache = ign.cache(TX_CACHE_NAME);

        awaitPartitionMapExchange();

        int keysCnt = 3;

        Map<Integer, Integer> batch = primaryKeys(txCache, keysCnt).stream()
            .collect(Collectors.toMap(key -> key, val -> val, (a, b) -> a, TreeMap::new));

        // Put data in batch because it will be logged in form of `DataRecord(List<DataEntry)` on backup node.
        txCache.putAll(batch);

        // Check `DataRecord(List<DataEntry>)` logged.
        File archive = U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            grid(1).configuration().getDataStorageConfiguration().getWalArchivePath(),
            false
        );

        IteratorParametersBuilder param = new IteratorParametersBuilder().filesOrDirs(archive)
            .filter((type, pointer) -> type == WALRecord.RecordType.DATA_RECORD_V2);

        assertTrue("DataRecord(List<DataEntry>) should be logged.", waitForCondition(() -> {
            try (WALIterator iter = new IgniteWalIteratorFactory(log).iterator(param)) {
                while (iter.hasNext()) {
                    DataRecord rec = (DataRecord)iter.next().get2();

                    if (rec.entryCount() > 1)
                        return true;
                }
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            return false;
        }, getTestTimeout()));

        for (int i = 0; i < 2; i++) {
            IgniteEx grid = grid(i);

            Set<Integer> data = new HashSet<>();

            AtomicBoolean firstEvt = new AtomicBoolean(true);

            CdcConsumer cnsmr = new CdcConsumer() {
                @Override public boolean onEvents(Iterator<CdcEvent> evts) {
                    if (!evts.hasNext())
                        return true;

                    if (!firstEvt.get())
                        throw new RuntimeException("Expected fail.");

                    data.add((Integer)evts.next().key());

                    firstEvt.set(false);

                    if (data.size() == keysCnt)
                        throw new RuntimeException("Expected fail.");

                    return true;
                }

                @Override public void onTypes(Iterator<BinaryType> types) {
                    types.forEachRemaining(t -> assertNotNull(t));
                }

                @Override public void onMappings(Iterator<TypeMapping> mappings) {
                    mappings.forEachRemaining(m -> assertNotNull(m));
                }

                @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
                    cacheEvents.forEachRemaining(ce -> assertNotNull(ce));
                }

                /** {@inheritDoc} */
                @Override public void onCacheDestroy(Iterator<Integer> caches) {
                    caches.forEachRemaining(ce -> assertNotNull(ce));
                }

                @Override public void stop() {
                    // No-op.
                }

                @Override public void start(MetricRegistry mreg) {
                    // No-op.
                }
            };

            for (int j = 0; j < keysCnt; j++) {
                CdcMain cdc = createCdc(cnsmr, getConfiguration(grid.name()));

                IgniteInternalFuture<?> fut = runAsync(cdc);

                // Restart CDC after read and commit single key.
                assertTrue(waitForCondition(fut::isDone, getTestTimeout()));
                assertEquals(j + 1, data.size());

                firstEvt.set(true);
            }

            assertTrue(F.eqNotOrdered(batch.keySet(), data));
        }
    }

    /** Test check that state restored correctly and next event read by CDC on each restart. */
    @Test
    public void testReadFromNextEntry() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteEx ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        int keysCnt = 10;

        addData(cache, 0, keysCnt / 2);

        long segIdx = ign.context().cache().context().wal(true).lastArchivedSegment();

        waitForCondition(() -> ign.context().cache().context().wal(true).lastArchivedSegment() > segIdx, getTestTimeout());

        addData(cache, keysCnt / 2, keysCnt);

        AtomicInteger expKey = new AtomicInteger();
        int lastKey = 0;

        while (expKey.get() != keysCnt) {
            String errMsg = "Expected fail";

            IgniteInternalFuture<?> fut = runAsync(createCdc(new CdcConsumer() {
                boolean oneConsumed;

                @Override public boolean onEvents(Iterator<CdcEvent> evts) {
                    if (!evts.hasNext())
                        return true;

                    // Fail application after one event read AND state committed.
                    if (oneConsumed)
                        throw new RuntimeException(errMsg);

                    CdcEvent evt = evts.next();

                    assertEquals(expKey.get(), evt.key());

                    expKey.incrementAndGet();

                    // Fail application if all expected data read e.g. next event doesn't exist.
                    if (expKey.get() == keysCnt)
                        throw new RuntimeException(errMsg);

                    oneConsumed = true;

                    return true;
                }

                @Override public void onTypes(Iterator<BinaryType> types) {
                    types.forEachRemaining(t -> assertNotNull(t));
                }

                @Override public void onMappings(Iterator<TypeMapping> mappings) {
                    mappings.forEachRemaining(m -> assertNotNull(m));
                }

                @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
                    cacheEvents.forEachRemaining(ce -> assertNotNull(ce));
                }

                @Override public void onCacheDestroy(Iterator<Integer> caches) {
                    caches.forEachRemaining(ce -> assertNotNull(ce));
                }

                @Override public void stop() {
                    // No-op.
                }

                @Override public void start(MetricRegistry mreg) {
                    // No-op.
                }
            }, cfg));

            assertTrue(waitForCondition(fut::isDone, getTestTimeout()));

            if (!errMsg.equals(fut.error().getMessage()))
                throw new RuntimeException(fut.error());

            assertEquals(1, expKey.get() - lastKey);

            lastKey = expKey.get();
        }
    }

    /** */
    @Test
    public void testReadBeforeGracefulShutdown() throws Exception {
        Ignite ign = startGrid(getConfiguration("ignite-0"));

        ign.cluster().state(ACTIVE);

        CountDownLatch cnsmrStarted = new CountDownLatch(1);
        CountDownLatch startProcEvts = new CountDownLatch(1);

        UserCdcConsumer cnsmr = new UserCdcConsumer() {
            @Override public boolean onEvents(Iterator<CdcEvent> evts) {
                cnsmrStarted.countDown();

                try {
                    startProcEvts.await(getTestTimeout(), MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                return super.onEvents(evts);
            }
        };

        CdcMain cdc = createCdc(cnsmr, getConfiguration(ign.name()));

        runAsync(cdc);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);

        // Make sure all streamed data will become available for consumption.
        Thread.sleep(2 * WAL_ARCHIVE_TIMEOUT);

        cnsmrStarted.await(getTestTimeout(), MILLISECONDS);

        // Initiate graceful shutdown.
        cdc.stop();

        startProcEvts.countDown();

        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, cnsmr);

        assertTrue(waitForCondition(cnsmr::stopped, getTestTimeout()));

        List<Integer> keys = cnsmr.data(UPDATE, cacheId(DEFAULT_CACHE_NAME));

        assertEquals(KEYS_CNT, keys.size());

        for (int i = 0; i < KEYS_CNT; i++)
            assertTrue(keys.contains(i));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID, value = "true")
    public void testMultiNodeConsumption() throws Exception {
        IgniteEx ign1 = startGrid(0);

        IgniteEx ign2 = startGrid(1);

        ign1.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign1.getOrCreateCache(DEFAULT_CACHE_NAME);

        // Calculate expected count of key for each node.
        int[] keysCnt = new int[2];

        for (int i = 0; i < KEYS_CNT * 2; i++) {
            Ignite primary = primaryNode(i, DEFAULT_CACHE_NAME);

            assertTrue(primary == ign1 || primary == ign2);

            keysCnt[primary == ign1 ? 0 : 1]++;
        }

        // Adds data concurrently with CDC start.
        IgniteInternalFuture<?> addDataFut = runAsync(() -> addData(cache, 0, KEYS_CNT));

        UserCdcConsumer cnsmr1 = new UserCdcConsumer();
        UserCdcConsumer cnsmr2 = new UserCdcConsumer();

        IgniteConfiguration cfg1 = getConfiguration(ign1.name());
        IgniteConfiguration cfg2 = getConfiguration(ign2.name());

        // Always run CDC with consistent id to ensure instance read data for specific node.
        if (!specificConsistentId) {
            cfg1.setConsistentId((Serializable)ign1.localNode().consistentId());
            cfg2.setConsistentId((Serializable)ign2.localNode().consistentId());
        }

        CountDownLatch latch = new CountDownLatch(2);

        GridAbsPredicate sizePredicate1 = sizePredicate(keysCnt[0], DEFAULT_CACHE_NAME, UPDATE, cnsmr1);
        GridAbsPredicate sizePredicate2 = sizePredicate(keysCnt[1], DEFAULT_CACHE_NAME, UPDATE, cnsmr2);

        CdcMain cdc1 = createCdc(cnsmr1, cfg1, latch, sizePredicate1);
        CdcMain cdc2 = createCdc(cnsmr2, cfg2, latch, sizePredicate2);

        IgniteInternalFuture<?> fut1 = runAsync(cdc1);
        IgniteInternalFuture<?> fut2 = runAsync(cdc2);

        addDataFut.get(getTestTimeout());

        runAsync(() -> addData(cache, KEYS_CNT, KEYS_CNT * 2)).get(getTestTimeout());

        // Wait while predicate will become true and state saved on the disk for both cdc.
        assertTrue(latch.await(getTestTimeout(), MILLISECONDS));

        checkMetrics(cdc1, keysCnt[0]);
        checkMetrics(cdc2, keysCnt[1]);

        assertFalse(cnsmr1.stopped());
        assertFalse(cnsmr2.stopped());

        fut1.cancel();
        fut2.cancel();

        assertTrue(cnsmr1.stopped());
        assertTrue(cnsmr2.stopped());

        removeData(cache, 0, KEYS_CNT * 2);

        cdc1 = createCdc(cnsmr1, cfg1);
        cdc2 = createCdc(cnsmr2, cfg2);

        IgniteInternalFuture<?> rmvFut1 = runAsync(cdc1);
        IgniteInternalFuture<?> rmvFut2 = runAsync(cdc2);

        waitForSize(KEYS_CNT * 2, DEFAULT_CACHE_NAME, DELETE, cnsmr1, cnsmr2);

        checkMetrics(cdc1, keysCnt[0]);
        checkMetrics(cdc2, keysCnt[1]);

        rmvFut1.cancel();
        rmvFut2.cancel();

        assertTrue(cnsmr1.stopped());
        assertTrue(cnsmr2.stopped());
    }

    /** */
    @Test
    public void testCdcSingleton() throws Exception {
        IgniteEx ign = startGrid(0);

        UserCdcConsumer cnsmr1 = new UserCdcConsumer();
        UserCdcConsumer cnsmr2 = new UserCdcConsumer();

        IgniteInternalFuture<?> fut1 = runAsync(createCdc(cnsmr1, getConfiguration(ign.name())));
        IgniteInternalFuture<?> fut2 = runAsync(createCdc(cnsmr2, getConfiguration(ign.name())));

        assertTrue(waitForCondition(() -> fut1.isDone() || fut2.isDone(), getTestTimeout()));

        assertEquals(fut1.error() == null, fut2.error() != null);

        if (fut1.isDone()) {
            fut2.cancel();

            assertTrue(cnsmr2.stopped());
        }
        else {
            fut1.cancel();

            assertTrue(cnsmr1.stopped());
        }
    }

    /** */
    @Test
    public void testReReadWhenStateWasNotStored() throws Exception {
        Supplier<IgniteEx> restart = () -> {
            stopAllGrids(false);

            try {
                IgniteEx ign = startGrid(getConfiguration("ignite-0"));

                ign.cluster().state(ACTIVE);

                return ign;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        IgniteEx ign = restart.get();

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT / 2);

        ign = restart.get();

        cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, KEYS_CNT / 2, KEYS_CNT);

        UserCdcConsumer cnsmr = new UserCdcConsumer() {
            @Override protected boolean commit() {
                return false;
            }
        };

        for (int i = 0; i < 3; i++) {
            CdcMain cdc = createCdc(cnsmr, getConfiguration(ign.name()));

            IgniteInternalFuture<?> fut = runAsync(cdc);

            waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, cnsmr);

            checkMetrics(cdc, KEYS_CNT);

            fut.cancel();

            assertTrue(cnsmr.stopped());

            cnsmr.data.clear();
        }

        AtomicBoolean consumeHalf = new AtomicBoolean(true);
        AtomicBoolean halfCommitted = new AtomicBoolean(false);

        int half = KEYS_CNT / 2;

        cnsmr = new UserCdcConsumer() {
            @Override public boolean onEvents(Iterator<CdcEvent> evts) {
                if (consumeHalf.get() && F.size(data(UPDATE, cacheId(DEFAULT_CACHE_NAME))) == half) {
                    // This means that state committed as a result of the previous call.
                    halfCommitted.set(true);

                    return false;
                }

                while (evts.hasNext()) {
                    CdcEvent evt = evts.next();

                    if (!evt.primary())
                        continue;

                    data.computeIfAbsent(
                        F.t(evt.value() == null ? DELETE : UPDATE, evt.cacheId()),
                        k -> new ArrayList<>()).add((Integer)evt.key()
                    );

                    if (consumeHalf.get())
                        return F.size(data(UPDATE, cacheId(DEFAULT_CACHE_NAME))) == half;
                }

                return true;
            }
        };

        CdcMain cdc = createCdc(cnsmr, getConfiguration(ign.name()));

        IgniteInternalFuture<?> fut = runAsync(cdc);

        waitForSize(half, DEFAULT_CACHE_NAME, UPDATE, cnsmr);

        checkMetrics(cdc, half);

        waitForCondition(halfCommitted::get, getTestTimeout());

        fut.cancel();

        assertTrue(cnsmr.stopped());

        removeData(cache, 0, KEYS_CNT);

        consumeHalf.set(false);

        cdc = createCdc(cnsmr, getConfiguration(ign.name()));

        fut = runAsync(cdc);

        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, cnsmr);
        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, cnsmr);

        checkMetrics(cdc, KEYS_CNT * 2 - half);

        fut.cancel();

        assertTrue(cnsmr.stopped());
    }

    /** */
    @Test
    public void testDisable() throws Exception {
        IgniteEx ign = startGrid(0);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, 1);

        File walCdcDir = U.field(ign.context().cache().context().wal(true), "walCdcDir");

        assertTrue(waitForCondition(() -> 1 == walCdcDir.list().length, 2 * WAL_ARCHIVE_TIMEOUT));

        DistributedChangeableProperty<Serializable> disabled = ign.context().distributedConfiguration()
            .property(FileWriteAheadLogManager.CDC_DISABLED);

        disabled.propagate(true);

        addData(cache, 0, 1);

        Thread.sleep(2 * WAL_ARCHIVE_TIMEOUT);

        assertEquals(1, walCdcDir.list().length);

        disabled.propagate(false);

        addData(cache, 0, 1);

        assertTrue(waitForCondition(() -> 2 == walCdcDir.list().length, 2 * WAL_ARCHIVE_TIMEOUT));
    }

    /** */
    @Test
    public void testCdcDirectoryMaxSize() throws Exception {
        cdcWalDirMaxSize = 10 * U.MB;
        int segmentSize = (int)(cdcWalDirMaxSize / 2);

        IgniteEx ign = startGrid(0);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteWriteAheadLogManager wal = ign.context().cache().context().wal(true);
        File walCdcDir = U.field(ign.context().cache().context().wal(true), "walCdcDir");

        RunnableX writeSgmnt = () -> {
            int sgmnts = wal.walArchiveSegments();
            int dataSize = (int)(segmentSize * 0.8);

            for (int i = 0; i < dataSize / DFLT_PAGE_SIZE; i++)
                wal.log(new PageSnapshot(new FullPageId(-1, -1), new byte[DFLT_PAGE_SIZE], 1));

            addData(cache, 0, 1);

            waitForCondition(() -> wal.walArchiveSegments() > sgmnts, 2 * WAL_ARCHIVE_TIMEOUT);
        };

        // Write to the WAL to exceed the configured max size.
        writeSgmnt.run();
        writeSgmnt.run();

        // The segment link creation should be skipped.
        writeSgmnt.run();

        assertTrue(cdcWalDirMaxSize >= Arrays.stream(walCdcDir.listFiles()).mapToLong(File::length).sum());

        UserCdcConsumer cnsmr = new UserCdcConsumer();

        CdcMain cdc = createCdc(cnsmr, getConfiguration(ign.name()));

        IgniteInternalFuture<?> fut = runAsync(cdc);

        waitForSize(2, DEFAULT_CACHE_NAME, UPDATE, cnsmr);

        assertFalse(fut.isDone());

        // Write next segment after skipped.
        writeSgmnt.run();

        assertThrows(log, () -> fut.get(getTestTimeout()), IgniteCheckedException.class,
            "Found missed segments. Some events are missed.");
    }

    /** */
    public static void addData(IgniteCache<Integer, User> cache, int from, int to) {
        for (int i = from; i < to; i++)
            cache.put(i, createUser(i));
    }

    /** */
    private void removeData(IgniteCache<Integer, ?> cache, int from, int to) {
        for (int i = from; i < to; i++)
            cache.remove(i);
    }
}
