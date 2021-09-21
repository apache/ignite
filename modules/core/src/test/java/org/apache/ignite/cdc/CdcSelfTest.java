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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.DELETE;
import static org.apache.ignite.cdc.AbstractCdcTest.ChangeEventType.UPDATE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class CdcSelfTest extends AbstractCdcTest {
    /** */
    public static final String TX_CACHE_NAME = "tx-cache";

    /** */
    public static final int WAL_ARCHIVE_TIMEOUT = 5_000;

    /** */
    @Parameterized.Parameter
    public boolean specificConsistentId;

    /** */
    @Parameterized.Parameter(1)
    public WALMode walMode;

    /** */
    @Parameterized.Parameter(2)
    public Supplier<MetricExporterSpi> metricExporter;

    /** */
    @Parameterized.Parameters(name = "specificConsistentId={0}, walMode={1}, metricExporter={2}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (WALMode mode : EnumSet.of(WALMode.FSYNC, WALMode.LOG_ONLY, WALMode.BACKGROUND))
            for (boolean specificConsistentId : new boolean[] {true, false}) {
                Supplier<MetricExporterSpi> jmx = JmxMetricExporterSpi::new;

                params.add(new Object[] {specificConsistentId, mode, null});
                params.add(new Object[] {specificConsistentId, mode, jmx});
            }

        return params;
    }

    /** Consistent id. */
    private UUID consistentId = UUID.randomUUID();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (specificConsistentId)
            cfg.setConsistentId(consistentId);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setCdcEnabled(true)
            .setWalMode(walMode)
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(TX_CACHE_NAME).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        return cfg;
    }

    /** Simplest CDC test. */
    @Test
    public void testReadAllKeys() throws Exception {
        // Read all records from iterator.
        readAll(new UserCdcConsumer(), true);

        // Read one record per call.
        readAll(new UserCdcConsumer() {
            @Override public boolean onEvents(Iterator<CdcEvent> evts) {
                super.onEvents(Collections.singleton(evts.next()).iterator());

                return false;
            }
        }, false);

        // Read one record per call and commit.
        readAll(new UserCdcConsumer() {
            @Override public boolean onEvents(Iterator<CdcEvent> evts) {
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

        Supplier<CdcMain> cdc = () -> new CdcMain(cfg, null, cdcConfig(cnsmr));

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, User> txCache = ign.getOrCreateCache(TX_CACHE_NAME);

        addAndWaitForConsumption(
            cnsmr,
            cdc.get(),
            cache,
            txCache,
            CdcSelfTest::addData,
            0,
            KEYS_CNT + 3,
            getTestTimeout()
        );

        removeData(cache, 0, KEYS_CNT);

        CdcMain cdcMain = cdc.get();

        IgniteInternalFuture<?> rmvFut = runAsync(cdcMain);

        assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, getTestTimeout(), cnsmr));

        checkMetrics(cdcMain, offsetCommit ? KEYS_CNT : ((KEYS_CNT + 3) * 2 + KEYS_CNT));

        rmvFut.cancel();

        assertTrue(cnsmr.stopped());

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testReadBeforeGracefulShutdown() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        CountDownLatch cnsmrStarted = new CountDownLatch(1);
        CountDownLatch startProcEvts = new CountDownLatch(1);

        UserCdcConsumer cnsmr = new UserCdcConsumer() {
            @Override public boolean onEvents(Iterator<CdcEvent> evts) {
                cnsmrStarted.countDown();

                try {
                    startProcEvts.await(getTestTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                return super.onEvents(evts);
            }
        };

        CdcMain cdc = new CdcMain(cfg, null, cdcConfig(cnsmr));

        runAsync(cdc);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);

        // Make sure all streamed data will become available for consumption.
        Thread.sleep(2 * WAL_ARCHIVE_TIMEOUT);

        cnsmrStarted.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        // Initiate graceful shutdown.
        cdc.stop();

        startProcEvts.countDown();

        assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr));
        assertTrue(waitForCondition(cnsmr::stopped, getTestTimeout()));

        checkMetrics(cdc, KEYS_CNT);

        List<Integer> keys = cnsmr.data(UPDATE, cacheId(DEFAULT_CACHE_NAME));

        assertEquals(KEYS_CNT, keys.size());

        for (int i = 0; i < KEYS_CNT; i++)
            assertTrue(keys.contains(i));
    }

    /** */
    @Test
    public void testMultiNodeConsumption() throws Exception {
        IgniteEx ign1 = startGrid(0);

        if (specificConsistentId)
            consistentId = UUID.randomUUID();

        IgniteEx ign2 = startGrid(1);

        ign1.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign1.getOrCreateCache(DEFAULT_CACHE_NAME);

        // Adds data concurrently with CDC start.
        IgniteInternalFuture<?> addDataFut = runAsync(() -> addData(cache, 0, KEYS_CNT));

        UserCdcConsumer cnsmr1 = new UserCdcConsumer();
        UserCdcConsumer cnsmr2 = new UserCdcConsumer();

        IgniteConfiguration cfg1 = ign1.configuration();
        IgniteConfiguration cfg2 = ign2.configuration();

        CdcMain cdc1 = new CdcMain(cfg1, null, cdcConfig(cnsmr1));
        CdcMain cdc2 = new CdcMain(cfg2, null, cdcConfig(cnsmr2));

        IgniteInternalFuture<?> fut1 = runAsync(cdc1);
        IgniteInternalFuture<?> fut2 = runAsync(cdc2);

        addDataFut.get(getTestTimeout());

        addDataFut = runAsync(() -> addData(cache, KEYS_CNT, KEYS_CNT * 2));

        addDataFut.get(getTestTimeout());

        assertTrue(waitForSize(KEYS_CNT * 2, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr1, cnsmr2));

        long evtsCnt1 = checkMetrics(cdc1, -1);
        long evtsCnt2 = checkMetrics(cdc2, -1);

        assertEquals(KEYS_CNT * 2, evtsCnt1 + evtsCnt2);

        assertFalse(cnsmr1.stopped());
        assertFalse(cnsmr2.stopped());

        fut1.cancel();
        fut2.cancel();

        assertTrue(cnsmr1.stopped());
        assertTrue(cnsmr2.stopped());

        removeData(cache, 0, KEYS_CNT * 2);

        cdc1 = new CdcMain(cfg1, null, cdcConfig(cnsmr1));
        cdc2 = new CdcMain(cfg2, null, cdcConfig(cnsmr2));

        IgniteInternalFuture<?> rmvFut1 = runAsync(cdc1);
        IgniteInternalFuture<?> rmvFut2 = runAsync(cdc2);

        assertTrue(waitForSize(KEYS_CNT * 2, DEFAULT_CACHE_NAME, DELETE, getTestTimeout(), cnsmr1, cnsmr2));

        evtsCnt1 = checkMetrics(cdc1, -1);
        evtsCnt2 = checkMetrics(cdc2, -1);

        assertEquals(KEYS_CNT * 2, evtsCnt1 + evtsCnt2);

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

        IgniteInternalFuture<?> fut1 = runAsync(new CdcMain(ign.configuration(), null, cdcConfig(cnsmr1)));
        IgniteInternalFuture<?> fut2 = runAsync(new CdcMain(ign.configuration(), null, cdcConfig(cnsmr2)));

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
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteEx ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);

        for (int i = 0; i < 3; i++) {
            UserCdcConsumer cnsmr = new UserCdcConsumer() {
                @Override protected boolean commit() {
                    return false;
                }
            };

            CdcMain cdc = new CdcMain(cfg, null, cdcConfig(cnsmr));

            IgniteInternalFuture<?> fut = runAsync(cdc);

            assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr));

            checkMetrics(cdc, KEYS_CNT);

            fut.cancel();

            assertTrue(cnsmr.stopped());
        }

        AtomicBoolean consumeHalf = new AtomicBoolean(true);
        AtomicBoolean halfCommitted = new AtomicBoolean(false);

        int half = KEYS_CNT / 2;

        UserCdcConsumer cnsmr = new UserCdcConsumer() {
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

        CdcMain cdc = new CdcMain(cfg, null, cdcConfig(cnsmr));

        IgniteInternalFuture<?> fut = runAsync(cdc);

        waitForSize(half, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr);

        checkMetrics(cdc, half);

        waitForCondition(halfCommitted::get, getTestTimeout());

        fut.cancel();

        assertTrue(cnsmr.stopped());

        removeData(cache, 0, KEYS_CNT);

        consumeHalf.set(false);

        cdc = new CdcMain(cfg, null, cdcConfig(cnsmr));

        fut = runAsync(cdc);

        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr);
        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, getTestTimeout(), cnsmr);

        checkMetrics(cdc, KEYS_CNT * 2 - half);

        fut.cancel();

        assertTrue(cnsmr.stopped());
    }

    /** {@inheritDoc} */
    @Override public MetricExporterSpi[] metricExporters() {
        if (metricExporter == null)
            return null;

        return new MetricExporterSpi[] {metricExporter.get()};
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
