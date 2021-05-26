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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.ChangeDataCapture;
import org.apache.ignite.internal.util.typedef.CI3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cdc.ChangeDataCaptureSelfTest.ChangeEventType.DELETE;
import static org.apache.ignite.cdc.ChangeDataCaptureSelfTest.ChangeEventType.UPDATE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class ChangeDataCaptureSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String TX_CACHE_NAME = "tx-cache";

    /** */
    public static final int WAL_ARCHIVE_TIMEOUT = 5_000;

    /** Keys count. */
    public static final int KEYS_CNT = 50;

    /** */
    @Parameterized.Parameter
    public boolean specificConsistentId;

    /** */
    @Parameterized.Parameter(1)
    public WALMode walMode;

    @Parameterized.Parameters(name = "specificConsistentId={0},walMode={1}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            {true, WALMode.FSYNC},
            {false, WALMode.FSYNC},
            {true, WALMode.LOG_ONLY},
            {false, WALMode.LOG_ONLY},
            {true, WALMode.BACKGROUND},
            {false, WALMode.BACKGROUND}
        });
    }

    /** Consistent id. */
    private UUID consistentId = UUID.randomUUID();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (specificConsistentId)
            cfg.setConsistentId(consistentId);

        int segmentSz = 10 * 1024 * 1024;

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setCdcEnabled(true)
            .setWalMode(walMode)
            .setMaxWalArchiveSize(10 * segmentSz)
            .setWalSegmentSize(segmentSz)
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(new CacheConfiguration<>(TX_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.beforeTest();
    }

    /** Simplest CDC test. */
    @Test
    public void testReadAllKeys() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        TestCDCConsumer cnsmr = new TestCDCConsumer();

        ChangeDataCapture cdc = new ChangeDataCapture(cfg, null, cdcConfig(cnsmr));

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, User> txCache = ign.getOrCreateCache(TX_CACHE_NAME);

        addAndWaitForConsumption(cnsmr, cdc, cache, txCache, ChangeDataCaptureSelfTest::addData, 0, KEYS_CNT * 2, getTestTimeout());

        removeData(cache, 0, KEYS_CNT);

        IgniteInternalFuture<?> rmvFut = runAsync(cdc);

        assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, getTestTimeout(), cnsmr));

        rmvFut.cancel();

        assertTrue(cnsmr.stopped);
    }

    /** */
    @Test
    public void testReadBeforeStop() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch onChangeLatch1 = new CountDownLatch(1);
        CountDownLatch onChangeLatch2 = new CountDownLatch(1);

        TestCDCConsumer cnsmr = new TestCDCConsumer() {
            @Override public void start() {
                try {
                    startLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                super.start();
            }

            @Override public boolean onEvents(Iterator<ChangeDataCaptureEvent> evts) {
                onChangeLatch1.countDown();

                try {
                    onChangeLatch2.await(getTestTimeout(), TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                return super.onEvents(evts);
            }
        };

        ChangeDataCapture cdc = new ChangeDataCapture(cfg, null, cdcConfig(cnsmr));

        runAsync(cdc);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);

        Thread.sleep(2 * WAL_ARCHIVE_TIMEOUT);

        startLatch.countDown();

        onChangeLatch1.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        cdc.stop();

        onChangeLatch1.await(getTestTimeout(), TimeUnit.MILLISECONDS);
        onChangeLatch2.countDown();

        assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr));
        assertTrue(cnsmr.stopped);

        List<Integer> keys = cnsmr.keys(UPDATE, cacheId(DEFAULT_CACHE_NAME));

        assertEquals(KEYS_CNT, keys.size());

        for (int i = 0; i < KEYS_CNT; i++)
            assertTrue(keys.contains(i));
    }

    /** */
    @Test
    @Ignore("Not implemented yet")
    public void testReadAfterNodeStop() throws Exception {
        cleanPersistenceDir();

        AtomicInteger cnt = new AtomicInteger();

        TestCDCConsumer cnsmr = new TestCDCConsumer();

        // Restart node several time to make sure we can continue after gracefull shutdown.
        for (int restarts = 0; restarts < 2; restarts++) {
            IgniteConfiguration cfg = getConfiguration("ignite-0");

            Ignite ign = startGrid(cfg);

            ign.cluster().state(ACTIVE);

            long startCnt = cnt.get();

            runAsync(() -> {
                IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

                while (true) {
                    byte[] bytes = new byte[1024];

                    ThreadLocalRandom.current().nextBytes(bytes);

                    int key = cnt.getAndIncrement();

                    try {
                        cache.put(key, new User("John Connor " + key, 42 + key, bytes));
                    }
                    catch (Exception e) {
                        cnt.decrementAndGet();

                        throw e;
                    }
                }
            });

            waitForCondition(() -> cnt.get() - startCnt > KEYS_CNT, getTestTimeout());

            ign.close();

            ChangeDataCapture cdc = new ChangeDataCapture(cfg, null, cdcConfig(cnsmr));

            IgniteInternalFuture<?> fut = runAsync(cdc);

            assertTrue(waitForSize(cnt.get(), DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr));

            fut.cancel();

            List<Integer> keys = cnsmr.keys(UPDATE, cacheId(DEFAULT_CACHE_NAME));

            assertTrue(cnt.get() <= keys.size());

            for (int i = 0; i < cnt.get(); i++)
                assertTrue(keys.contains(i));
        }

    }

    /** Simplest CDC test. */
    @Test
    public void testRestoreStateAfterStop() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        TestCDCConsumer cnsmr = new TestCDCConsumer();

        ChangeDataCapture cdc = new ChangeDataCapture(cfg, null, cdcConfig(cnsmr));

        IgniteInternalFuture<?> runFut = runAsync(cdc);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, User> txCache = ign.getOrCreateCache(TX_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);
        addData(txCache, 0, KEYS_CNT);

        CountDownLatch latch = new CountDownLatch(2);

        IgniteInternalFuture<?> restartFut = runAsync(() -> {
            try {
                assertTrue(waitForSize(2, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr));
                assertTrue(waitForSize(2, TX_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr));

                runFut.cancel();

                assertTrue(cnsmr.stopped);

                latch.countDown();
                latch.await(getTestTimeout(), TimeUnit.MILLISECONDS);

                cdc.run();
            }
            catch (IgniteCheckedException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        latch.countDown();
        latch.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        addData(cache, KEYS_CNT, KEYS_CNT * 2);
        addData(txCache, KEYS_CNT, KEYS_CNT * 2);

        assertTrue(waitForSize(KEYS_CNT * 2, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr));
        assertTrue(waitForSize(KEYS_CNT * 2, TX_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr));

        restartFut.cancel();

        List<Integer> keys = cnsmr.keys(UPDATE, cacheId(DEFAULT_CACHE_NAME));

        for (int i = 0; i < KEYS_CNT * 2; i++)
            assertTrue(keys.contains(i));

        assertTrue(cnsmr.stopped);
    }

    /** */
    @Test
    public void testTwoGrids() throws Exception {
        IgniteEx ign1 = startGrid(0);

        if (specificConsistentId)
            consistentId = UUID.randomUUID();

        IgniteEx ign2 = startGrid(1);

        ign1.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign1.getOrCreateCache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<?> addDataFut = runAsync(() -> addData(cache, 0, KEYS_CNT));

        TestCDCConsumer cnsmr1 = new TestCDCConsumer();
        TestCDCConsumer cnsmr2 = new TestCDCConsumer();

        IgniteConfiguration cfg1 = ign1.configuration();
        IgniteConfiguration cfg2 = ign2.configuration();

        ChangeDataCapture cdc1 = new ChangeDataCapture(cfg1, null, cdcConfig(cnsmr1));
        ChangeDataCapture cdc2 = new ChangeDataCapture(cfg2, null, cdcConfig(cnsmr2));

        IgniteInternalFuture<?> fut1 = runAsync(cdc1);

        IgniteInternalFuture<?> fut2 = runAsync(cdc2);

        addDataFut.get(getTestTimeout());

        addDataFut = runAsync(() -> addData(cache, KEYS_CNT, KEYS_CNT * 2));

        addDataFut.get(getTestTimeout());

        assertTrue(waitForSize(KEYS_CNT * 2, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr1, cnsmr2));

        assertFalse(cnsmr1.stopped);
        assertFalse(cnsmr2.stopped);

        fut1.cancel();
        fut2.cancel();

        assertTrue(cnsmr1.stopped);
        assertTrue(cnsmr2.stopped);

        removeData(cache, 0, KEYS_CNT * 2);

        IgniteInternalFuture<?> rmvFut1 = runAsync(cdc1);
        IgniteInternalFuture<?> rmvFut2 = runAsync(cdc2);

        assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, getTestTimeout(), cnsmr1, cnsmr2));

        rmvFut1.cancel();
        rmvFut2.cancel();

        assertTrue(cnsmr1.stopped);
        assertTrue(cnsmr2.stopped);
    }

    /** */
    @Test
    public void testOneOfConcurrentRunsFail() throws Exception {
        IgniteEx ign = startGrid(0);

        TestCDCConsumer cnsmr1 = new TestCDCConsumer();
        TestCDCConsumer cnsmr2 = new TestCDCConsumer();

        IgniteInternalFuture<?> fut1 = runAsync(new ChangeDataCapture(ign.configuration(), null, cdcConfig(cnsmr1)));
        IgniteInternalFuture<?> fut2 = runAsync(new ChangeDataCapture(ign.configuration(), null, cdcConfig(cnsmr2)));

        assertTrue(waitForCondition(() -> fut1.isDone() || fut2.isDone(), getTestTimeout()));

        if (fut1.isDone()) {
            assertNotNull(fut1.error());

            assertFalse(fut2.isDone());

            fut2.cancel();

            assertTrue(cnsmr2.stopped);
        }
        else {
            assertNotNull(fut2.error());

            assertFalse(fut1.isDone());

            fut1.cancel();

            assertTrue(cnsmr1.stopped);
        }
    }

    /** */
    @Test
    public void testReReadIfNoCommit() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteEx ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);

        for (int i = 0; i < 3; i++) {
            TestCDCConsumer cnsmr = new TestCDCConsumer() {
                @Override protected boolean commit() {
                    return false;
                }
            };

            ChangeDataCapture cdc = new ChangeDataCapture(cfg, null, cdcConfig(cnsmr));

            IgniteInternalFuture<?> fut = runAsync(cdc);

            assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr));

            fut.cancel();

            assertTrue(cnsmr.stopped);
        }

        final int[] expSz = {KEYS_CNT};

        TestCDCConsumer cnsmr = new TestCDCConsumer() {
            @Override protected boolean commit() {
                // Commiting on the half of the data.
                List<Integer> keys = keys(UPDATE, cacheId(DEFAULT_CACHE_NAME));

                if (keys == null)
                    return false;

                int sz = keys.size();

                if (sz >= KEYS_CNT / 2) {
                    expSz[0] = KEYS_CNT - sz;

                    return true;
                }

                return false;
            }
        };

        ChangeDataCapture cdc = new ChangeDataCapture(cfg, null, cdcConfig(cnsmr));

        IgniteInternalFuture<?> fut = runAsync(cdc);

        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr);

        fut.cancel();

        assertTrue(cnsmr.stopped);

        removeData(cache, 0, KEYS_CNT);

        fut = runAsync(cdc);

        waitForSize(expSz[0], DEFAULT_CACHE_NAME, UPDATE, getTestTimeout(), cnsmr);
        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, getTestTimeout(), cnsmr);

        fut.cancel();

        assertTrue(cnsmr.stopped);
    }

    /** */
    public static boolean waitForSize(
        int expSz,
        String cacheName,
        ChangeEventType evtType,
        long timeout,
        TestCDCConsumer... cnsmrs
    ) throws IgniteInterruptedCheckedException {
        return waitForCondition(
            () -> {
                int sum = Arrays.stream(cnsmrs).mapToInt(c -> F.size(c.keys(evtType, cacheId(cacheName)))).sum();
                return sum >= expSz;
            },
            timeout);
    }

    /** */
    public static void addData(IgniteCache<Integer, User> cache, int from, int to) {
        for (int i = from; i < to; i++) {
            byte[] bytes = new byte[1024];

            ThreadLocalRandom.current().nextBytes(bytes);

            cache.put(i, new User("John Connor " + i, 42 + i, bytes));
        }
    }

    /** */
    private void removeData(IgniteCache<Integer, ?> cache, int from, int to) {
        for (int i = from; i < to; i++)
            cache.remove(i);
    }

    /** */
    public static void addAndWaitForConsumption(
        TestCDCConsumer cnsmr,
        ChangeDataCapture cdc,
        IgniteCache<Integer, User> cache,
        IgniteCache<Integer, User> txCache,
        CI3<IgniteCache<Integer, User>, Integer, Integer> addData,
        int from,
        int to,
        long timeout
    ) throws IgniteCheckedException {
        IgniteInternalFuture<?> fut = runAsync(cdc);

        addData.apply(cache, from, to);

        if (txCache != null)
            addData.apply(txCache, from, to);

        assertTrue(waitForSize(to - from, cache.getName(), UPDATE, timeout, cnsmr));

        if (txCache != null)
            assertTrue(waitForSize(to - from, txCache.getName(), UPDATE, timeout, cnsmr));

        fut.cancel();

        List<Integer> keys = cnsmr.keys(UPDATE, cacheId(cache.getName()));

        assertEquals(to - from, keys.size());

        for (int i = from; i < to; i++)
            assertTrue(Integer.toString(i), keys.contains(i));

        assertTrue(cnsmr.stopped);
    }

    /** */
    public static class TestCDCConsumer implements ChangeDataCaptureConsumer {
        /** Keys */
        final ConcurrentMap<IgniteBiTuple<ChangeEventType, Integer>, List<Integer>> cacheKeys =
            new ConcurrentHashMap<>();

        /** */
        volatile boolean stopped;

        /** */
        volatile byte drId = -1;

        /** */
        volatile byte otherDrId = -1;

        /** {@inheritDoc} */
        @Override public void start() {
            stopped = false;
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            stopped = true;
        }

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<ChangeDataCaptureEvent> evts) {
            evts.forEachRemaining(evt -> {
                if (!evt.primary())
                    return;

                cacheKeys.computeIfAbsent(
                    F.t(evt.value() == null ? DELETE : UPDATE, evt.cacheId()),
                    k -> new ArrayList<>()).add((Integer)evt.key()
                );

                if (drId != -1)
                    assertEquals(drId, evt.version().dataCenterId());

                if (otherDrId != -1)
                    assertEquals(otherDrId, evt.version().otherDataCenterOrder().dataCenterId());

                if (evt.value() != null) {
                    assertTrue(((User)evt.value()).getName().startsWith("John Connor"));
                    assertTrue(((User)evt.value()).getAge() >= 42);
                }
            });

            return commit();
        }

        /** */
        protected boolean commit() {
            return true;
        }

        /** @return Read keys. */
        public List<Integer> keys(ChangeEventType op, int cacheId) {
            return cacheKeys.get(F.t(op, cacheId));
        }
    }

    /** */
    public static class User {
        /** */
        private final String name;

        /** */
        private final int age;

        /** */
        private final byte[] payload;

        /** */
        public User(String name, int age, byte[] payload) {
            this.name = name;
            this.age = age;
            this.payload = payload;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public int getAge() {
            return age;
        }

        /** */
        public byte[] getPayload() {
            return payload;
        }
    }

    /** */
    public static ChangeDataCaptureConfiguration cdcConfig(ChangeDataCaptureConsumer cnsmr) {
        ChangeDataCaptureConfiguration cdcCfg = new ChangeDataCaptureConfiguration();

        cdcCfg.setConsumer(cnsmr);
        cdcCfg.setKeepBinary(false);

        return cdcCfg;
    }

    public enum ChangeEventType {
        UPDATE, DELETE
    }
}
