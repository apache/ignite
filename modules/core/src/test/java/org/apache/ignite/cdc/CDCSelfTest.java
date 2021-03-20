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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cdc.EntryEventType.DELETE;
import static org.apache.ignite.cdc.EntryEventType.UPDATE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class CDCSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String TX_CACHE_NAME = "tx-cache";

    /** */
    public static final int WAL_ARCHIVE_TIMEOUT = 1_000;

    /** */
    @Parameterized.Parameter
    public boolean specificConsistentId;

    @Parameterized.Parameters(name = "specificConsistentId={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{true}, {false},});
    }

    /** Keys count. */
    private static final int KEYS_CNT = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (specificConsistentId)
            cfg.setConsistentId(UUID.randomUUID());

        int segmentSz = 10 * 1024 * 1024;

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setCdcEnabled(true)
            .setWalMode(WALMode.FSYNC)
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
        TestCDCConsumer lsnr = new TestCDCConsumer();

        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteCDC cdc = new IgniteCDC(cfg, lsnr);

        IgniteInternalFuture<?> fut;

        fut = runAsync(cdc);

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, User> txCache = ign.getOrCreateCache(TX_CACHE_NAME);

        addData(cache, 0, KEYS_CNT * 2);
        addData(txCache, 0, KEYS_CNT * 2);

        assertTrue(waitForSize(KEYS_CNT * 2, DEFAULT_CACHE_NAME, UPDATE, lsnr));
        assertTrue(waitForSize(KEYS_CNT * 2, TX_CACHE_NAME, UPDATE, lsnr));

        fut.cancel();

        List<Integer> keys = lsnr.keys(UPDATE, cacheId(DEFAULT_CACHE_NAME));

        assertEquals(KEYS_CNT * 2, keys.size());

        for (int i = 0; i < KEYS_CNT * 2; i++)
            assertTrue(keys.contains(i));

        assertTrue(lsnr.stoped);

        removeData(cache, 0, KEYS_CNT);

        IgniteInternalFuture<?> rmvFut = runAsync(cdc);

        assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, lsnr));

        rmvFut.cancel();

        assertTrue(lsnr.stoped);

        //TODO: assert empty CDC dir.
    }

    /** Simplest CDC test. */
    @Test
    public void testRestoreStateAfterStop() throws Exception {
        TestCDCConsumer lsnr = new TestCDCConsumer();

        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteCDC cdc = new IgniteCDC(cfg, lsnr);

        IgniteInternalFuture<?> runFut = runAsync(cdc);

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, User> txCache = ign.getOrCreateCache(TX_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);
        addData(txCache, 0, KEYS_CNT);

        IgniteInternalFuture<?> restartFut = runAsync(() -> {
            try {
                assertTrue(waitForSize(2, DEFAULT_CACHE_NAME, UPDATE, lsnr));
                assertTrue(waitForSize(2, TX_CACHE_NAME, UPDATE, lsnr));

                runFut.cancel();

                assertTrue(lsnr.stoped);

                cdc.run();
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        });

        addData(cache, KEYS_CNT, KEYS_CNT * 2);
        addData(txCache, KEYS_CNT, KEYS_CNT * 2);

        assertTrue(waitForSize(KEYS_CNT * 2, DEFAULT_CACHE_NAME, UPDATE, lsnr));
        assertTrue(waitForSize(KEYS_CNT * 2, TX_CACHE_NAME, UPDATE, lsnr));

        restartFut.cancel();

        List<Integer> keys = lsnr.keys(UPDATE, cacheId(DEFAULT_CACHE_NAME));

        for (int i = 0; i < KEYS_CNT * 2; i++)
            assertTrue(keys.contains(i));

        assertTrue(lsnr.stoped);
    }

    @Test
    public void testTwoGrids() throws Exception {
        IgniteEx ign1 = startGrid(0);
        IgniteEx ign2 = startGrid(1);

        ign1.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign1.getOrCreateCache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<?> addDataFut = runAsync(() -> addData(cache, 0, KEYS_CNT));

        TestCDCConsumer lsnr1 = new TestCDCConsumer();
        TestCDCConsumer lsnr2 = new TestCDCConsumer();

        IgniteConfiguration cfg1 = ign1.configuration();
        IgniteConfiguration cfg2 = ign2.configuration();

        try {
            System.setProperty(IgniteCDC.IGNITE_CDC_CONSISTENT_ID, Objects.toString(ign1.localNode().consistentId()));
            System.setProperty(IgniteCDC.IGNITE_CDC_NODE_IDX, "0");

            IgniteCDC cdc1 = new IgniteCDC(cfg1, lsnr1);

            System.setProperty(IgniteCDC.IGNITE_CDC_CONSISTENT_ID, Objects.toString(ign2.localNode().consistentId()));
            System.setProperty(IgniteCDC.IGNITE_CDC_NODE_IDX, "1");

            IgniteCDC cdc2 = new IgniteCDC(cfg2, lsnr2);

            IgniteInternalFuture<?> fut1 = runAsync(cdc1);

            IgniteInternalFuture<?> fut2 = runAsync(cdc2);

            addDataFut.get(getTestTimeout());

            addDataFut = runAsync(() -> addData(cache, KEYS_CNT, KEYS_CNT * 2));

            addDataFut.get(getTestTimeout());

            assertTrue(waitForSize(KEYS_CNT * 2, DEFAULT_CACHE_NAME, UPDATE, lsnr1, lsnr2));

            assertFalse(lsnr1.stoped);
            assertFalse(lsnr2.stoped);

            fut1.cancel();
            fut2.cancel();

            assertTrue(lsnr1.stoped);
            assertTrue(lsnr2.stoped);

            removeData(cache, 0, KEYS_CNT * 2);

            IgniteInternalFuture<?> rmvFut1 = runAsync(cdc1);
            IgniteInternalFuture<?> rmvFut2 = runAsync(cdc2);

            assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, lsnr1, lsnr2));

            rmvFut1.cancel();
            rmvFut2.cancel();

            assertTrue(lsnr1.stoped);
            assertTrue(lsnr2.stoped);
        }
        finally {
            System.clearProperty(IgniteCDC.IGNITE_CDC_CONSISTENT_ID);
            System.clearProperty(IgniteCDC.IGNITE_CDC_NODE_IDX);
        }
    }

    @Test
    public void testOneOfConcurrentRunsFail() throws Exception {
        IgniteEx ign = startGrid(0);

        TestCDCConsumer lsnr1 = new TestCDCConsumer();
        TestCDCConsumer lsnr2 = new TestCDCConsumer();

        IgniteInternalFuture<?> fut1 = runAsync(new IgniteCDC(ign.configuration(), lsnr1));
        IgniteInternalFuture<?> fut2 = runAsync(new IgniteCDC(ign.configuration(), lsnr2));

        assertTrue(waitForCondition(() -> fut1.isDone() || fut2.isDone(), getTestTimeout()));

        if (fut1.isDone()) {
            assertNotNull(fut1.error());

            assertFalse(fut2.isDone());

            fut2.cancel();

            assertTrue(lsnr2.stoped);
        }
        else {
            assertNotNull(fut2.error());

            assertFalse(fut1.isDone());

            fut1.cancel();

            assertTrue(lsnr1.stoped);
        }
    }

    @Test
    public void testReReadIfNoCommit() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);

        for (int i = 0; i < 3; i++) {
            TestCDCConsumer lsnr = new TestCDCConsumer() {
                @Override protected boolean commit() {
                    return false;
                }
            };

            IgniteCDC cdc = new IgniteCDC(cfg, lsnr);

            IgniteInternalFuture<?> fut = runAsync(cdc);

            assertTrue(waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, lsnr));

            fut.cancel();

            assertTrue(lsnr.stoped);
        }

        final int[] expSz = {KEYS_CNT};

        TestCDCConsumer lsnr = new TestCDCConsumer() {
            @Override public String id() {
                return "half-consumer";
            }

            @Override protected boolean commit() {
                // Commiting on the half of the data.
                int sz = keys(UPDATE, cacheId(DEFAULT_CACHE_NAME)).size();

                if (sz >= KEYS_CNT / 2) {
                    expSz[0] = KEYS_CNT - sz;

                    return true;
                }

                return false;
            }
        };

        IgniteCDC cdc = new IgniteCDC(cfg, lsnr);

        IgniteInternalFuture<?> fut = runAsync(cdc);

        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, UPDATE, lsnr);

        fut.cancel();

        assertTrue(lsnr.stoped);

        removeData(cache, 0, KEYS_CNT);

        fut = runAsync(cdc);

        waitForSize(expSz[0], DEFAULT_CACHE_NAME, UPDATE, lsnr);
        waitForSize(KEYS_CNT, DEFAULT_CACHE_NAME, DELETE, lsnr);

        fut.cancel();

        assertTrue(lsnr.stoped);
    }

    /** */
    private boolean waitForSize(int expSz, String cacheName, EntryEventType evtType, TestCDCConsumer... lsnrs) throws IgniteInterruptedCheckedException {
        return waitForCondition(
            () -> Arrays.stream(lsnrs).mapToInt(l -> F.size(l.keys(evtType, cacheId(cacheName)))).sum() >= expSz,
            getTestTimeout());
    }

    /** */
    private void addData(IgniteCache<Integer, User> cache, int from, int to) {
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

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    private static class TestCDCConsumer implements CDCConsumer<Integer, User> {
        /** Keys */
        private final ConcurrentMap<IgniteBiTuple<EntryEventType, Integer>, List<Integer>> cacheKeys = new ConcurrentHashMap<>();

        /** */
        public boolean stoped;

        /** {@inheritDoc} */
        @Override public String id() {
            return getClass().getName();
        }

        /** {@inheritDoc} */
        @Override public void start(IgniteConfiguration configuration, IgniteLogger log) {
            stoped = false;
        }

        /** {@inheritDoc} */
        @Override public boolean keepBinary() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            stoped = true;
        }

        /** {@inheritDoc} */
        @Override public boolean onChange(Iterator<EntryEvent<Integer, User>> events) {
            events.forEachRemaining(evt -> {
                if (!evt.primary())
                    return;

                cacheKeys.computeIfAbsent(F.t(evt.operation(), evt.cacheId()),
                    k -> new ArrayList<>()).add(evt.key());

                if (evt.operation() == UPDATE) {
                    assertTrue(evt.value().getName().startsWith("John Connor"));
                    assertTrue(evt.value().getAge() >= 42);
                }
            });

            return commit();
        }

        /** */
        protected boolean commit() {
            return true;
        }

        /** @return Read keys. */
        public List<Integer> keys(EntryEventType op, int cacheId) {
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
}
