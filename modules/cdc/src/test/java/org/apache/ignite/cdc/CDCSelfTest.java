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
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.wal.record.DataEntry;
import org.apache.ignite.wal.record.DataRecord;
import org.apache.ignite.wal.record.MarshalledDataEntry;
import org.apache.ignite.wal.record.UnwrappedDataEntry;
import org.apache.ignite.wal.record.WALRecord;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class CDCSelfTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public boolean specificConsistentId;

    @Parameterized.Parameters(name = "specificConsistentId={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{true}, {false},});
    }

    /** Keys count. */
    private static final int KEYS_CNT = 1_000;

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
            .setWalForceArchiveTimeout(1_000)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

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
        StoreKeysConsumer consumer = new StoreKeysConsumer();

        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteCDC cdc = new IgniteCDC(cfg, consumer);

        IgniteInternalFuture<?> fut;

        fut = runAsync(cdc);

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, byte[]> cache = ign.createCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);

        assertTrue(waitForSize(KEYS_CNT, consumer));

        fut.cancel();

        Set<Integer> keys = consumer.keys(cacheId(DEFAULT_CACHE_NAME));

        for (int i = 0; i < KEYS_CNT; i++)
            assertTrue(keys.contains(i));

        assertTrue(consumer.stoped);
    }

    /** Simplest CDC test. */
    @Test
    public void testRestoreStateAfterStop() throws Exception {
        StoreKeysConsumer consumer = new StoreKeysConsumer();

        IgniteConfiguration cfg = getConfiguration("ignite-0");

        IgniteCDC cdc = new IgniteCDC(cfg, consumer);

        IgniteInternalFuture<?> runFut = runAsync(cdc);

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, byte[]> cache = ign.createCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);

        IgniteInternalFuture<?> restartFut = runAsync(() -> {
            try {
                assertTrue(waitForSize(2, consumer));

                runFut.cancel();

                assertTrue(consumer.stoped);

                cdc.run();
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        });

        addData(cache, KEYS_CNT, KEYS_CNT * 2);

        assertTrue(waitForSize(KEYS_CNT * 2, consumer));

        restartFut.cancel();

        Set<Integer> keys = consumer.keys(cacheId(DEFAULT_CACHE_NAME));

        for (int i = 0; i < KEYS_CNT * 2; i++)
            assertTrue(keys.contains(i));

        assertTrue(consumer.stoped);
    }

    @Test
    public void testTwoGrids() throws Exception {
        IgniteEx ign1 = startGrid(0);
        IgniteEx ign2 = startGrid(1);

        ign1.cluster().state(ACTIVE);

        IgniteCache<Integer, byte[]> cache = ign1.createCache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<?> addDataFut = runAsync(() -> addData(cache, 0, KEYS_CNT));

        StoreKeysConsumer consumer1 = new StoreKeysConsumer();
        StoreKeysConsumer consumer2 = new StoreKeysConsumer();

        IgniteConfiguration cfg1 = ign1.configuration();
        IgniteConfiguration cfg2 = ign2.configuration();

        try {
            System.setProperty(IgniteCDC.IGNITE_CDC_CONSISTENT_ID, Objects.toString(ign1.localNode().consistentId()));
            System.setProperty(IgniteCDC.IGNITE_CDC_NODE_IDX, "0");

            IgniteCDC cdc1 = new IgniteCDC(cfg1, consumer1);

            System.setProperty(IgniteCDC.IGNITE_CDC_CONSISTENT_ID, Objects.toString(ign2.localNode().consistentId()));
            System.setProperty(IgniteCDC.IGNITE_CDC_NODE_IDX, "1");

            IgniteCDC cdc2 = new IgniteCDC(cfg2, consumer2);

            IgniteInternalFuture<?> fut1 = runAsync(cdc1);

            IgniteInternalFuture<?> fut2 = runAsync(cdc2);

            addDataFut.get(getTestTimeout());

            addDataFut = runAsync(() -> addData(cache, KEYS_CNT, KEYS_CNT * 2));

            addDataFut.get(getTestTimeout());

            assertTrue(waitForSize(KEYS_CNT * 2, consumer1, consumer2));

            assertFalse(consumer1.stoped);
            assertFalse(consumer2.stoped);

            fut1.cancel();
            fut2.cancel();

            assertTrue(consumer1.stoped);
            assertTrue(consumer2.stoped);
        }
        finally {
            System.clearProperty(IgniteCDC.IGNITE_CDC_CONSISTENT_ID);
            System.clearProperty(IgniteCDC.IGNITE_CDC_NODE_IDX);
        }
    }

    @Test
    public void testOneOfConcurrentRunsFail() throws Exception {
        IgniteEx ign = startGrid(0);

        StoreKeysConsumer consumer1 = new StoreKeysConsumer();
        StoreKeysConsumer consumer2 = new StoreKeysConsumer();

        IgniteInternalFuture<?> fut1 = runAsync(new IgniteCDC(ign.configuration(), consumer1));
        IgniteInternalFuture<?> fut2 = runAsync(new IgniteCDC(ign.configuration(), consumer2));

        assertTrue(waitForCondition(() -> fut1.isDone() || fut2.isDone(), getTestTimeout()));

        if (fut1.isDone()) {
            assertNotNull(fut1.error());

            assertFalse(fut2.isDone());

            fut2.cancel();

            assertTrue(consumer2.stoped);
        }
        else {
            assertNotNull(fut2.error());

            assertFalse(fut1.isDone());

            fut1.cancel();

            assertTrue(consumer1.stoped);
        }
    }

    @Test
    public void testReReadIfNoCommit() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

        Ignite ign = startGrid(cfg);

        ign.cluster().state(ACTIVE);

        IgniteCache<Integer, byte[]> cache = ign.createCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, KEYS_CNT);

        for (int i=0; i<3; i++) {
            StoreKeysConsumer consumer = new StoreKeysConsumer() {
                @Override protected boolean commit() {
                    return false;
                }
            };

            IgniteCDC cdc = new IgniteCDC(cfg, consumer);

            IgniteInternalFuture<?> fut = runAsync(cdc);

            assertTrue(waitForSize(KEYS_CNT, consumer));

            fut.cancel();

            assertTrue(consumer.stoped);
        }

        final int[] expSz = {KEYS_CNT};

        StoreKeysConsumer consumer = new StoreKeysConsumer() {
            @Override public String id() {
                return "half-consumer";
            }

            @Override protected boolean commit() {
                // Commiting on the half of the data.
                int sz = keys(cacheId(DEFAULT_CACHE_NAME)).size();

                if (sz >= KEYS_CNT/2) {
                    expSz[0] = KEYS_CNT - sz;

                    return true;
                }

                return false;
            }
        };

        IgniteCDC cdc = new IgniteCDC(cfg, consumer);

        IgniteInternalFuture<?> fut = runAsync(cdc);

        waitForSize(KEYS_CNT, consumer);

        fut.cancel();

        assertTrue(consumer.stoped);

        fut = runAsync(cdc);

        waitForSize(expSz[0], consumer);

        fut.cancel();

        assertTrue(consumer.stoped);
    }

    /** */
    private boolean waitForSize(int expSz, StoreKeysConsumer... consumers) throws IgniteInterruptedCheckedException {
        return waitForCondition(() ->
            Arrays.stream(consumers).mapToInt(c -> F.size(c.keys(cacheId(DEFAULT_CACHE_NAME)))).sum() >= expSz,
            getTestTimeout());
    }

    /** */
    private void addData(IgniteCache<Integer, byte[]> cache, int from, int to) {
        for (int i = from; i < to; i++) {
            byte[] bytes = new byte[1024];

            ThreadLocalRandom.current().nextBytes(bytes);

            cache.put(i, bytes);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    private static class StoreKeysConsumer extends ProcessDataConsumer<Integer, byte[]> {
        /** Keys */
        private final ConcurrentMap<Integer, Set<Integer>> cacheKeys = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override protected void onData(int cacheId, Integer key, byte[] val) {
            cacheKeys.computeIfAbsent(cacheId, k -> new GridConcurrentHashSet<>()).add(key);
        }

        /** @return Read keys. */
        public Set<Integer> keys(int cacheId) {
            if (cacheKeys == null)
                return null;

            return cacheKeys.get(cacheId);
        }
    }

    /** */
    private abstract static class ProcessDataConsumer<K, V> implements CDCConsumer {
        /** */
        public boolean stoped;

        /** {@inheritDoc} */
        @Override public String id() {
            return getClass().getName();
        }

        /** {@inheritDoc} */
        @Override public <T extends WALRecord> boolean onRecord(T rec) {
            if (rec.type() != WALRecord.RecordType.DATA_RECORD)
                return false;

            DataRecord dataRecord = (DataRecord)rec;

            for (DataEntry entry : dataRecord.writeEntries()) {
                if (entry instanceof UnwrappedDataEntry) {
                    UnwrappedDataEntry unwrapDataEntry = (UnwrappedDataEntry)entry;

                    onData(entry.cacheId(), (K)unwrapDataEntry.unwrappedKey(), (V)unwrapDataEntry.unwrappedValue());
                }
                else if (entry instanceof MarshalledDataEntry) {
                    fail("Unexpected data entry type.");
                }
                else {
                    fail("Unexpected data entry type.");
                }
            }

            return commit();
        }

        /** {@inheritDoc} */
        @Override public void start(IgniteConfiguration configuration, IgniteLogger log) {
            stoped = false;
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            stoped = true;
        }

        /** */
        protected boolean commit() {
            return true;
        }

        /** */
        protected abstract void onData(int cacheId, K key, V val);
    }
}
