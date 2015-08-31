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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.dr.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import javax.cache.processor.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Update partition index.
 */
public abstract class CachePartUpdateIndexTxAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setNearConfiguration(nearConfiguration());
        cacheCfg.setRebalanceMode(ASYNC);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        if (isStoreEnabled()) {
            cacheCfg.setCacheStoreFactory(new TestStoreFactory());
            cacheCfg.setReadThrough(true);
            cacheCfg.setWriteThrough(true);
            cacheCfg.setLoadPreviousValue(true);
        }

        cacheCfg.setBackups(2);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @return Distribution.
     */
    protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @return If {@code true} then cache start with store.
     */
    protected boolean isStoreEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /**
     * @return Transaction isolation.
     */
    public TransactionIsolation txIsolation(){
        return TransactionIsolation.REPEATABLE_READ;
    }

    /**
     * @return Transaction concurrency.
     */
    public TransactionConcurrency txConcurrency() {
        return TransactionConcurrency.PESSIMISTIC;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (int i = 0; i < gridCount(); i++) {
                    if (grid(i).cluster().nodes().size() != gridCount())
                        return false;
                }

                return true;
            }
        }, 3000);

        for (int i = 0; i < gridCount(); i++)
            assertEquals(gridCount(), grid(i).cluster().nodes().size());

        for (int i = 0; i < gridCount(); i++) {
            for (int j = 0; j < 5; j++) {
                try {
                    IgniteCache<Object, Object> cache = grid(i).cache(null);

                    for (Cache.Entry<Object, Object> entry :
                        cache.localEntries(new CachePeekMode[] {CachePeekMode.ALL}))
                        cache.remove(entry.getKey());

                    break;
                }
                catch (IgniteException e) {
                    if (j == 4)
                        throw new Exception("Failed to clear cache for grid: " + i, e);

                    U.warn(log, "Failed to clear cache for grid (will retry in 500 ms) [gridIdx=" + i +
                        ", err=" + e.getMessage() + ']');

                    U.sleep(500);
                }
            }
        }

        for (int i = 0; i < gridCount(); i++)
            assertEquals("Cache is not empty [entrySet=" + grid(i).cache(null).localEntries() +
                ", i=" + i + ']', 0, grid(i).cache(null).localSize());
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return Grids count.
     */
    protected abstract int gridCount();

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        final IgniteCache<Integer, Integer> cache = ignite(0).cache(null);

        Affinity<Object> aff = ignite(0).affinity(null);

        // Map partition on entry with counters.
        ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> primaryCntrs = new ConcurrentHashMap<>(),
            backupCntrs = new ConcurrentHashMap<>();

        initDrManager(primaryCntrs, backupCntrs, aff);

        Iterator<ClusterNode> iter = aff.mapKeyToPrimaryAndBackups(0).iterator();

        log.info("Primary node: " + iter.next().id());
        log.info("Backup node: " + iter.next().id());

        final List<Integer> keys = new ArrayList<>();

        final int threadCnt = 4;

        final int keyCnt = 500;

        final int partCnt = 3;

        for (int i = 0; i <= 1024 * keyCnt; i+= 1024) {
            keys.add(i);
            keys.add(i + 1);
            keys.add(i + 2);
        }

        final AtomicInteger startIdx = new AtomicInteger(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int b = startIdx.getAndIncrement() * (keyCnt * partCnt) / threadCnt;
                int end = b + (keyCnt * partCnt) / threadCnt;

                try (Transaction tx = createTx()) {
                    for (int i = b; i < end; i++)
                        cache.put(keys.get(i), keys.get(i));

                    tx.commit();
                }
            }
        }, threadCnt, "putter-tx-");

        checkMap(primaryCntrs, backupCntrs);

        primaryCntrs.clear();
        backupCntrs.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutImplicitTx() throws Exception {
        final IgniteCache<Integer, Integer> cache = ignite(0).cache(null);

        Affinity<Object> aff = ignite(0).affinity(null);

        // Map partition on entry with counters.
        ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> primaryCntrs = new ConcurrentHashMap<>(),
            backupCntrs = new ConcurrentHashMap<>();

        initDrManager(primaryCntrs, backupCntrs, aff);

        Iterator<ClusterNode> iter = aff.mapKeyToPrimaryAndBackups(0).iterator();

        log.info("Primary node: " + iter.next().id());
        log.info("Backup node: " + iter.next().id());

        final List<Integer> keys = new ArrayList<>();

        final int threadCnt = 4;

        final int keyCnt = 500;

        final int partCnt = 3;

        for (int i = 0; i <= 1024 * keyCnt; i+= 1024) {
            keys.add(i);
            keys.add(i + 1);
            keys.add(i + 2);
        }

        final AtomicInteger startIdx = new AtomicInteger(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int b = startIdx.getAndIncrement() * (keyCnt * partCnt) / threadCnt;
                int end = b + (keyCnt * partCnt) / threadCnt;

                for (int i = b; i < end; i++)
                    cache.put(keys.get(i), keys.get(i));
            }
        }, threadCnt, "putter-tx-");

        checkMap(primaryCntrs, backupCntrs);

        primaryCntrs.clear();
        backupCntrs.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        final IgniteCache<Integer, Integer> cache = ignite(0).cache(null);

        Affinity<Object> aff = ignite(0).affinity(null);

        // Map partition on entry with counters.
        ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> primaryCntrs = new ConcurrentHashMap<>(),
            backupCntrs = new ConcurrentHashMap<>();

        initDrManager(primaryCntrs, backupCntrs, aff);

        Iterator<ClusterNode> iter = aff.mapKeyToPrimaryAndBackups(0).iterator();

        log.info("Primary node: " + iter.next().id());
        log.info("Backup node: " + iter.next().id());

        final List<Integer> keys = new ArrayList<>();

        final int threadCnt = 4;

        final int keyCnt = 500;

        final int partCnt = 3;

        for (int i = 0; i <= 1024 * keyCnt; i+= 1024) {
            keys.add(i);
            keys.add(i + 1);
            keys.add(i + 2);
        }

        final AtomicInteger startIdx = new AtomicInteger(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int b = startIdx.getAndIncrement() * (keyCnt * partCnt) / threadCnt;
                int end = b + (keyCnt * partCnt) / threadCnt;

                try (Transaction tx = createTx()) {
                    for (int i = b; i < end; i++)
                        cache.put(keys.get(i), keys.get(i));

                    tx.commit();
                }
            }
        }, threadCnt, "putter-tx-");

        checkMap(primaryCntrs, backupCntrs);

        primaryCntrs.clear();
        backupCntrs.clear();

        startIdx.set(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int b = startIdx.getAndIncrement() * (keyCnt * partCnt) / threadCnt;
                int end = b + (keyCnt * partCnt) / threadCnt;

                try (Transaction tx = createTx()) {
                    for (int i = b; i < end; i++)
                        cache.remove(keys.get(i));

                    tx.commit();
                }
            }
        }, threadCnt, "remover-tx-");

        checkMap(primaryCntrs, backupCntrs);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveImplicitTx() throws Exception {
        final IgniteCache<Integer, Integer> cache = ignite(0).cache(null);

        Affinity<Object> aff = ignite(0).affinity(null);

        // Map partition on entry with counters.
        ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> primaryCntrs = new ConcurrentHashMap<>(),
            backupCntrs = new ConcurrentHashMap<>();

        initDrManager(primaryCntrs, backupCntrs, aff);

        Iterator<ClusterNode> iter = aff.mapKeyToPrimaryAndBackups(0).iterator();

        log.info("Primary node: " + iter.next().id());
        log.info("Backup node: " + iter.next().id());

        final List<Integer> keys = new ArrayList<>();

        final int threadCnt = 4;

        final int keyCnt = 500;

        final int partCnt = 3;

        for (int i = 0; i <= 1024 * keyCnt; i+= 1024) {
            keys.add(i);
            keys.add(i + 1);
            keys.add(i + 2);
        }

        final AtomicInteger startIdx = new AtomicInteger(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int b = startIdx.getAndIncrement() * (keyCnt * partCnt) / threadCnt;
                int end = b + (keyCnt * partCnt) / threadCnt;

                for (int i = b; i < end; i++)
                    cache.put(keys.get(i), keys.get(i));
            }
        }, threadCnt, "putter-tx-");

        checkMap(primaryCntrs, backupCntrs);

        primaryCntrs.clear();
        backupCntrs.clear();

        startIdx.set(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int b = startIdx.getAndIncrement() * (keyCnt * partCnt) / threadCnt;
                int end = b + (keyCnt * partCnt) / threadCnt;

                for (int i = b; i < end; i++)
                    cache.remove(keys.get(i));
            }
        }, threadCnt, "remover-tx-");

        checkMap(primaryCntrs, backupCntrs);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvoke() throws Exception {
        final IgniteCache<Integer, Integer> cache = ignite(0).cache(null);

        Affinity<Object> aff = ignite(0).affinity(null);

        // Map partition on entry with counters.
        ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> primaryCntrs = new ConcurrentHashMap<>(),
            backupCntrs = new ConcurrentHashMap<>();

        initDrManager(primaryCntrs, backupCntrs, aff);

        Iterator<ClusterNode> iter = aff.mapKeyToPrimaryAndBackups(0).iterator();

        log.info("Primary node: " + iter.next().id());
        log.info("Backup node: " + iter.next().id());

        final List<Integer> keys = new ArrayList<>();

        final int threadCnt = 4;

        final int keyCnt = 500;

        final int partCnt = 3;

        for (int i = 0; i <= 1024 * keyCnt; i+= 1024) {
            keys.add(i);
            keys.add(i + 1);
            keys.add(i + 2);
        }

        final AtomicInteger startIdx = new AtomicInteger(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int b = startIdx.getAndIncrement() * (keyCnt * partCnt) / threadCnt;
                int end = b + (keyCnt * partCnt) / threadCnt;

                try (Transaction tx = createTx()) {
                    for (int i = b; i < end; i++)
                        cache.put(keys.get(i), keys.get(i));

                    tx.commit();
                }
            }
        }, threadCnt, "putter-tx-");

        checkMap(primaryCntrs, backupCntrs);

        primaryCntrs.clear();
        backupCntrs.clear();

        startIdx.set(0);

        final int threshold = 6;

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int b = startIdx.getAndIncrement() * (keyCnt * partCnt) / threadCnt;
                int end = b + (keyCnt * partCnt) / threadCnt;

                Set<Integer> entKeys = new TreeSet<>();

                for (int i = b; i < end; i++) {
                    entKeys.add(i);

                    if (entKeys.size() >= threshold)
                        commitAndClear(entKeys);
                }

                if (!entKeys.isEmpty())
                    commitAndClear(entKeys);
            }

            private void commitAndClear(Set<Integer> entKeys) {
                try (Transaction tx = createTx()) {
                    cache.invokeAll(entKeys, new TestEntryProcessor());

                    tx.commit();
                }

                entKeys.clear();
            }
        }, threadCnt, "putter-tx-");

        checkMap(primaryCntrs, backupCntrs);
    }

    /**
     * @return Transaction.
     */
    private Transaction createTx() {
        return ignite(0).transactions().txStart(txConcurrency(), txIsolation());
    }

    /**
     * Check primary and backup counters.
     *
     * @param primCntrs Primary counters.
     * @param backCntrs Backup counters.
     */
    private void checkMap(Map<Integer, List<IgniteBiTuple<Integer, Long>>> primCntrs,
        Map<Integer, List<IgniteBiTuple<Integer, Long>>> backCntrs) {

        for (Map.Entry<Integer, List<IgniteBiTuple<Integer, Long>>> e : backCntrs.entrySet()) {
            List<IgniteBiTuple<Integer, Long>> primaryVals = primCntrs.get(e.getKey());
            List<IgniteBiTuple<Integer, Long>> backupVals = e.getValue();

            assert backupVals.size() >= primaryVals.size();

            for (IgniteBiTuple<Integer, Long> backVal : backupVals)
                assert primaryVals.contains(backVal) :
                    "Failed. Partition: " + e.getKey() + ", key: " + backVal.get1() + ", cnrt: " + backVal.get2();

            for (IgniteBiTuple<Integer, Long> val : primaryVals)
                assert backupVals.contains(val);
        }
    }

    /**
     * Init dr manager.
     *
     * @param primaryCntrs primary map.
     * @param backupCntrs backup map.
     * @param aff Affinity.
     */
    private void initDrManager(ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> primaryCntrs,
        ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> backupCntrs, Affinity<Object> aff) {
        for (int i = 0; i < gridCount(); i++)
            GridTestUtils.setFieldValue(((IgniteKernal)grid(i)).internalCache(null).context(),
                "drMgr",
                new TestCacheDrManager(primaryCntrs, backupCntrs, aff));
    }

    /**
     *
     */
    private static class TestCacheDrManager extends GridOsCacheDrManager {
        /** */
        private ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> primary;

        /** */
        private ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> backup;

        /** */
        private Affinity<Object> aff;

        /**
         * @param primary Primary map.
         * @param backup Backup map.
         * @param aff Affinity.
         */
        public TestCacheDrManager(ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> primary,
            ConcurrentMap<Integer, List<IgniteBiTuple<Integer, Long>>> backup, Affinity<Object> aff) {
            this.primary = primary;
            this.backup = backup;
            this.aff = aff;
        }

        /** {@inheritDoc */
        @Override public boolean enabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void replicate(KeyCacheObject key, @Nullable CacheObject val, long ttl, long expireTime,
            GridCacheVersion ver, GridDrType drType, long updatePartIdx) {
            Integer key0 = key.value(null, false);

            int part = aff.partition(key0);

            if (drType == GridDrType.DR_PRIMARY) {
                List<IgniteBiTuple<Integer, Long>> vals = primary.get(part);

                if (vals == null) {
                    vals = new CopyOnWriteArrayList<>();

                    List<IgniteBiTuple<Integer, Long>> oldVals = primary.putIfAbsent(part, vals);

                    if (oldVals != null)
                        vals = oldVals;
                }

                vals.add(F.t(key0, updatePartIdx));
            }
            else if (drType == GridDrType.DR_BACKUP) {
                List<IgniteBiTuple<Integer, Long>> vals = backup.get(part);

                if (vals == null) {
                    vals = new CopyOnWriteArrayList<>();

                    List<IgniteBiTuple<Integer, Long>> oldVals = backup.putIfAbsent(part, vals);

                    if (oldVals != null)
                        vals = oldVals;
                }

                vals.add(F.t(key0, updatePartIdx));
            }
        }
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore> {
        @Override public CacheStore create() {
            return new TestStore();
        }
    }

    /**
     * Store.
     */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (int i = 0; i < 10; i++)
                clo.apply(i, i);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object load(Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }

    /**
     *
     */
    public static class TestEntryProcessor implements CacheEntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> entry,
            Object... arguments) throws EntryProcessorException {
            entry.setValue(entry.getValue() + 1);

            return entry.getValue();
        }
    }
}

