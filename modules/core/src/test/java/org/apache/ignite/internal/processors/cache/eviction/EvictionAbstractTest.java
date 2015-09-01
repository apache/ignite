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

package org.apache.ignite.internal.processors.cache.eviction;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedCache;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.internal.processors.cache.eviction.EvictionAbstractTest.EvictionPolicyProxy.proxy;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Base class for eviction tests.
 */
public abstract class EvictionAbstractTest<T extends EvictionPolicy<?, ?>>
    extends GridCommonAbstractTest {
    /** IP finder. */
    protected static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Put entry size. */
    protected static final int PUT_ENTRY_SIZE = 10;

    /** Replicated cache. */
    protected CacheMode mode = REPLICATED;

    /** Near enabled flag. */
    protected boolean nearEnabled;

    /** Evict backup sync. */
    protected boolean evictSync;

    /** Evict near sync. */
    protected boolean evictNearSync = true;

    /** Policy max. */
    protected int plcMax = 10;

    /** Policy batch size. */
    protected int plcBatchSize = 1;

    /** Policy max memory size. */
    protected long plcMaxMemSize = 0;

    /** Near policy max. */
    protected int nearMax = 3;

    /** Synchronous commit. */
    protected boolean syncCommit;

    /** */
    protected int gridCnt = 2;

    /** */
    protected EvictionFilter<?, ?> filter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);
        cc.setEvictionPolicy(createPolicy(plcMax));
        cc.setEvictSynchronized(evictSync);
        cc.setSwapEnabled(false);
        cc.setWriteSynchronizationMode(syncCommit ? FULL_SYNC : FULL_ASYNC);
        cc.setStartSize(plcMax);
        cc.setAtomicityMode(TRANSACTIONAL);

        if (nearEnabled) {
            NearCacheConfiguration nearCfg = new NearCacheConfiguration();

            nearCfg.setNearEvictionPolicy(createNearPolicy(nearMax));

            cc.setNearConfiguration(nearCfg);
        }
        else
            cc.setNearConfiguration(null);

        if (mode == PARTITIONED)
            cc.setBackups(1);

        if (filter != null)
            cc.setEvictionFilter(filter);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        c.setIncludeProperties();

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        filter = null;

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizePolicy() throws Exception {
        plcMax = 3;
        plcMaxMemSize = 0;
        plcBatchSize = 1;

        doTestPolicy();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizePolicyWithBatch() throws Exception {
        plcMax = 3;
        plcMaxMemSize = 0;
        plcBatchSize = 2;

        doTestPolicyWithBatch();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxMemSizePolicy() throws Exception {
        plcMax = 0;
        plcMaxMemSize = 3 * MockEntry.ENTRY_SIZE;
        plcBatchSize = 1;

        doTestPolicy();
    }

    /**
     * Batch ignored when {@code maxSize > 0} and {@code maxMemSize > 0}.
     *
     * @throws Exception If failed.
     */
    public void testMaxMemSizePolicyWithBatch() throws Exception {
        plcMax = 3;
        plcMaxMemSize = 10 * MockEntry.ENTRY_SIZE;
        plcBatchSize = 2;

        doTestPolicy();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizeMemory() throws Exception {
        int max = 10;

        plcMax = max;
        plcMaxMemSize = 0;
        plcBatchSize = 1;

        doTestMemory(max);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizeMemoryWithBatch() throws Exception {
        int max = 10;

        plcMax = max;
        plcMaxMemSize = 0;
        plcBatchSize = 2;

        doTestMemory(max);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxMemSizeMemory() throws Exception {
        int max = 10;

        plcMax = 0;
        plcMaxMemSize = max * MockEntry.ENTRY_SIZE;
        plcBatchSize = 1;

        doTestMemory(max);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizeRandom() throws Exception {
        plcMax = 10;
        plcMaxMemSize = 0;
        plcBatchSize = 1;

        doTestRandom();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizeRandomWithBatch() throws Exception {
        plcMax = 10;
        plcMaxMemSize = 0;
        plcBatchSize = 2;

        doTestRandom();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxMemSizeRandom() throws Exception {
        plcMax = 0;
        plcMaxMemSize = 10 * MockEntry.KEY_SIZE;
        plcBatchSize = 1;

        doTestRandom();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizeAllowEmptyEntries() throws Exception {
        plcMax = 10;
        plcMaxMemSize = 0;
        plcBatchSize = 1;

        doTestAllowEmptyEntries();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizeAllowEmptyEntriesWithBatch() throws Exception {
        plcMax = 10;
        plcMaxMemSize = 0;
        plcBatchSize = 2;

        doTestAllowEmptyEntries();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxMemSizeAllowEmptyEntries() throws Exception {
        plcMax = 0;
        plcMaxMemSize = 10 * MockEntry.KEY_SIZE;
        plcBatchSize = 1;

        doTestAllowEmptyEntries();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizePut() throws Exception {
        plcMax = 100;
        plcBatchSize = 1;
        plcMaxMemSize = 0;

        doTestPut(plcMax);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxSizePutWithBatch() throws Exception {
        plcMax = 100;
        plcBatchSize = 2;
        plcMaxMemSize = 0;

        doTestPut(plcMax);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxMemSizePut() throws Exception {
        int max = 100;

        plcMax = 0;
        plcBatchSize = 2;
        plcMaxMemSize = max * PUT_ENTRY_SIZE;

        doTestPut(max);
    }

    /**
     * Tests policy behaviour.
     *
     * @throws Exception If failed.
     */
    protected abstract void doTestPolicy() throws Exception;

    /**
     * Tests policy behaviour with batch enabled.
     *
     * @throws Exception If failed.
     */
    protected abstract void doTestPolicyWithBatch() throws Exception;

    /**
     * @throws Exception If failed.
     */
    protected void doTestAllowEmptyEntries() throws Exception {
        try {
            startGrid();

            MockEntry e1 = new MockEntry("1");
            MockEntry e2 = new MockEntry("2");
            MockEntry e3 = new MockEntry("3");
            MockEntry e4 = new MockEntry("4");
            MockEntry e5 = new MockEntry("5");

            EvictionPolicyProxy p = proxy(policy());

            p.onEntryAccessed(false, e1);

            assertFalse(e1.isEvicted());

            check(p.queue().size(), MockEntry.KEY_SIZE);

            p.onEntryAccessed(false, e2);

            assertFalse(e1.isEvicted());
            assertFalse(e2.isEvicted());

            check(p.queue().size(), MockEntry.KEY_SIZE);

            p.onEntryAccessed(false, e3);

            assertFalse(e1.isEvicted());
            assertFalse(e3.isEvicted());

            check(p.queue().size(), MockEntry.KEY_SIZE);

            p.onEntryAccessed(false, e4);

            assertFalse(e1.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());

            check(p.queue().size(), MockEntry.KEY_SIZE);

            p.onEntryAccessed(false, e5);

            assertFalse(e1.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e5.isEvicted());

            check(p.queue().size(), MockEntry.KEY_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected void doTestMemory(int max) throws Exception {
        try {
            startGrid();

            EvictionPolicyProxy p = proxy(policy());

            int cnt = max + plcBatchSize;

            for (int i = 0; i < cnt; i++)
                p.onEntryAccessed(false, new MockEntry(Integer.toString(i), Integer.toString(i)));

            info(p);

            check(max, MockEntry.ENTRY_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected void doTestRandom() throws Exception {
        try {
            startGrid();

            EvictionPolicyProxy p = proxy(policy());

            int max = 10;

            Random rand = new Random();

            int keys = 31;

            MockEntry[] entries = new MockEntry[keys];

            for (int i = 0; i < entries.length; i++)
                entries[i] = new MockEntry(Integer.toString(i));

            int runs = 5000000;

            for (int i = 0; i < runs; i++) {
                boolean rmv = rand.nextBoolean();

                int j = rand.nextInt(entries.length);

                MockEntry e = entry(entries, j);

                if (rmv)
                    entries[j] = new MockEntry(Integer.toString(j));

                p.onEntryAccessed(rmv, e);
            }

            info(p);

            assertTrue(p.getCurrentSize() <= (plcMaxMemSize > 0 ? max : max + plcBatchSize));
            assertTrue(p.getCurrentMemorySize() <= (plcMaxMemSize > 0 ? max : max + plcBatchSize) * MockEntry.KEY_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected void doTestPut(int max) throws Exception {
        mode = LOCAL;
        syncCommit = true;

        try {
            Ignite ignite = startGrid();

            IgniteCache<Object, Object> cache = ignite.cache(null);

            int cnt = 500;

            int min = Integer.MAX_VALUE;

            int minIdx = 0;

            for (int i = 0; i < cnt; i++) {
                cache.put(i, i);

                int cacheSize = cache.size();

                if (i > max && cacheSize < min) {
                    min = cacheSize;
                    minIdx = i;
                }
            }

            assertTrue("Min cache size is too small: " + min, min >= max);

            check(max, PUT_ENTRY_SIZE);

            info("Min cache size [min=" + min + ", idx=" + minIdx + ']');
            info("Current cache size " + cache.size());
            info("Current cache key size " + cache.size());

            min = Integer.MAX_VALUE;

            minIdx = 0;

            // Touch.
            for (int i = cnt; --i > cnt - max;) {
                cache.get(i);

                int cacheSize = cache.size();

                if (cacheSize < min) {
                    min = cacheSize;
                    minIdx = i;
                }
            }

            info("----");
            info("Min cache size [min=" + min + ", idx=" + minIdx + ']');
            info("Current cache size " + cache.size());
            info("Current cache key size " + cache.size());

            check(max, PUT_ENTRY_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param arr Array.
     * @param idx Index.
     * @return Entry at the index.
     */
    protected MockEntry entry(MockEntry[] arr, int idx) {
        MockEntry e = arr[idx];

        if (e.isEvicted())
            e = arr[idx] = new MockEntry(e.getKey());

        return e;
    }

    /**
     * @param prefix Prefix.
     * @param p Policy.
     */
    protected void info(String prefix, EvictionPolicy<?, ?> p) {
        info(prefix + ": " + p.toString());
    }

    /** @param p Policy. */
    protected void info(EvictionPolicy<?, ?> p) {
        info(p.toString());
    }

    /**
     * @param c1 Policy collection.
     * @param c2 Expected list.
     */
    protected static void check(Collection<EvictableEntry<String, String>> c1, MockEntry... c2) {
        check(c1, F.asList(c2));
    }

    /**
     * @param expSize Expected size.
     * @param entrySize Entry size.
     */
    protected void check(int expSize, int entrySize) {
        EvictionPolicyProxy proxy = proxy(policy());

        assertEquals(expSize, proxy.getCurrentSize());
        assertEquals(expSize * entrySize, proxy.getCurrentMemorySize());
    }

    /**
     * @param entrySize Entry size.
     * @param c1 Closure 1.
     * @param c2 Closure 2.
     */
    protected void check(int entrySize, Collection<EvictableEntry<String, String>> c1, MockEntry... c2) {
        check(c2.length, entrySize);

        check(c1, c2);
    }

    /** @return Policy. */
    @SuppressWarnings({"unchecked"})
    protected T policy() {
        return (T)grid().cache(null).getConfiguration(CacheConfiguration.class).getEvictionPolicy();
    }

    /**
     * @param i Grid index.
     * @return Policy.
     */
    @SuppressWarnings({"unchecked"})
    protected T policy(int i) {
        return (T)grid(i).cache(null).getConfiguration(CacheConfiguration.class).getEvictionPolicy();
    }

    /**
     * @param i Grid index.
     * @return Policy.
     */
    @SuppressWarnings({"unchecked"})
    protected T nearPolicy(int i) {
        CacheConfiguration cfg = grid(i).cache(null).getConfiguration(CacheConfiguration.class);

        NearCacheConfiguration nearCfg = cfg.getNearConfiguration();

        return (T)(nearCfg == null ? null : nearCfg.getNearEvictionPolicy());
    }

    /**
     * @param c1 Policy collection.
     * @param c2 Expected list.
     */
    protected static void check(Collection<EvictableEntry<String, String>> c1, List<MockEntry> c2) {
        assert c1.size() == c2.size() : "Mismatch [actual=" + string(c1) + ", expected=" + string(c2) + ']';

        assert c1.containsAll(c2) : "Mismatch [actual=" + string(c1) + ", expected=" + string(c2) + ']';

        int i = 0;

        // Check order.
        for (Cache.Entry<String, String> e : c1)
            assertEquals(e, c2.get(i++));
    }

    /**
     * @param c Collection.
     * @return String.
     */
    @SuppressWarnings("unchecked")
    protected static String string(Iterable<? extends Cache.Entry> c) {
        return "[" +
            F.fold(
                c,
                "",
                new C2<Cache.Entry, String, String>() {
                    @Override public String apply(Cache.Entry e, String b) {
                        return b.isEmpty() ? e.getKey().toString() : b + ", " + e.getKey();
                    }
                }) +
            "]]";
    }

    /** @throws Exception If failed. */
    public void testMaxSizePartitionedNearDisabled() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;
        plcMax = 10;
        syncCommit = true;

        gridCnt = 2;

        checkPartitioned();
    }

    /** @throws Exception If failed. */
    public void testMaxSizePartitionedNearDisabledWithBatch() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;
        plcMax = 10;
        plcBatchSize = 2;
        syncCommit = true;

        gridCnt = 2;

        checkPartitioned();
    }

    /** @throws Exception If failed. */
    public void testMaxMemSizePartitionedNearDisabled() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;
        plcMax = 0;
        plcMaxMemSize = 100;
        syncCommit = true;

        gridCnt = 2;

        checkPartitioned();
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearEnabled() throws Exception {
        mode = PARTITIONED;
        nearEnabled = true;
        nearMax = 3;
        plcMax = 10;
        evictNearSync = true;
        syncCommit = true;

        gridCnt = 2;

        checkPartitioned(); // Near size is 0 because of backups present.
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearDisabledMultiThreaded() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;
        plcMax = 100;
        evictSync = false;

        gridCnt = 2;

        checkPartitionedMultiThreaded();
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearDisabledBackupSyncMultiThreaded() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;
        plcMax = 100;
        evictSync = true;

        gridCnt = 2;

        checkPartitionedMultiThreaded();
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearEnabledMultiThreaded() throws Exception {
        mode = PARTITIONED;
        nearEnabled = true;
        plcMax = 10;
        evictSync = false;

        gridCnt = 2;

        checkPartitionedMultiThreaded();
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearEnabledBackupSyncMultiThreaded() throws Exception {
        mode = PARTITIONED;
        nearEnabled = true;
        plcMax = 10;
        evictSync = true;

        gridCnt = 2;

        checkPartitionedMultiThreaded();
    }

    /**
     * @throws Exception If failed.
     */
    protected void checkPartitioned() throws Exception {

        int endSize = nearEnabled ? 0 : plcMax;

        int endPlcSize = nearEnabled ? 0 : plcMax;

        startGridsMultiThreaded(gridCnt);

        try {
            Random rand = new Random();

            int cnt = 500;

            for (int i = 0; i < cnt; i++) {
                IgniteCache<Integer, String> cache = grid(rand.nextInt(2)).cache(null);

                int key = rand.nextInt(100);
                String val = Integer.toString(key);

                cache.put(key, val);

                if (i % 100 == 0)
                    info("Stored cache object for key [key=" + key + ", idx=" + i + ']');
            }

            if (nearEnabled) {
                for (int i = 0; i < gridCnt; i++)
                    assertEquals(endSize, near(i).nearSize());

                if (endPlcSize >= 0)
                    checkNearPolicies(endPlcSize);
            }
            else {
                if (plcMaxMemSize > 0) {
                    for (int i = 0; i < gridCnt; i++) {
                        GridDhtColocatedCache<Object, Object> cache = colocated(i);

                        int memSize = 0;

                        for (Cache.Entry<Object, Object> entry : cache.entrySet())
                            memSize += entry.unwrap(EvictableEntry.class).size();

                        EvictionPolicyProxy plc = proxy(policy(i));

                        assertTrue(plc.getCurrentMemorySize() <= memSize);
                    }
                }

                if (plcMax > 0) {
                    for (int i = 0; i < gridCnt; i++) {
                        int actual = colocated(i).size();

                        assertTrue("Cache size is greater then policy size [expected=" + endSize + ", actual=" + actual + ']',
                            actual <= endSize + (plcMaxMemSize > 0 ? 1 : plcBatchSize));
                    }
                }

                checkPolicies();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected void checkPartitionedMultiThreaded() throws Exception {
        try {
            startGridsMultiThreaded(gridCnt);

            final Random rand = new Random();

            final AtomicInteger cntr = new AtomicInteger();

            multithreaded(new Callable() {
                @Nullable @Override public Object call() throws Exception {
                    int cnt = 100;

                    for (int i = 0; i < cnt && !Thread.currentThread().isInterrupted(); i++) {
                        IgniteEx grid = grid(rand.nextInt(2));

                        IgniteCache<Integer, String> cache = grid.cache(null);

                        int key = rand.nextInt(1000);
                        String val = Integer.toString(key);

                        try (Transaction tx = grid.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            String v = cache.get(key);

                            assert v == null || v.equals(Integer.toString(key)) : "Invalid value for key [key=" + key +
                                ", val=" + v + ']';

                            cache.put(key, val);

                            tx.commit();
                        }

                        if (cntr.incrementAndGet() % 100 == 0)
                            info("Stored cache object for key [key=" + key + ", idx=" + i + ']');
                    }

                    return null;
                }
            }, 10);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param plcMax Policy max.
     * @return Policy.
     */
    protected abstract T createPolicy(int plcMax);

    /**
     * @param nearMax Near max.
     * @return Policy.
     */
    protected abstract T createNearPolicy(int nearMax);

    /**
     * Performs after-test near policy check.
     *
     * @param nearMax Near max.
     */
    protected void checkNearPolicies(int nearMax) {
        for (int i = 0; i < gridCnt; i++) {

            EvictionPolicyProxy proxy = proxy(nearPolicy(i));

            for (EvictableEntry e : proxy.queue())
                assert !e.isCached() : "Invalid near policy size: " + proxy.queue();
        }
    }

    /**
     * Performs after-test policy check.
     */
    protected void checkPolicies() {
        for (int i = 0; i < gridCnt; i++) {
            if (plcMaxMemSize > 0) {
                int size = 0;

                for (EvictableEntry entry : proxy(policy(i)).queue())
                    size += entry.size();

                assertEquals(size, proxy(policy(i)).getCurrentMemorySize());
            }
            else
                assertTrue(proxy(policy(i)).queue().size() <= plcMax + plcBatchSize);
        }
    }

    /**
     *
     */
    @SuppressWarnings({"PublicConstructorInNonPublicClass"})
    protected static class MockEntry extends GridCacheMockEntry<String, String> {
        /** Key size. */
        public static final int KEY_SIZE = 1;

        /** Value size. */
        public static final int VALUE_SIZE = 1;

        /** Entry size. */
        public static final int ENTRY_SIZE = KEY_SIZE + VALUE_SIZE;

        /** */
        private IgniteCache<String, String> parent;

        /** Entry value. */
        private String val;

        /** @param key Key. */
        public MockEntry(String key) {
            super(key);
        }

        /**
         * @param key Key.
         * @param val Value.
         */
        public MockEntry(String key, String val) {
            super(key);

            this.val = val;
        }

        /**
         * @param key Key.
         * @param parent Parent.
         */
        public MockEntry(String key, @Nullable IgniteCache<String, String> parent) {
            super(key);

            this.parent = parent;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public <T> T unwrap(Class<T> clazz) {
            if (clazz.isAssignableFrom(IgniteCache.class))
                return (T)parent;

            return super.unwrap(clazz);
        }

        /** {@inheritDoc} */
        @Override public String getValue() throws IllegalStateException {
            return val;
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return val == null ? KEY_SIZE : ENTRY_SIZE;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MockEntry.class, this, super.toString());
        }
    }

    /**
     * Rvicition policy proxy.
     */
    public static class EvictionPolicyProxy implements EvictionPolicy {
        /** Policy. */
        private final EvictionPolicy plc;

        /**
         * @param plc Policy.
         */
        private EvictionPolicyProxy(EvictionPolicy plc) {
            this.plc = plc;
        }

        /**
         * @param plc Policy.
         */
        public static EvictionPolicyProxy proxy(EvictionPolicy plc) {
            return new EvictionPolicyProxy(plc);
        }

        /**
         * Get current size.
         */
        public int getCurrentSize() {
            try {
                return (Integer)plc.getClass().getDeclaredMethod("getCurrentSize").invoke(plc);
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Current memory size.
         */
        public long getCurrentMemorySize() {
            try {
                return (Long)plc.getClass().getDeclaredMethod("getCurrentMemorySize").invoke(plc);
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Current queue.
         */
        public Collection<EvictableEntry> queue() {
            try {
                return (Collection<EvictableEntry>)plc.getClass().getDeclaredMethod("queue").invoke(plc);
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * @param rmv Remove.
         * @param entry Entry.
         */
        @Override public void onEntryAccessed(boolean rmv, EvictableEntry entry) {
            try {
                plc.getClass()
                    .getDeclaredMethod("onEntryAccessed", boolean.class, EvictableEntry.class)
                    .invoke(plc, rmv, entry);
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}