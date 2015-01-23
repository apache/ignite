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

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Base class for eviction tests.
 */
public abstract class GridCacheEvictionAbstractTest<T extends CacheEvictionPolicy<?, ?>>
    extends GridCommonAbstractTest {
    /** IP finder. */
    protected static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

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

    /** Near policy max. */
    protected int nearMax = 3;

    /** Synchronous commit. */
    protected boolean syncCommit;

    /** */
    protected int gridCnt = 2;

    /** */
    protected CacheEvictionFilter<?, ?> filter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);
        cc.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
        cc.setEvictionPolicy(createPolicy(plcMax));
        cc.setNearEvictionPolicy(createNearPolicy(nearMax));
        cc.setEvictSynchronized(evictSync);
        cc.setEvictNearSynchronized(evictNearSync);
        cc.setSwapEnabled(false);
        cc.setWriteSynchronizationMode(syncCommit ? FULL_SYNC : FULL_ASYNC);
        cc.setStartSize(plcMax);
        cc.setAtomicityMode(TRANSACTIONAL);

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
    protected void info(String prefix, CacheEvictionPolicy<?, ?> p) {
        info(prefix + ": " + p.toString());
    }

    /** @param p Policy. */
    protected void info(CacheEvictionPolicy<?, ?> p) {
        info(p.toString());
    }

    /**
     * @param c1 Policy collection.
     * @param c2 Expected list.
     */
    protected void check(Collection<CacheEntry<String, String>> c1, MockEntry... c2) {
        check(c1, F.asList(c2));
    }

    /** @return Policy. */
    @SuppressWarnings({"unchecked"})
    protected T policy() {
        return (T)grid().cache(null).configuration().getEvictionPolicy();
    }

    /**
     * @param i Grid index.
     * @return Policy.
     */
    @SuppressWarnings({"unchecked"})
    protected T policy(int i) {
        return (T)grid(i).cache(null).configuration().getEvictionPolicy();
    }

    /**
     * @param i Grid index.
     * @return Policy.
     */
    @SuppressWarnings({"unchecked"})
    protected T nearPolicy(int i) {
        return (T)grid(i).cache(null).configuration().getNearEvictionPolicy();
    }

    /**
     * @param c1 Policy collection.
     * @param c2 Expected list.
     */
    protected void check(Collection<CacheEntry<String, String>> c1, List<MockEntry> c2) {
        assert c1.size() == c2.size() : "Mismatch [actual=" + string(c1) + ", expected=" + string(c2) + ']';

        assert c1.containsAll(c2) : "Mismatch [actual=" + string(c1) + ", expected=" + string(c2) + ']';

        int i = 0;

        // Check order.
        for (CacheEntry<String, String> e : c1)
            assertEquals(e, c2.get(i++));
    }

    /**
     * @param c Collection.
     * @return String.
     */
    protected String string(Iterable<? extends CacheEntry> c) {
        return "[" + F.fold(c, "", new C2<CacheEntry, String, String>() {
            @Override public String apply(CacheEntry e, String b) {
                return b.isEmpty() ? e.getKey().toString() : b + ", " + e.getKey();
            }
        }) + "]]";
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearDisabled() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;
        plcMax = 10;
        syncCommit = true;

        gridCnt = 2;

        checkPartitioned(plcMax, plcMax, false);
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

        checkPartitioned(0, 0, true); // Near size is 0 because of backups present.
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearDisabledMultiThreaded() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;
        plcMax = 100;
        evictSync = false;

        gridCnt = 2;

        checkPartitionedMultiThreaded(gridCnt);
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearDisabledBackupSyncMultiThreaded() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;
        plcMax = 100;
        evictSync = true;

        gridCnt = 2;

        checkPartitionedMultiThreaded(gridCnt);
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearEnabledMultiThreaded() throws Exception {
        mode = PARTITIONED;
        nearEnabled = true;
        plcMax = 10;
        evictSync = false;

        gridCnt = 2;

        checkPartitionedMultiThreaded(gridCnt);
    }

    /** @throws Exception If failed. */
    public void testPartitionedNearEnabledBackupSyncMultiThreaded() throws Exception {
        mode = PARTITIONED;
        nearEnabled = true;
        plcMax = 10;
        evictSync = true;

        gridCnt = 2;

        checkPartitionedMultiThreaded(gridCnt);
    }

    /**
     * @param endSize Final near size.
     * @param endPlcSize Final near policy size.
     * @throws Exception If failed.
     */
    private void checkPartitioned(int endSize, int endPlcSize, boolean near) throws Exception {
        startGridsMultiThreaded(gridCnt);

        try {
            Random rand = new Random();

            int cnt = 500;

            for (int i = 0; i < cnt; i++) {
                Cache<Integer, String> cache = grid(rand.nextInt(2)).cache(null);

                int key = rand.nextInt(100);
                String val = Integer.toString(key);

                cache.put(key, val);

                if (i % 100 == 0)
                    info("Stored cache object for key [key=" + key + ", idx=" + i + ']');
            }

            if (near) {
                for (int i = 0; i < gridCnt; i++)
                    assertEquals(endSize, near(i).nearSize());

                if (endPlcSize >= 0)
                    checkNearPolicies(endPlcSize);
            }
            else {
                for (int i = 0; i < gridCnt; i++) {
                    int actual = colocated(i).size();

                    assertTrue("Cache size is greater then policy size [expected=" + endSize + ", actual=" + actual + ']',
                        actual <= endSize);
                }

                checkPolicies(endPlcSize);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param gridCnt Grid count.
     * @throws Exception If failed.
     */
    protected void checkPartitionedMultiThreaded(int gridCnt) throws Exception {
        try {
            startGridsMultiThreaded(gridCnt);

            final Random rand = new Random();

            final AtomicInteger cntr = new AtomicInteger();

            multithreaded(new Callable() {
                @Nullable @Override public Object call() throws Exception {
                    int cnt = 100;

                    for (int i = 0; i < cnt && !Thread.currentThread().isInterrupted(); i++) {
                        Cache<Integer, String> cache = grid(rand.nextInt(2)).cache(null);

                        int key = rand.nextInt(1000);
                        String val = Integer.toString(key);

                        try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
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
    protected abstract void checkNearPolicies(int nearMax);

    /**
     * Performs after-test policy check.
     *
     * @param plcMax Maximum allowed size of ploicy.
     */
    protected abstract void checkPolicies(int plcMax);

    /**
     *
     */
    @SuppressWarnings({"PublicConstructorInNonPublicClass"})
    protected static class MockEntry extends GridCacheMockEntry<String, String> {
        /** */
        private final CacheProjection<String, String> parent;

        /** Entry value. */
        private String val;

        /** @param key Key. */
        public MockEntry(String key) {
            super(key);

            parent = null;
        }

        /**
         * @param key Key.
         * @param val Value.
         */
        public MockEntry(String key, String val) {
            super(key);

            this.val = val;
            parent = null;
        }

        /**
         * @param key Key.
         * @param parent Parent.
         */
        public MockEntry(String key, @Nullable CacheProjection<String, String> parent) {
            super(key);

            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public String getValue() throws IllegalStateException {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String setValue(String val) {
            String old = this.val;

            this.val = val;

            return old;
        }

        /** {@inheritDoc} */
        @Override public CacheProjection<String, String> projection() {
            return parent;
        }
    }
}
