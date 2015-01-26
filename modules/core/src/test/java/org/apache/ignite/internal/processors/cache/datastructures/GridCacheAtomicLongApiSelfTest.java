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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CachePreloadMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Cache atomic long api test.
 */
public class GridCacheAtomicLongApiSelfTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RND = new Random();

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        CacheConfiguration repCacheCfg = defaultCacheConfiguration();

        repCacheCfg.setName("replicated");
        repCacheCfg.setCacheMode(REPLICATED);
        repCacheCfg.setPreloadMode(SYNC);
        repCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        repCacheCfg.setEvictionPolicy(null);
        repCacheCfg.setAtomicityMode(TRANSACTIONAL);
        repCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        CacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setName("partitioned");
        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setBackups(1);
        partCacheCfg.setPreloadMode(SYNC);
        partCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        partCacheCfg.setEvictionPolicy(null);
        partCacheCfg.setNearEvictionPolicy(null);
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);
        partCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        CacheConfiguration locCacheCfg = defaultCacheConfiguration();

        locCacheCfg.setName("local");
        locCacheCfg.setCacheMode(LOCAL);
        locCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        locCacheCfg.setEvictionPolicy(null);
        locCacheCfg.setAtomicityMode(TRANSACTIONAL);
        locCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(repCacheCfg, partCacheCfg, locCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateRemove() throws Exception {
        createRemove("local");

        createRemove("replicated");

        createRemove("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void createRemove(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        String atomicName1 = "FIRST";

        String atomicName2 = "SECOND";

        IgniteAtomicLong atomic1 = cache.dataStructures().atomicLong(atomicName1, 0, true);
        IgniteAtomicLong atomic2 = cache.dataStructures().atomicLong(atomicName2, 0, true);
        IgniteAtomicLong atomic3 = cache.dataStructures().atomicLong(atomicName1, 0, true);

        assertNotNull(atomic1);
        assertNotNull(atomic2);
        assertNotNull(atomic3);

        assert atomic1.equals(atomic3);
        assert atomic3.equals(atomic1);
        assert !atomic3.equals(atomic2);

        assertEquals(0, cache.primarySize());

        assert cache.dataStructures().removeAtomicLong(atomicName1);
        assert cache.dataStructures().removeAtomicLong(atomicName2);
        assert !cache.dataStructures().removeAtomicLong(atomicName1);
        assert !cache.dataStructures().removeAtomicLong(atomicName2);

        assertEquals(0, cache.primarySize());

        try {
            atomic1.get();

            fail();
        }
        catch (IgniteCheckedException e) {
            info("Caught expected exception: " + e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementAndGet() throws Exception {
        incrementAndGet("local");

        incrementAndGet("replicated");

        incrementAndGet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void incrementAndGet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        IgniteAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long curAtomicVal = atomic.get();

        assert curAtomicVal + 1 == atomic.incrementAndGet();
        assert curAtomicVal + 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndIncrement() throws Exception {
        getAndIncrement("local");

        getAndIncrement("replicated");

        getAndIncrement("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndIncrement(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        IgniteAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndIncrement();
        assert curAtomicVal + 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecrementAndGet() throws Exception {
        decrementAndGet("local");

        decrementAndGet("replicated");

        decrementAndGet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void decrementAndGet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        IgniteAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long curAtomicVal = atomic.get();

        assert curAtomicVal - 1 == atomic.decrementAndGet();
        assert curAtomicVal - 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndDecrement() throws Exception {
        getAndDecrement("local");

        getAndDecrement("replicated");

        getAndDecrement("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndDecrement(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        IgniteAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndDecrement();
        assert curAtomicVal - 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndAdd() throws Exception {
        getAndAdd("local");

        getAndAdd("replicated");

        getAndAdd("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndAdd(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        IgniteAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long delta = RND.nextLong();

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndAdd(delta);
        assert curAtomicVal + delta == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddAndGet() throws Exception {
        addAndGet("local");

        addAndGet("replicated");

        addAndGet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void addAndGet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        IgniteAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long delta = RND.nextLong();

        long curAtomicVal = atomic.get();

        assert curAtomicVal + delta == atomic.addAndGet(delta);
        assert curAtomicVal + delta == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndSet() throws Exception {
        getAndSet("local");

        getAndSet("replicated");

        getAndSet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndSet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        IgniteAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long newVal = RND.nextLong();

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndSet(newVal);
        assert newVal == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompareAndSet() throws Exception {
        compareAndSet("local");

        compareAndSet("replicated");

        compareAndSet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"NullableProblems", "ConstantConditions"})
    private void compareAndSet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        IgniteAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long newVal = RND.nextLong();

        final long oldVal = atomic.get();

        // Don't set new value.
        assert !atomic.compareAndSet(oldVal - 1, newVal);

        assert oldVal == atomic.get();

        // Set new value.
        assert atomic.compareAndSet(oldVal, newVal);

        assert newVal == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndSetInTx() throws Exception {
        getAndSetInTx("local");

        getAndSetInTx("replicated");

        getAndSetInTx("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndSetInTx(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        IgniteAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        try (IgniteTx tx = cache.txStart()) {
            long newVal = RND.nextLong();

            long curAtomicVal = atomic.get();

            assert curAtomicVal == atomic.getAndSet(newVal);
            assert newVal == atomic.get();
        }
    }
}
