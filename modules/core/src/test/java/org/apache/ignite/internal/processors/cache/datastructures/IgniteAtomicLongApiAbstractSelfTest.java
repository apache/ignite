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

import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Cache atomic long api test.
 */
public abstract class IgniteAtomicLongApiAbstractSelfTest extends IgniteAtomicsAbstractTest {
    /** Random number generator. */
    private static final Random RND = new Random();

    /** */
    private static final String TRANSACTIONAL_CACHE_NAME = "tx_cache";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(TRANSACTIONAL_CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateRemove() throws Exception {
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        String atomicName1 = "FIRST";

        String atomicName2 = "SECOND";

        IgniteAtomicLong atomic1 = ignite.atomicLong(atomicName1, 0, true);
        IgniteAtomicLong atomic2 = ignite.atomicLong(atomicName2, 0, true);
        IgniteAtomicLong atomic3 = ignite.atomicLong(atomicName1, 0, true);

        assertNotNull(atomic1);
        assertNotNull(atomic2);
        assertNotNull(atomic3);

        assert atomic1.equals(atomic3);
        assert atomic3.equals(atomic1);
        assert !atomic3.equals(atomic2);

        atomic1.close();
        atomic2.close();
        atomic3.close();

        assertNull(ignite.atomicLong(atomicName1, 0, false));
        assertNull(ignite.atomicLong(atomicName2, 0, false));

        try {
            atomic1.get();

            fail();
        }
        catch (IllegalStateException | IgniteException e) {
            info("Caught expected exception: " + e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementAndGet() throws Exception {
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

        long curAtomicVal = atomic.get();

        assert curAtomicVal + 1 == atomic.incrementAndGet();
        assert curAtomicVal + 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndIncrement() throws Exception {
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndIncrement();
        assert curAtomicVal + 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecrementAndGet() throws Exception {
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

        long curAtomicVal = atomic.get();

        assert curAtomicVal - 1 == atomic.decrementAndGet();
        assert curAtomicVal - 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndDecrement() throws Exception {
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndDecrement();
        assert curAtomicVal - 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndAdd() throws Exception {
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

        long delta = RND.nextLong();

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndAdd(delta);
        assert curAtomicVal + delta == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddAndGet() throws Exception {
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

        long delta = RND.nextLong();

        long curAtomicVal = atomic.get();

        assert curAtomicVal + delta == atomic.addAndGet(delta);
        assert curAtomicVal + delta == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndSet() throws Exception {
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

        long newVal = RND.nextLong();

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndSet(newVal);
        assert newVal == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"NullableProblems", "ConstantConditions"})
    public void testCompareAndSet() throws Exception {
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

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
        info("Running test [name=" + getName() + ", cacheMode=" + atomicsCacheMode() + ']');

        Ignite ignite = grid(0);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

        IgniteCache<Object, Object> cache = ignite.cache(TRANSACTIONAL_CACHE_NAME);

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(1, 1);

            long newVal = RND.nextLong();

            long curAtomicVal = atomic.get();

            assert curAtomicVal == atomic.getAndSet(newVal);
            assert newVal == atomic.get();
        }
    }

    /**
     * Implementation of ignite data structures internally uses special system caches, need make sure that
     * transaction on these system caches do not intersect with transactions started by user.
     *
     * @throws Exception If failed.
     */
    public void testIsolation() throws Exception {
        Ignite ignite = grid(0);

        IgniteCache<Object, Object> cache = ignite.cache(TRANSACTIONAL_CACHE_NAME);

        IgniteAtomicLong atomic = ignite.atomicLong("atomic", 0, true);

        long curAtomicVal = atomic.get();

        try (Transaction tx = ignite.transactions().txStart()) {
            atomic.getAndIncrement();

            cache.put(1, 1);

            tx.rollback();
        }

        assertEquals(0, cache.size());
        assertEquals(curAtomicVal + 1, atomic.get());
    }
}