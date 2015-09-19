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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests {@link CacheInterceptor}.
 */
public abstract class GridCacheInterceptorAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static Interceptor interceptor;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        interceptor = new Interceptor();

        super.beforeTestsStarted();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        interceptor.reset();

        interceptor.disabled = true;

        super.afterTest();

        interceptor.disabled = false;

        assertEquals(0, interceptor.invokeCnt.get());

        atomicClockModeDelay(jcache(0));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(spi);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        assertNotNull(interceptor);

        ccfg.setInterceptor(interceptor);

        if (ccfg.getAtomicityMode() == ATOMIC) {
            assertNotNull(writeOrderMode());

            ccfg.setAtomicWriteOrderMode(writeOrderMode());
        }

        if (!storeEnabled()) {
            ccfg.setCacheStoreFactory(null);
            ccfg.setReadThrough(false);
            ccfg.setWriteThrough(false);
        }

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Atomic cache write order mode.
     */
    @Nullable protected CacheAtomicWriteOrderMode writeOrderMode() {
        return null;
    }

    /**
     * @return {@code True} if cache store is enabled.
     */
    protected boolean storeEnabled() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        testGet(primaryKey(0));

        afterTest();

        if (cacheMode() != LOCAL)
            testGet(backupKey(0));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testGet(String key) throws Exception {
        // Try when value is not in cache.

        interceptor.retInterceptor = new NullGetInterceptor();

        log.info("Get 1.");

        IgniteCache<String, Integer> cache = jcache(0);

        assertEquals(null, cache.get(key));

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(0, interceptor.getMap.size());

        interceptor.reset();

        interceptor.retInterceptor = new OneGetInterceptor();

        log.info("Get 2.");

        assertEquals((Integer)1, cache.get(key));

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(0, interceptor.getMap.size());

        interceptor.reset();

        // Disable interceptor and update cache.

        interceptor.disabled = true;

        cache.put(key, 100);

        interceptor.disabled = false;

        // Try when value is in cache.

        interceptor.retInterceptor = new NullGetInterceptor();

        log.info("Get 3.");

        assertEquals(null, cache.get(key));

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(1, interceptor.getMap.size());

        assertEquals(100, interceptor.getMap.get(key));

        checkCacheValue(key, 100);

        interceptor.reset();

        interceptor.retInterceptor = new GetIncrementInterceptor();

        log.info("Get 4.");

        assertEquals((Integer)101, cache.get(key));

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(1, interceptor.getMap.size());

        assertEquals(100, interceptor.getMap.get(key));

        checkCacheValue(key, 100);

        interceptor.reset();

        interceptor.retInterceptor = new GetIncrementInterceptor();

        log.info("GetAsync 1.");

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cacheAsync.get(key);

        assertEquals((Integer)101, cacheAsync.<Integer>future().get());

        assertEquals(1, interceptor.invokeCnt.get());

        assertEquals(1, interceptor.getMap.size());

        assertEquals(100, interceptor.getMap.get(key));

        checkCacheValue(key, 100);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        Set<String> keys = new LinkedHashSet<>();

        for (int i = 0; i < 1000; i++)
            keys.add(String.valueOf(i));

        interceptor.retInterceptor = new NullGetInterceptor();

        IgniteCache<String, Integer> cache = jcache(0);

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        Map<String, Integer> map = cache.getAll(keys);

        for (String key : keys)
            assertEquals(null, map.get(key));

        assertEquals(1000, interceptor.invokeCnt.get());

        interceptor.reset();

        interceptor.retInterceptor = new GetAllInterceptor1();

        map = cache.getAll(keys);

        for (String key : keys) {
            int k = Integer.valueOf(key);

            if (k % 2 == 0)
                assertEquals(null, map.get(key));
            else
                assertEquals((Integer)(k * 2), map.get(key));
        }

        assertEquals(1000, interceptor.invokeCnt.get());

        // Put some values in cache.

        interceptor.disabled = true;

        for (int i = 0; i < 500; i++)
            cache.put(String.valueOf(i), i);

        interceptor.disabled = false;

        for (int j = 0; j < 2; j++) {
            interceptor.reset();

            interceptor.retInterceptor = new GetAllInterceptor2();

            if (j == 0)
                map = cache.getAll(keys);
            else {
                cacheAsync.getAll(keys);

                map = cacheAsync.<Map<String, Integer>>future().get();
            }

            int i = 0;

            for (String key : keys) {
                switch (i % 3) {
                    case 0:
                        assertEquals(null, map.get(key));

                        break;

                    case 1:
                        Integer exp = i < 500 ? i : null;

                        assertEquals(exp, map.get(key));

                        break;

                    case 2:
                        assertEquals((Integer)(i * 3), map.get(key));

                        break;

                    default:
                        fail();
                }

                i++;
            }

            assertEquals(1000, interceptor.invokeCnt.get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelUpdate() throws Exception {
        for (Operation op : Operation.values()) {
            testCancelUpdate(primaryKey(0), op);

            afterTest();

            if (cacheMode() != LOCAL) {
                testCancelUpdate(backupKey(0), op);

                afterTest();
            }
        }
    }

    /**
     * @param op Operation type.
     * @return {@code True} if this is atomic cache and update is first run on primary node.
     */
    private int expectedIgnoreInvokeCount(Operation op) {
        int dataNodes = cacheMode() == REPLICATED ? gridCount() : 2;

        if (atomicityMode() == TRANSACTIONAL)
            return dataNodes + (storeEnabled() ? 1 : 0); // One call before store is updated.
        else {
            // If update goes through primary node and it is cancelled then backups aren't updated.
            return (writeOrderMode() == PRIMARY || op == Operation.TRANSFORM) ? 1 : dataNodes;
        }
    }

    /**
     * @param op Operation type.
     * @return {@code True} if this is atomic cache and update is first run on primary node.
     */
    private int expectedInvokeCount(Operation op) {
        int dataNodes = cacheMode() == REPLICATED ? gridCount() : 2;

        if (atomicityMode() == TRANSACTIONAL)
            // Update + after update + one call before store is updated.
            return dataNodes * 2 + (storeEnabled() ? 1 : 0);
        else
            return (writeOrderMode() == PRIMARY || op == Operation.TRANSFORM) ? 2 : dataNodes * 2;
    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    private void testCancelUpdate(String key, Operation op) throws Exception {
        // Interceptor returns null to disabled update.
        CacheInterceptor retInterceptor = new NullPutInterceptor();

        interceptor.retInterceptor = retInterceptor;

        // Execute update when value is null, it should not change cache value.

        log.info("Update 1 " + op);

        update(0, op, key, 1, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        IgniteBiTuple t = interceptor.beforePutMap.get(key);

        assertEquals(null, t.get1());
        assertEquals(1, t.get2());

        // Disable interceptor and update cache.

        interceptor.reset();

        interceptor.disabled = true;

        clearCaches();

        jcache(0).put(key, 1);

        checkCacheValue(key, 1);

        // Execute update when value is not null, it should not change cache value.

        interceptor.disabled = false;
        interceptor.retInterceptor = retInterceptor;

        log.info("Update 2 " + op);

        update(0, op, key, 2, 1);

        checkCacheValue(key, 1);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        t = interceptor.beforePutMap.get(key);

        assertEquals(1, t.get1());
        assertEquals(2, t.get2());
    }

    /**
     * @throws Exception If failed.
     */
    public void testModifyUpdate() throws Exception {
        for (Operation op : Operation.values()) {
            testModifyUpdate(primaryKey(0), op);

            afterTest();

            if (cacheMode() != LOCAL) {
                testModifyUpdate(backupKey(0), op);

                afterTest();
            }
        }
    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    private void testModifyUpdate(String key, Operation op) throws Exception {
        // Interceptor returns incremented new value.
        CacheInterceptor retInterceptor = new PutIncrementInterceptor();

        // Execute update when value is null.

        interceptor.retInterceptor = retInterceptor;

        log.info("Update 1 " + op);

        update(0, op, key, 1, null);

        checkCacheValue(key, 2);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        IgniteBiTuple t = interceptor.beforePutMap.get(key);

        assertEquals(null, t.get1());
        assertEquals(1, t.get2());

        assertEquals(1, interceptor.afterPutMap.size());

        assertEquals(2, interceptor.afterPutMap.get(key));

        // Execute update when value is not null.

        interceptor.reset();

        interceptor.retInterceptor = retInterceptor;

        log.info("Update 2 " + op);

        update(0, op, key, 3, 2);

        checkCacheValue(key, 4);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforePutMap.size());

        t = interceptor.beforePutMap.get(key);

        assertEquals(2, t.get1());
        assertEquals(3, t.get2());

        assertEquals(1, interceptor.afterPutMap.size());

        assertEquals(4, interceptor.afterPutMap.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelRemove() throws Exception {
        for (Operation op : Operation.values()) {
            testCancelRemove(primaryKey(0), op);

            afterTest();

            if (cacheMode() != LOCAL) {
                testCancelRemove(backupKey(0), op);

                afterTest();
            }
        }
    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void testCancelRemove(String key, Operation op) throws Exception {
        // Interceptor disables remove and returns null.
        interceptor.retInterceptor = new BeforeRemoveInterceptor(new IgniteBiTuple(true, null));

        // Execute remove when value is null.

        log.info("Remove 1 " + op);

        remove(0, op, key, null, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRmvMap.size());

        assertEquals(null, interceptor.beforeRmvMap.get(key));

        log.info("Remove 2 " + op);

        interceptor.reset();

        // Interceptor disables remove and changes return value.
        interceptor.retInterceptor = new BeforeRemoveInterceptor(new IgniteBiTuple(true, 900));

        // Execute remove when value is null, interceptor changes return value.

        remove(0, op, key, null, 900);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRmvMap.size());

        assertEquals(null, interceptor.beforeRmvMap.get(key));

        // Disable interceptor and update cache.

        interceptor.reset();

        interceptor.disabled = true;

        clearCaches();

        jcache(0).put(key, 1);

        checkCacheValue(key, 1);

        // Execute remove when value is not null, it should not change cache value.

        interceptor.reset();

        interceptor.disabled = false;

        // Interceptor disables remove and returns null.
        interceptor.retInterceptor = new BeforeRemoveInterceptor(new IgniteBiTuple(true, null));

        log.info("Remove 3 " + op);

        remove(0, op, key, 1, null);

        checkCacheValue(key, 1);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRmvMap.size());

        assertEquals(1, interceptor.beforeRmvMap.get(key));

        interceptor.reset();

        // Interceptor disables remove and changes return value.
        interceptor.retInterceptor = new BeforeRemoveInterceptor(new IgniteBiTuple(true, 1000));

        log.info("Remove 4 " + op);

        remove(0, op, key, 1, 1000);

        checkCacheValue(key, 1);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRmvMap.size());

        assertEquals(1, interceptor.beforeRmvMap.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        for (Operation op : Operation.values()) {
            testRemove(primaryKey(0), op);

            afterTest();

            if (cacheMode() != LOCAL) {
                testRemove(backupKey(0), op);

                afterTest();
            }
        }
    }

    /**
     * @param key Key.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void testRemove(String key, Operation op) throws Exception {
        // Interceptor changes return value to null.
        interceptor.retInterceptor = new BeforeRemoveInterceptor( new IgniteBiTuple(false, null));

        // Execute remove when value is null.

        log.info("Remove 1 " + op);

        remove(0, op, key, null, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRmvMap.size());

        assertEquals(0, interceptor.afterRmvMap.size());

        log.info("Remove 2 " + op);

        interceptor.reset();

        // Interceptor changes return value.
        interceptor.retInterceptor = new BeforeRemoveInterceptor(new IgniteBiTuple(false, 900));

        // Execute remove when value is null.

        remove(0, op, key, null, 900);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(0, interceptor.beforeRmvMap.size());

        assertEquals(0, interceptor.afterRmvMap.size());

        // Disable interceptor and update cache.

        interceptor.reset();

        interceptor.disabled = true;

        clearCaches();

        jcache(0).put(key, 1);

        checkCacheValue(key, 1);

        // Execute remove when value is not null.

        interceptor.reset();

        interceptor.disabled = false;

        // Interceptor changes return value to null.
        interceptor.retInterceptor = new BeforeRemoveInterceptor(new IgniteBiTuple(false, null));

        log.info("Remove 3 " + op);

        remove(0, op, key, 1, null);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRmvMap.size());

        assertEquals(1, interceptor.beforeRmvMap.get(key));

        assertEquals(1, interceptor.afterRmvMap.size());

        assertEquals(1, interceptor.afterRmvMap.get(key));

        // Disable interceptor and update cache.

        interceptor.disabled = true;

        clearCaches();

        jcache(0).put(key, 2);

        checkCacheValue(key, 2);

        // Execute remove when value is not null.

        interceptor.reset();

        interceptor.disabled = false;

        // Interceptor changes return value.
        interceptor.retInterceptor = new BeforeRemoveInterceptor(new IgniteBiTuple(false, 1000));

        log.info("Remove 4 " + op);

        remove(0, op, key, 2, 1000);

        checkCacheValue(key, null);

        // Check values passed to interceptor.

        assertEquals(1, interceptor.beforeRmvMap.size());

        assertEquals(2, interceptor.beforeRmvMap.get(key));

        assertEquals(1, interceptor.afterRmvMap.size());

        assertEquals(2, interceptor.afterRmvMap.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearNodeKey() throws Exception {
        if (cacheMode() != PARTITIONED)
            return;

        if (atomicityMode() == TRANSACTIONAL) {
            for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
                for (TransactionIsolation txIsolation : TransactionIsolation.values()) {
                    for (Operation op : Operation.values()) {
                        testNearNodeKey(txConcurrency, txIsolation, op);

                        afterTest();
                    }
                }
            }
        }

        testNearNodeKey(null, null, null);
    }

    /**
     * @param txConcurrency Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @param op Operation type.
     * @throws Exception If failed.
     */
    private void testNearNodeKey(@Nullable TransactionConcurrency txConcurrency,
        @Nullable TransactionIsolation txIsolation, @Nullable Operation op) throws Exception {
        // Interceptor returns incremented new value.
        interceptor.retInterceptor = new PutIncrementInterceptor();

        String key1 = primaryKey(0);
        String key2 = backupKey(0);
        String key3 = nearKey(0);

        interceptor.disabled = true;

        // Put from grid 1 to be sure grid 0 does not have value for near key.
        jcache(1).putAll(F.asMap(key1, 1, key2, 2, key3, 3));

        atomicClockModeDelay(jcache(1));

        interceptor.disabled = false;

        log.info("Update [op=" + op + ", key1=" + key1 + ", key2=" + key2 + ", key3=" + key3 +
            ", txConcurrency=" + txConcurrency + ", txIsolation=" + txIsolation + ']');

        if (txConcurrency != null) {
            assertNotNull(txIsolation);
            assertNotNull(op);

            try (Transaction tx = ignite(0).transactions().txStart(txConcurrency, txIsolation)) {
                update(0, op, key1, 100, 1);
                update(0, op, key2, 200, 2);
                update(0, op, key3, 300, 3);

                tx.commit();
            }
        }
        else
            jcache(0).putAll(F.asMap(key1, 100, key2, 200, key3, 300));

        checkCacheValue(key1, 101);
        checkCacheValue(key2, 201);
        checkCacheValue(key3, 301);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBatchUpdate() throws Exception {
        testBatchUpdate(Operation.UPDATE);

        afterTest();

        testBatchUpdate(Operation.TRANSFORM);
    }

    /**
     * @param op Operation type.
     * @throws Exception If failed.
     */
    private void testBatchUpdate(Operation op) throws Exception {
        // Interceptor returns incremented new value.
        interceptor.retInterceptor = new PutIncrementInterceptor();

        Map<String, Integer> map = new HashMap<>();

        final String key1;
        String key2;
        String key3;

        if (cacheMode() == LOCAL) {
            key1 = "1";
            key2 = "2";
            key3 = "3";
        }
        else {
            List<String> keys = primaryKeys(0, 2);
            key1 = keys.get(0); // Need two keys for the same node to test atomic cache batch store upadte.
            key2 = keys.get(1);
            key3 = backupKey(0);
        }

        map.put(key1, 1);
        map.put(key2, 2);
        map.put(key3, 3);

        log.info("Batch update 1: " + op);

        batchUpdate(0, op, map);

        checkCacheValue(key1, 2);
        checkCacheValue(key2, 3);
        checkCacheValue(key3, 4);

        assertEquals(3, interceptor.beforePutMap.size());

        assertBeforePutValue(key1, null, 1);
        assertBeforePutValue(key2, null, 2);
        assertBeforePutValue(key3, null, 3);

        assertEquals(3, interceptor.afterPutMap.size());

        assertEquals(2, interceptor.afterPutMap.get(key1));
        assertEquals(3, interceptor.afterPutMap.get(key2));
        assertEquals(4, interceptor.afterPutMap.get(key3));

        interceptor.reset();

        // Interceptor returns incremented new value, cancels update for one key.
        interceptor.retInterceptor = new BatchPutInterceptor1(key1);

        map.put(key1, 100);
        map.put(key2, 200);
        map.put(key3, 300);

        log.info("Batch update 2: " + op);

        batchUpdate(0, op, map);

        checkCacheValue(key1, 2);
        checkCacheValue(key2, 201);
        checkCacheValue(key3, 301);

        assertEquals(3, interceptor.beforePutMap.size());

        assertBeforePutValue(key1, 2, 100);
        assertBeforePutValue(key2, 3, 200);
        assertBeforePutValue(key3, 4, 300);

        assertEquals(2, interceptor.afterPutMap.size());

        assertEquals(201, interceptor.afterPutMap.get(key2));
        assertEquals(301, interceptor.afterPutMap.get(key3));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBatchRemove() throws Exception {
        testBatchRemove(Operation.UPDATE);

        afterTest();

        testBatchRemove(Operation.TRANSFORM);
    }

    /**
     * @param op Operation type.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void testBatchRemove(Operation op) throws Exception {
        Map<String, Integer> map = new HashMap<>();

        final String key1;
        String key2;
        String key3;

        if (cacheMode() == LOCAL) {
            key1 = "1";
            key2 = "2";
            key3 = "3";
        }
        else {
            List<String> keys = primaryKeys(0, 2);
            key1 = keys.get(0);
            key2 = keys.get(1);
            key3 = backupKey(0);
        }

        map.put(key1, 1);
        map.put(key2, 2);
        map.put(key3, 3);

        // Interceptor does not cancel update.
        interceptor.retInterceptor = new BeforeRemoveInterceptor(new IgniteBiTuple(false, 999));

        log.info("Batch remove 1: " + op);

        batchRemove(0, op, map);

        checkCacheValue(key1, null);
        checkCacheValue(key2, null);
        checkCacheValue(key3, null);

        assertEquals(0, interceptor.beforeRmvMap.size());

        assertEquals(0, interceptor.afterRmvMap.size());

        // Disable interceptor and put some values in cache.

        interceptor.disabled = true;

        jcache(0).putAll(map);

        interceptor.disabled = false;

        interceptor.reset();

        // Interceptor does not cancel update.
        interceptor.retInterceptor = new BeforeRemoveInterceptor(new IgniteBiTuple(false, 999));

        log.info("Batch remove 2: " + op);

        batchRemove(0, op, map);

        checkCacheValue(key1, null);
        checkCacheValue(key2, null);
        checkCacheValue(key3, null);

        assertEquals(3, interceptor.beforeRmvMap.size());

        assertEquals(1, interceptor.beforeRmvMap.get(key1));
        assertEquals(2, interceptor.beforeRmvMap.get(key2));
        assertEquals(3, interceptor.beforeRmvMap.get(key3));

        assertEquals(3, interceptor.afterRmvMap.size());

        assertEquals(1, interceptor.afterRmvMap.get(key1));
        assertEquals(2, interceptor.afterRmvMap.get(key2));
        assertEquals(3, interceptor.afterRmvMap.get(key3));

        // Disable interceptor and put some values in cache.

        interceptor.disabled = true;

        jcache(0).putAll(map);

        interceptor.disabled = false;

        interceptor.reset();

        // Interceptor cancels update for one key.
        interceptor.retInterceptor = new BatchRemoveInterceptor(key1);

        log.info("Batch remove 3: " + op);

        batchRemove(0, op, map);

        checkCacheValue(key1, 1);
        checkCacheValue(key2, null);
        checkCacheValue(key3, null);

        assertEquals(3, interceptor.beforeRmvMap.size());

        assertEquals(1, interceptor.beforeRmvMap.get(key1));
        assertEquals(2, interceptor.beforeRmvMap.get(key2));
        assertEquals(3, interceptor.beforeRmvMap.get(key3));

        assertEquals(2, interceptor.afterRmvMap.size());

        assertEquals(2, interceptor.afterRmvMap.get(key2));
        assertEquals(3, interceptor.afterRmvMap.get(key3));
    }

    /**
     * @param key Key.
     * @param oldVal Expected old value.
     * @param newVal Expected new value.
     */
    private void assertBeforePutValue(String key, @Nullable Object oldVal, @Nullable Object newVal) {
        IgniteBiTuple t = interceptor.beforePutMap.get(key);

        assertNotNull(t);
        assertEquals(t.get1(), oldVal);
        assertEquals(t.get2(), newVal);
    }

    /**
     * @param grid Grid index.
     * @param op Operation type.
     * @param key Key.
     * @param val Value.
     * @param expOld Expected expOld value.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void update(int grid, Operation op, String key, final Integer val, @Nullable final Integer expOld)
        throws Exception {
        cacheUpdate(grid, false, op, key, val, expOld, null);
    }

    /**
     * @param grid Grid index.
     * @param op Operation type.
     * @param key Key.
     * @param expOld Expected expOld value.
     * @param expRmvRet Expected remove result.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void remove(int grid, Operation op, String key, @Nullable final Integer expOld,
        @Nullable final Integer expRmvRet) throws Exception {
        cacheUpdate(grid, true, op, key, null, expOld, expRmvRet);
    }

    /**
     * @param grid Grid index.
     * @param rmv If {@code true} then executes remove.
     * @param op Operation type.
     * @param key Key.
     * @param val Value.
     * @param expOld Expected expOld value.
     * @param expRmvRet Expected remove result.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void cacheUpdate(int grid, boolean rmv, Operation op, String key, final Integer val,
        @Nullable final Integer expOld, @Nullable final Integer expRmvRet)
        throws Exception {
        IgniteCache<String, Integer> cache = jcache(grid);

        if (rmv) {
            assertNull(val);

            switch (op) {
                case UPDATE: {
                    assertEquals(expRmvRet, cache.getAndRemove(key));

                    break;
                }

                case UPDATEX: {
                    cache.remove(key);

                    break;
                }

                case TRANSFORM: {
                    cache.invoke(key, new EntryProcessor<String, Integer, Void>() {
                        @Override public Void process(MutableEntry<String, Integer> e, Object... args) {
                            Integer old = e.getValue();

                            assertEquals(expOld, old);

                            e.remove();

                            return null;
                        }
                    });

                    break;
                }

                default:
                    fail();
            }
        }
        else {
            switch (op) {
                case UPDATE: {
                    assertEquals(expOld, cache.getAndPut(key, val));

                    break;
                }

                case UPDATEX: {
                    cache.put(key, val);

                    break;
                }

                case TRANSFORM: {
                    cache.invoke(key, new EntryProcessor<String, Integer, Void>() {
                        @Override public Void process(MutableEntry<String, Integer> e, Object... args) {
                            Integer old = e.getValue();

                            assertEquals(expOld, old);

                            e.setValue(val);

                            return null;
                        }
                    });

                    break;
                }

                default:
                    fail();
            }
        }
    }

    /**
     * @param grid Grid index.
     * @param op Operation type.
     * @param map Key/values map.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void batchUpdate(int grid, Operation op, final Map<String, Integer> map) throws Exception {
        cacheBatchUpdate(grid, false, op, map);
    }

    /**
     * @param grid Grid index.
     * @param op Operation type.
     * @param map Key/values map.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void batchRemove(int grid, Operation op, final Map<String, Integer> map) throws Exception {
        cacheBatchUpdate(grid, true, op, map);
    }

    /**
     * @param grid Grid index.
     * @param rmv If {@code true} then executes remove.
     * @param op Operation type.
     * @param map Key/values map.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void cacheBatchUpdate(int grid, boolean rmv, Operation op, final Map<String, Integer> map)
        throws Exception {
        IgniteCache<String, Integer> cache = jcache(grid);

        if (rmv) {
            switch (op) {
                case UPDATE: {
                    cache.removeAll(map.keySet());

                    break;
                }

                case TRANSFORM: {
                    cache.invokeAll(map.keySet(), new EntryProcessor<String, Integer, Void>() {
                        @Override public Void process(MutableEntry<String, Integer> e, Object... args) {
                            e.remove();

                            return null;
                        }
                    });

                    break;
                }

                default:
                    fail();
            }
        }
        else {
            switch (op) {
                case UPDATE: {
                    cache.putAll(map);

                    break;
                }

                case TRANSFORM: {
                    cache.invokeAll(map.keySet(), new EntryProcessor<String, Integer, Void>() {
                        @Override public Void process(MutableEntry<String, Integer> e, Object... args) {
                            e.setValue(map.get(e.getKey()));

                            return null;
                        }
                    });

                    break;
                }

                default:
                    fail();
            }
        }
    }

    /**
     * @param idx Grid index.
     * @return Primary key for grid.
     */
    private String primaryKey(int idx) {
        return primaryKeys(idx, 1).get(0);
    }

    /**
     * @param idx Grid index.
     * @param cnt Number of keys.
     * @return Primary keys for grid.
     */
    private List<String> primaryKeys(int idx, int cnt) {
        assert cnt > 0;

        Affinity aff = ignite(0).affinity(null);

        List<String> keys = new ArrayList<>(cnt);

        for (int i = 0; i < 10_000; i++) {
            String key = String.valueOf(i);

            if (aff.isPrimary(grid(idx).localNode(), key)) {
                keys.add(key);

                if (keys.size() == cnt)
                    break;
            }
        }

        assertEquals(cnt, keys.size());

        return keys;
    }

    /**
     * @param idx Grid index.
     * @return Primary key for grid.
     */
    private String backupKey(int idx) {
        Affinity aff = ignite(0).affinity(null);

        String key = null;

        for (int i = 0; i < 10_000; i++) {
            if (aff.isBackup(grid(idx).localNode(), String.valueOf(i))) {
                key = String.valueOf(i);

                break;
            }
        }

        assertNotNull(key);

        return key;
    }

    /**
     * @param idx Grid index.
     * @return Key which does not belong to the grid.
     */
    private String nearKey(int idx) {
        Affinity aff = ignite(0).affinity(null);

        String key = null;

        for (int i = 0; i < 10_000; i++) {
            if (!aff.isPrimaryOrBackup(grid(idx).localNode(), String.valueOf(i))) {
                key = String.valueOf(i);

                break;
            }
        }

        assertNotNull(key);

        return key;
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     * @throws Exception If failed.
     */
    private void checkCacheValue(Object key, @Nullable Object expVal) throws Exception {
        interceptor.disabled = true;

        if (storeEnabled())
            assertEquals("Unexpected store value", expVal, map.get(key));

        try {
            for (int i = 0; i < gridCount(); i++)
                assertEquals("Unexpected value for grid " + i, expVal, grid(i).cache(null).get(key));
        }
        finally {
            interceptor.disabled = false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void clearCaches() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            jcache(i).removeAll();
    }

    /**
     *
     */
    private enum Operation {
        /**
         *
         */
        UPDATE,

        /**
         *
         */
        UPDATEX,

        /**
         *
         */
        TRANSFORM,
    }

    /**
     *
     */
    private static class InterceptorAdapter implements CacheInterceptor {
        /** */
        @Nullable @Override public Object onGet(Object key, Object val) {
            fail("onGet not expected");

            return null;
        }

        /** */
        @Nullable @Override public Object onBeforePut(Cache.Entry entry, Object newVal) {
            fail("onBeforePut not expected");

            return null;
        }

        /** */
        @Override public void onAfterPut(Cache.Entry entry) {
            fail("onAfterPut not expected");
        }

        /** */
        @Nullable @Override public IgniteBiTuple onBeforeRemove(Cache.Entry entry) {
            fail("onBeforeRemove not expected");

            return null;
        }

        /** */
        @Override public void onAfterRemove(Cache.Entry entry) {
            fail("onAfterRemove not expected");
        }
    }

    /**
     *
     */
    private static class BeforeRemoveInterceptor extends InterceptorAdapter {
        /**
         *
         */
        private IgniteBiTuple ret;

        /**
         * @param ret Return value.
         */
        private BeforeRemoveInterceptor(IgniteBiTuple ret) {
            this.ret = ret;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple onBeforeRemove(Cache.Entry entry) {
            return ret;
        }
    }

    /**
     *
     */
    private static class Interceptor implements CacheInterceptor {
        /** */
        private final Map<Object, Object> getMap = new ConcurrentHashMap8<>();

        /** */
        private final Map<Object, Object> afterPutMap = new ConcurrentHashMap8<>();

        /** */
        private final Map<Object, IgniteBiTuple> beforePutMap = new ConcurrentHashMap8<>();

        /** */
        private final Map<Object, Object> beforeRmvMap = new ConcurrentHashMap8<>();

        /** */
        private final Map<Object, Object> afterRmvMap = new ConcurrentHashMap8<>();

        /** */
        private final AtomicInteger invokeCnt = new AtomicInteger();

        /** */
        private volatile boolean disabled;

        /** */
        private volatile CacheInterceptor retInterceptor;

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Nullable @Override public Object onGet(Object key, Object val) {
            if (disabled)
                return val;

            assertNotNull(retInterceptor);

            Object ret = retInterceptor.onGet(key, val);

            System.out.println("Get [key=" + key + ", val=" + val + ", ret=" + ret + ']');

            if (val != null) {
                Object old = getMap.put(key, val);

                assertNull(old); // Fot get interceptor is called on near node only.
            }

            invokeCnt.incrementAndGet();

            return ret;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Nullable @Override public Object onBeforePut(Cache.Entry entry, Object newVal) {
            if (disabled)
                return newVal;

            assertNotNull(retInterceptor);

            Object ret = retInterceptor.onBeforePut(entry, newVal);

            System.out.println("Before put [key=" + entry.getKey() + ", oldVal=" + entry.getValue()+ ", newVal=" + newVal
                + ", ret=" + ret + ']');

            invokeCnt.incrementAndGet();

            IgniteBiTuple t = beforePutMap.put(entry.getKey(), new IgniteBiTuple(entry.getValue(), newVal));

            if (t != null) {
                assertEquals("Interceptor called with different old values for key " + entry.getKey(), t.get1(),
                    entry.getValue());
                assertEquals("Interceptor called with different new values for key " + entry.getKey(), t.get2(),
                    newVal);
            }

            return ret;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry entry) {
            if (disabled)
                return;

            System.out.println("After put [key=" + entry.getKey() + ", val=" + entry.getValue() + ']');

            invokeCnt.incrementAndGet();

            Object old = afterPutMap.put(entry.getKey(), entry.getValue());

            if (old != null)
                assertEquals(old, entry.getValue());
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override @Nullable public IgniteBiTuple onBeforeRemove(Cache.Entry entry) {
            if (disabled)
                return new IgniteBiTuple(false, entry.getValue());

            assertNotNull(retInterceptor);

            IgniteBiTuple ret = retInterceptor.onBeforeRemove(entry);

            System.out.println("Before remove [key=" + entry.getKey() + ", val=" + entry.getValue() + ", ret=" + ret + ']');

            invokeCnt.incrementAndGet();

            if (entry.getValue() != null) {
                Object old = beforeRmvMap.put(entry.getKey(), entry.getValue());

                if (old != null)
                    assertEquals(old, entry.getValue());
            }

            return ret;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry entry) {
            if (disabled)
                return;

            System.out.println("After remove [key=" + entry.getKey() + ", val=" + entry.getValue() + ']');

            invokeCnt.incrementAndGet();

            if (entry.getValue() != null) {
                Object old = afterRmvMap.put(entry.getKey(), entry.getValue());

                if (old != null)
                    assertEquals(old, entry.getValue());
            }
        }

        /**
         *
         */
        public void reset() {
            invokeCnt.set(0);

            getMap.clear();
            beforePutMap.clear();
            afterPutMap.clear();
            afterRmvMap.clear();
            beforeRmvMap.clear();

            retInterceptor = null;
        }
    }

    /**
     *
     */
    private static class BatchRemoveInterceptor extends InterceptorAdapter {
        /** */
        private final String key1;

        /**
         * @param key1 Key.
         */
        public BatchRemoveInterceptor(String key1) {
            this.key1 = key1;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple onBeforeRemove(Cache.Entry entry) {
            return new IgniteBiTuple(entry.getKey().equals(key1), 999);
        }
    }

    /**
     *
     */
    private static class PutIncrementInterceptor extends InterceptorAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object onBeforePut(Cache.Entry entry, Object newVal) {
            return (Integer)newVal + 1;
        }
    }

    /**
     *
     */
    private static class NullPutInterceptor extends InterceptorAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object onBeforePut(Cache.Entry entry, Object newVal) {
            return null;
        }
    }

    /**
     *
     */
    private static class NullGetInterceptor extends InterceptorAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object onGet(Object key, Object val) {
            return null;
        }
    }

    /**
     *
     */
    private static class GetAllInterceptor1 extends InterceptorAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object onGet(Object key, Object val) {
            int k = Integer.valueOf((String)key);

            return k % 2 == 0 ? null : (k * 2);
        }
    }

    /**
     *
     */
    private static class GetAllInterceptor2 extends InterceptorAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object onGet(Object key, Object val) {
            int k = Integer.valueOf((String)key);

            switch (k % 3) {
                case 0:
                    return null;

                case 1:
                    return val;

                case 2:
                    return k * 3;

                default:
                    fail();
            }

            return null;
        }
    }

    /**
     *
     */
    private static class BatchPutInterceptor1 extends InterceptorAdapter {
        /** */
        private final String key1;

        /**
         * @param key1 Key.
         */
        public BatchPutInterceptor1(String key1) {
            this.key1 = key1;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onBeforePut(Cache.Entry entry, Object newVal) {
            if (entry.getKey().equals(key1))
                return null;

            return (Integer)newVal + 1;
        }
    }

    /**
     *
     */
    private static class GetIncrementInterceptor extends InterceptorAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object onGet(Object key, Object val) {
            return (Integer)val + 1;
        }
    }

    /**
     *
     */
    private static class OneGetInterceptor extends InterceptorAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object onGet(Object key, Object val) {
            return 1;
        }
    }
}