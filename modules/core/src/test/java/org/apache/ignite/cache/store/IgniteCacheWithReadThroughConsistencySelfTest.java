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

package org.apache.ignite.cache.store;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.util.TestTcpCommunicationSpi;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests that transaction is invalidated in case of {@link IgniteTxHeuristicCheckedException}.
 */
public class IgniteCacheWithReadThroughConsistencySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    @SuppressWarnings("FieldCanBeLocal")
    private static volatile String CACHE_NAME = "mycache";

    /** */
    private static final Map<Object, Object> storeMap = new ConcurrentHashMap8<>();

    /** Keyset for bulk query. */
    private static final HashSet<Long> KEYS = new HashSet<>();

    /* static construtor */
    static {
        KEYS.add(1L);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TcpCommunicationSpi comm = new TestTcpCommunicationSpi();

        comm.setSharedMemoryPort(-1);

        TestTcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        BinaryConfiguration bcfg = new BinaryConfiguration();
        bcfg.setCompactFooter(false);

        return new IgniteConfiguration()
            .setBinaryConfiguration(bcfg)
            .setGridName(igniteInstanceName)
            .setFailureDetectionTimeout(5_000)
            .setDiscoverySpi(discoSpi)
            .setCommunicationSpi(comm);
    }

    /**
     * @return Cache config.
     * @throws Exception If failed.
     */
    private static CacheConfiguration<Long, Employee> cacheConfiguration() throws Exception {
        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(Long.class.getName());
        qryEntity.setValueType(Employee.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", Long.class.getName());
        fields.put("firstName", String.class.getName());
        fields.put("lastName", String.class.getName());

        qryEntity.setFields(fields);
        qryEntity.setIndexes(Collections.singleton(new QueryIndex("id", QueryIndexType.SORTED, true, "id")));

        CacheConfiguration<Long, Employee> cacheCfg = new CacheConfiguration<>(CACHE_NAME);
        cacheCfg
            .setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED)
            .setOffHeapMaxMemory(128L * 1024L * 1024L)
            .setSwapEnabled(false)
            .setQueryEntities(Collections.singletonList(qryEntity))
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(0)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setCacheStoreFactory(new TestStoreFactory<>())
            .setReadThrough(true)
            .setWriteThrough(true);

        cacheCfg.setStoreKeepBinary(true);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        fillStore();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllStoreReadThrough() throws Exception {
        assert !storeMap.isEmpty();

        Ignite ignite = startGrid(0);

        final IgniteCache<Long, Employee> cache = ignite.getOrCreateCache(cacheConfiguration());

        Map<Long, Employee> res = cache.getAll(KEYS);

        assertNotNull(res);
        assertFalse(res.isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllMultithreadedStoreReadThrough() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                checkGetAllMultithreadStoreReadThrough(concurrency, isolation);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllOutTxMultithreadedStoreReadThrough() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                checkGetAllOutTxMultithreadStoreReadThrough(concurrency, isolation);
        }
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkGetAllMultithreadStoreReadThrough(final TransactionConcurrency concurrency,
        final TransactionIsolation isolation) throws Exception {
        assert !storeMap.isEmpty();

        final Ignite ignite = startGrid(0);

        final IgniteCache<Long, Employee> cache = ignite.getOrCreateCache(cacheConfiguration());

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Transaction tx = ignite.transactions().txStart(concurrency, isolation);

                    Collection<Object> values = cache
                        .withKeepBinary()
                        .getAll(KEYS)
                        .values();

                    tx.commit();

                    assertFalse("Readthrough is broken.", values.isEmpty());

                    BinaryObject val = (BinaryObject)values.iterator().next();

                    assertNotNull("Readthrough is broken.", val);

                    return val;
                }
            },
            10,
            "test-thread"
        );

        fut.get();

        stopAllGrids();
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkGetAllOutTxMultithreadStoreReadThrough(final TransactionConcurrency concurrency,
        final TransactionIsolation isolation) throws Exception {
        assert !storeMap.isEmpty();

        final Ignite ignite = startGrid(0);

        final IgniteCache<Long, Employee> cache = ignite.getOrCreateCache(cacheConfiguration());

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Transaction tx = ignite.transactions().txStart(concurrency, isolation);

                    Collection<Object> values = cache
                        .withKeepBinary()
                        .getAllOutTx(KEYS)
                        .values();

                    tx.commit();

                    assertFalse("Readthrough is broken.", values.isEmpty());

                    BinaryObject val = (BinaryObject)values.iterator().next();

                    assertNotNull("Readthrough is broken.", val);

                    return val;
                }
            },
            10,
            "test-thread"
        );

        fut.get();

        stopAllGrids();
    }

    /** */
    private void fillStore() throws Exception {
        storeMap.clear();

        Ignite ignite = startGrid(0);

        try (IgniteCache<Long, Employee> cache = ignite.getOrCreateCache(cacheConfiguration())) {
            cache.put(1L, new Employee(1L, "John", "Doe"));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public static class TestStoreFactory<K, V> implements Factory<CacheStore<K, V>> {
        /** {@inheritDoc} */
        @Override public CacheStore<K, V> create() {
            return new TestStore();
        }
    }

    /**
     *
     */
    public static class TestStore<K, V> extends CacheStoreAdapter<K, V> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public V load(K key) {
            return (V)storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends K, ? extends V> entry) {
            storeMap.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            storeMap.remove(key);
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            // No-op
        }
    }

    /**
     * Employee class.
     */
    public class Employee implements Serializable {
        /** Person ID (indexed). */
        private Long id;

        /** First name (not-indexed). */
        private String firstName;

        /** Last name (not indexed). */
        private String lastName;

        /**
         * Constructs person record.
         *
         * @param id Person ID.
         * @param firstName First name.
         * @param lastName Last name.
         */
        Employee(Long id, String firstName, String lastName) {
            this.id = id;

            this.firstName = firstName;
            this.lastName = lastName;
        }

        /**
         * {@inheritDoc}
         */
        @Override public String toString() {
            return "Employee [id=" + id +
                ", lastName=" + lastName +
                ", firstName=" + firstName + ']';
        }
    }
}
