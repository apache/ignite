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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.lang.gridfunc.ContainsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11820 https://issues.apache.org/jira/browse/IGNITE-11797
 */
@SuppressWarnings({"unchecked", "ThrowableNotThrown"})
public class IgniteCacheGroupsTest extends GridCommonAbstractTest {
    /** */
    private static final String GROUP1 = "grp1";

    /** */
    private static final String GROUP2 = "grp2";

    /** */
    private static final String GROUP3 = "grp3";

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final int ASYNC_TIMEOUT = 5000;

    /** */
    private CacheConfiguration[] ccfgs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCloseCache1() throws Exception {
        startGrid(0);

        Ignite client = startClientGrid(1);

        IgniteCache c1 = client.createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 0, false));

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(0, GROUP1, true);

        checkCache(0, "c1", 10);
        checkCache(1, "c1", 10);

        c1.close();

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(1, GROUP1, false);

        checkCache(0, "c1", 10);

        assertNotNull(client.cache("c1"));

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(1, GROUP1, true);

        checkCache(0, "c1", 10);
        checkCache(1, "c1", 10);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDestroyCaches1() throws Exception {
        createDestroyCaches(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDestroyCaches2() throws Exception {
        createDestroyCaches(5);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCacheWithSameNameInAnotherGroup() throws Exception {
        startGridsMultiThreaded(2);

        final Ignite ignite = ignite(0);

        ignite.createCache(cacheConfiguration(GROUP1, CACHE1, PARTITIONED, ATOMIC, 2, false));

        GridTestUtils.assertThrows(null, new GridPlainCallable<Void>() {
            @Override public Void call() throws Exception {
                ignite(1).createCache(cacheConfiguration(GROUP2, CACHE1, PARTITIONED, ATOMIC, 2, false));
                return null;
            }
        }, CacheExistsException.class, "a cache with the same name is already started");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDestroyCachesAtomicPartitioned() throws Exception {
        createDestroyCaches(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDestroyCachesTxPartitioned() throws Exception {
        createDestroyCaches(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDestroyCachesMvccTxPartitioned() throws Exception {
        createDestroyCaches(PARTITIONED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDestroyCachesAtomicReplicated() throws Exception {
        createDestroyCaches(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDestroyCachesTxReplicated() throws Exception {
        createDestroyCaches(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDestroyCachesMvccTxReplicated() throws Exception {
        createDestroyCaches(REPLICATED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryAtomicPartitioned() throws Exception {
        scanQuery(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryTxPartitioned() throws Exception {
        scanQuery(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryMvccTxPartitioned() throws Exception {
        scanQuery(PARTITIONED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryAtomicReplicated() throws Exception {
        scanQuery(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryTxReplicated() throws Exception {
        scanQuery(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryMvccTxReplicated() throws Exception {
        scanQuery(REPLICATED, TRANSACTIONAL_SNAPSHOT);
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryAtomicLocal() throws Exception {
        scanQuery(LOCAL, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryTxLocal() throws Exception {
        scanQuery(LOCAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9530")
    @Test
    public void testScanQueryMvccTxLocal() throws Exception {
        scanQuery(LOCAL, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntriesTtlAtomicPartitioned() throws Exception {
        entriesTtl(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntriesTtlTxPartitioned() throws Exception {
        entriesTtl(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7311")
    @Test
    public void testEntriesTtlMvccTxPartitioned() throws Exception {
        entriesTtl(PARTITIONED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntriesTtlAtomicReplicated() throws Exception {
        entriesTtl(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntriesTtlTxReplicated() throws Exception {
        entriesTtl(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7311")
    @Test
    public void testEntriesTtlMvccTxReplicated() throws Exception {
        entriesTtl(REPLICATED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntriesTtlAtomicLocal() throws Exception {
        entriesTtl(LOCAL, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntriesTtlTxLocal() throws Exception {
        entriesTtl(LOCAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9530,https://issues.apache.org/jira/browse/IGNITE-7311")
    @Test
    public void testEntriesTtlMvccTxLocal() throws Exception {
        entriesTtl(LOCAL, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIteratorAtomicPartitioned() throws Exception {
        cacheIterator(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIteratorTxPartitioned() throws Exception {
        cacheIterator(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIteratorMvccTxPartitioned() throws Exception {
        cacheIterator(PARTITIONED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIteratorAtomicReplicated() throws Exception {
        cacheIterator(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIteratorTxReplicated() throws Exception {
        cacheIterator(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIteratorMvccTxReplicated() throws Exception {
        cacheIterator(REPLICATED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIteratorAtomicLocal() throws Exception {
        cacheIterator(LOCAL, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIteratorTxLocal() throws Exception {
        cacheIterator(LOCAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9530")
    @Test
    public void testCacheIteratorMvccTxLocal() throws Exception {
        cacheIterator(LOCAL, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryMultiplePartitionsAtomicPartitioned() throws Exception {
        scanQueryMultiplePartitions(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryMultiplePartitionsTxPartitioned() throws Exception {
        scanQueryMultiplePartitions(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryMultiplePartitionsMvccTxPartitioned() throws Exception {
        scanQueryMultiplePartitions(PARTITIONED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryMultiplePartitionsAtomicReplicated() throws Exception {
        scanQueryMultiplePartitions(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryMultiplePartitionsTxReplicated() throws Exception {
        scanQueryMultiplePartitions(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryMultiplePartitionsMvccTxReplicated() throws Exception {
        scanQueryMultiplePartitions(REPLICATED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryTxReplicated() throws Exception {
        continuousQuery(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryMvccTxReplicated() throws Exception {
        continuousQuery(REPLICATED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryTxPartitioned() throws Exception {
        continuousQuery(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryMvccTxPartitioned() throws Exception {
        continuousQuery(PARTITIONED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryTxLocal() throws Exception {
        continuousQuery(LOCAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9530")
    @Test
    public void testContinuousQueryMvccTxLocal() throws Exception {
        continuousQuery(LOCAL, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryAtomicReplicated() throws Exception {
        continuousQuery(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryAtomicPartitioned() throws Exception {
        continuousQuery(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryAtomicLocal() throws Exception {
        continuousQuery(LOCAL, ATOMIC);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void scanQuery(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10_000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        boolean loc = cacheMode == LOCAL;

        if (loc)
            startGrid(0);
        else
            startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false));
        srv0.createCache(cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false));

        IgniteCache<Integer, Integer> cache1;
        IgniteCache<Integer, Integer> cache2;

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(loc ? 0 : 1);

            cache1 = ignite.cache(CACHE1);
            cache2 = ignite.cache(CACHE2);

            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < keys; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            // Async put ops.
            int ldrs = 4;

            List<Callable<?>> cls = new ArrayList<>(ldrs * 2);

            for (int i = 0; i < ldrs; i++) {
                cls.add(putOperation(loc ? 0 : 1, ldrs, i, CACHE1, data1));
                cls.add(putOperation(loc ? 0 : 2, ldrs, i, CACHE2, data2));
            }

            GridTestUtils.runMultiThreaded(cls, "loaders");
        }

        ScanQuery<Integer, Integer> qry = new ScanQuery<>();

        Set<Integer> keysSet = sequence(keys);

        for (Cache.Entry<Integer, Integer> entry : ignite(loc ? 0 : 3).cache(CACHE1).query(qry)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data1[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());

        srv0.destroyCache(CACHE1);

        keysSet = sequence(keys);

        for (Cache.Entry<Integer, Integer> entry : ignite(loc ? 0 : 3).cache(CACHE2).query(qry)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data2[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void continuousQuery(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        final int keys = 10_000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        boolean loc = cacheMode == LOCAL;

        if (loc)
            startGrid(0);
        else
            startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false));
        srv0.createCache(cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false));

        final AtomicInteger cntr1 = new AtomicInteger();
        final AtomicInteger cntr2 = new AtomicInteger();

        CacheEntryUpdatedListener lsnr1 = new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(
                Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> ignored : evts)
                    cntr1.incrementAndGet();
            }
        };

        CacheEntryUpdatedListener lsnr2 = new CacheEntryUpdatedListener<Integer, Integer>() {
            @Override public void onUpdated(
                Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> ignored : evts)
                    cntr2.incrementAndGet();
            }
        };

        QueryCursor qry1 = ignite(loc ? 0 : 2).cache(CACHE1).query(new ContinuousQuery<>().setLocalListener(lsnr1));
        QueryCursor qry2 = ignite(loc ? 0 : 3).cache(CACHE2).query(new ContinuousQuery<>().setLocalListener(lsnr2));

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(loc ? 0 : 1);

            IgniteCache<Integer, Integer> cache1 = ignite.cache(CACHE1);
            IgniteCache<Integer, Integer> cache2 = ignite.cache(CACHE2);

            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < keys; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            int ldrs = 4;

            List<Callable<?>> cls = new ArrayList<>(ldrs * 2);

            for (int i = 0; i < ldrs; i++) {
                cls.add(putOperation(loc ? 0 : 1, ldrs, i, CACHE1, data1));
                cls.add(putOperation(loc ? 0 : 2, ldrs, i, CACHE2, data2));
            }

            GridTestUtils.runMultiThreaded(cls, "loaders");
        }

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return cntr1.get() == keys && cntr2.get() == keys;
            }
        }, 2000);

        assertEquals(cntr1.get(), keys);
        assertEquals(cntr2.get(), keys);

        qry1.close();

        Map<Integer, Integer> map = generateDataMap(10);

        srv0.cache(CACHE1).putAll(map);
        srv0.cache(CACHE2).putAll(map);

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return cntr2.get() == keys + 10;
            }
        }, 2000);

        assertEquals(keys + 10, cntr2.get());

        assertEquals(keys, cntr1.get());

        qry2.close();
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void scanQueryMultiplePartitions(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(
            cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(32)));
        srv0.createCache(
            cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(32)));

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1;
        IgniteCache<Integer, Integer> cache2;

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(1);

            cache1 = ignite.cache(CACHE1);
            cache2 = ignite.cache(CACHE2);

            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < keys; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            // Async put ops.
            int ldrs = 4;

            List<Callable<?>> cls = new ArrayList<>(ldrs * 2);

            for (int i = 0; i < ldrs; i++) {
                cls.add(putOperation(1, ldrs, i, CACHE1, data1));
                cls.add(putOperation(2, ldrs, i, CACHE2, data2));
            }

            GridTestUtils.runMultiThreaded(cls, "loaders");
        }

        int p = ThreadLocalRandom.current().nextInt(32);

        ScanQuery<Integer, Integer> qry = new ScanQuery().setPartition(p);

        Set<Integer> keysSet = new TreeSet<>();

        cache1 = ignite(3).cache(CACHE1);

        Affinity<Integer> aff = affinity(cache1);

        for (int i = 0; i < keys; i++) {
            if (aff.partition(i) == p)
                keysSet.add(i);
        }

        for (Cache.Entry<Integer, Integer> entry : cache1.query(qry)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data1[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());

        srv0.destroyCache(CACHE1);

        keysSet = new TreeSet<>();

        cache2 = ignite(3).cache(CACHE2);

        aff = affinity(cache2);

        for (int i = 0; i < keys; i++) {
            if (aff.partition(i) == p)
                keysSet.add(i);
        }

        for (Cache.Entry<Integer, Integer> entry : cache2.query(qry)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data2[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void cacheIterator(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        boolean loc = cacheMode == LOCAL;

        if (loc)
            startGrid(0);
        else
            startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false));
        srv0.createCache(cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false));

        if (!loc)
            awaitPartitionMapExchange();

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(loc ? 0 : 1);

            IgniteCache cache1 = ignite.cache(CACHE1);
            IgniteCache cache2 = ignite.cache(CACHE2);

            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < keys; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            // Async put ops.
            int ldrs = 4;

            List<Callable<?>> cls = new ArrayList<>(ldrs * 2);

            for (int i = 0; i < ldrs; i++) {
                cls.add(putOperation(loc ? 0 : 1, ldrs, i, CACHE1, data1));
                cls.add(putOperation(loc ? 0 : 2, ldrs, i, CACHE2, data2));
            }

            GridTestUtils.runMultiThreaded(cls, "loaders");
        }

        Set<Integer> keysSet = sequence(keys);

        for (Cache.Entry<Integer, Integer> entry : ignite(loc ? 0 : 3).<Integer, Integer>cache(CACHE1)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data1[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());

        srv0.destroyCache(CACHE1);

        keysSet = sequence(keys);

        for (Cache.Entry<Integer, Integer> entry : ignite(loc ? 0 : 3).<Integer, Integer>cache(CACHE2)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data2[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void entriesTtl(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10000;

        final int ttl = 10000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        boolean loc = cacheMode == LOCAL;

        if (loc)
            startGrid(0);
        else
            startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(
            cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false)
                // -1 = ETERNAL       just created entries are not expiring
                // -2 = NOT_CHANGED   not to change ttl on entry update
                .setExpiryPolicyFactory(new PlatformExpiryPolicyFactory(-1, -2, ttl)).setEagerTtl(true)
        );

        srv0.createCache(cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false));

        if (!loc)
            awaitPartitionMapExchange();

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(loc ? 0 : 1);

            IgniteCache cache1 = ignite.cache(CACHE1);
            IgniteCache cache2 = ignite.cache(CACHE2);

            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < keys; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            // async put ops
            int ldrs = 4;

            List<Callable<?>> cls = new ArrayList<>(ldrs * 2);

            for (int i = 0; i < ldrs; i++) {
                cls.add(putOperation(loc ? 0 : 1, ldrs, i, CACHE1, data1));
                cls.add(putOperation(loc ? 0 : 2, ldrs, i, CACHE2, data2));
            }

            GridTestUtils.runMultiThreaded(cls, "loaders");
        }

        checkData(loc ? 0 : 3, CACHE1, data1);
        checkData(loc ? 0 : 3, CACHE2, data2);

        srv0.destroyCache(CACHE2);

        checkData(loc ? 0 : 3, CACHE1, data1);

        // Wait for expiration

        Thread.sleep((long)(ttl * 1.2));

        assertEquals(0, ignite(loc ? 0 : 3).cache(CACHE1).size());
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void createDestroyCaches(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false));
        srv0.createCache(cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false));

        awaitPartitionMapExchange();

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(1);

            IgniteCache cache1 = ignite.cache(CACHE1);
            IgniteCache cache2 = ignite.cache(CACHE2);

            try (Transaction tx = ignite.transactions().txStart()) {
                for (int i = 0; i < keys; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            int ldrs = 4;

            List<Callable<?>> cls = new ArrayList<>(ldrs * 2);

            for (int i = 0; i < ldrs; i++) {
                cls.add(putOperation(1, ldrs, i, CACHE1, data1));
                cls.add(putOperation(2, ldrs, i, CACHE2, data2));
            }

            GridTestUtils.runMultiThreaded(cls, "loaders");
        }

        checkLocalData(3, CACHE1, data1);
        checkLocalData(0, CACHE2, data2);

        checkData(0, CACHE1, data1);
        checkData(3, CACHE2, data2);

        ignite(1).destroyCache(CACHE2);

        startGrid(5);

        awaitPartitionMapExchange();

        checkData(5, CACHE1, data1);
        checkLocalData(5, CACHE1, data1);

        ignite(1).destroyCache(CACHE1);

        checkCacheGroup(5, GROUP1, false);
    }

    /**
     * @param idx Node index.
     * @param ldrs Loaders count.
     * @param ldrIdx Loader index.
     * @param cacheName Cache name.
     * @param data Data.
     * @return Callable for put operation.
     */
    private Callable<Void> putOperation(
            final int idx,
            final int ldrs,
            final int ldrIdx,
            final String cacheName,
            final Integer[] data) {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteCache cache = ignite(idx).cache(cacheName);

                for (int j = 0, size = data.length; j < size; j++) {
                    if (j % ldrs == ldrIdx)
                        cache.put(j, data[j]);
                }

                return null;
            }
        };
    }

    /**
     * Creates an array of random integers.
     *
     * @param cnt Array length.
     * @return Array of random integers.
     */
    private Integer[] generateData(int cnt) {
        Random rnd = ThreadLocalRandom.current();

        Integer[] data = new Integer[cnt];

        for (int i = 0; i < data.length; i++)
            data[i] = rnd.nextInt();

        return data;
    }

    /**
     * Creates a map with random integers.
     *
     * @param cnt Map size length.
     * @return Map with random integers.
     */
    private Map<Integer, Integer> generateDataMap(int cnt) {
        return generateDataMap(0, cnt);
    }

    /**
     * Creates a map with random integers.
     *
     * @param startKey Start key.
     * @param cnt Map size length.
     * @return Map with random integers.
     */
    private Map<Integer, Integer> generateDataMap(int startKey, int cnt) {
        Random rnd = ThreadLocalRandom.current();

        Map<Integer, Integer> data = new TreeMap<>();

        for (int i = 0; i < cnt; i++)
            data.put(startKey++, rnd.nextInt());

        return data;
    }

    /**
     * @param cnt Sequence length.
     * @return Sequence of integers.
     */
    private Set<Integer> sequence(int cnt) {
        Set<Integer> res = new TreeSet<>();

        for (int i = 0; i < cnt; i++)
            res.add(i);

        return res;
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     * @param data Expected data.
     * @throws Exception If failed.
     */
    private void checkData(int idx, String cacheName, Integer[] data) throws Exception {
        Set<Integer> keys = sequence(data.length);

        Set<Map.Entry<Integer, Integer>> entries =
            ignite(idx).<Integer, Integer>cache(cacheName).getAll(keys).entrySet();

        for (Map.Entry<Integer, Integer> entry : entries) {
            assertTrue(keys.remove(entry.getKey()));
            assertEquals(data[entry.getKey()], entry.getValue());
        }

        assertTrue(keys.isEmpty());
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     * @param data Expected data.
     * @throws Exception If failed.
     */
    private void checkLocalData(int idx, String cacheName, Integer[] data) throws Exception {
        Ignite ignite = ignite(idx);
        ClusterNode node = ignite.cluster().localNode();
        IgniteCache cache = ignite.<Integer, Integer>cache(cacheName);

        Affinity aff = affinity(cache);

        Set<Integer> locKeys = new TreeSet<>();

        for (int key = 0; key < data.length; key++) {
            if (aff.isPrimaryOrBackup(node, key))
                locKeys.add(key);
        }

        Iterable<Cache.Entry<Integer, Integer>> locEntries = cache.localEntries(CachePeekMode.OFFHEAP);

        for (Cache.Entry<Integer, Integer> entry : locEntries) {
            assertTrue(locKeys.remove(entry.getKey()));
            assertEquals(data[entry.getKey()], entry.getValue());
        }

        assertTrue(locKeys.isEmpty());
    }

    /**
     * @param srvs Number of server nodes.
     * @throws Exception If failed.
     */
    private void createDestroyCaches(int srvs) throws Exception {
        startGridsMultiThreaded(srvs);

        checkCacheDiscoveryDataConsistent();

        Ignite srv0 = ignite(0);

        for (int i = 0; i < srvs; i++)
            checkCacheGroup(i, GROUP1, false);

        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            srv0.createCache(cacheConfiguration(GROUP1, CACHE1, PARTITIONED, ATOMIC, 2, false));

            checkCacheDiscoveryDataConsistent();

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, CACHE1, 10);
            }

            srv0.createCache(cacheConfiguration(GROUP1, CACHE2, PARTITIONED, ATOMIC, 2, false));

            checkCacheDiscoveryDataConsistent();

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, CACHE2, 10);
            }

            srv0.destroyCache(CACHE1);

            checkCacheDiscoveryDataConsistent();

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, CACHE2, 10);
            }

            srv0.destroyCache(CACHE2);

            checkCacheDiscoveryDataConsistent();

            for (int i = 0; i < srvs; i++)
                checkCacheGroup(i, GROUP1, false);
        }
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     * @param ops Number of operations to execute.
     */
    private void checkCache(int idx, String cacheName, int ops) {
        IgniteCache cache = ignite(idx).cache(cacheName);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < ops; i++) {
            Integer key = rnd.nextInt();

            cache.put(key, i);

            assertEquals(i, cache.get(key));
        }
    }

    /**
     * @param cache3 {@code True} if add last cache.
     * @return Cache configurations.
     */
    private CacheConfiguration[] staticConfigurations1(boolean cache3) {
        CacheConfiguration[] ccfgs = new CacheConfiguration[cache3 ? 3 : 2];

        ccfgs[0] = cacheConfiguration(null, "cache1", PARTITIONED, ATOMIC, 2, false);
        ccfgs[1] = cacheConfiguration(GROUP1, "cache2", PARTITIONED, ATOMIC, 2, false);

        if (cache3)
            ccfgs[2] = cacheConfiguration(GROUP1, "cache3", PARTITIONED, ATOMIC, 2, false);

        return ccfgs;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiscoveryDataConsistency1() throws Exception {
        ccfgs = staticConfigurations1(true);
        Ignite srv0 = startGrid(0);

        ccfgs = staticConfigurations1(true);
        startGrid(1);

        checkCacheDiscoveryDataConsistent();

        ccfgs = null;
        startGrid(2);

        checkCacheDiscoveryDataConsistent();

        srv0.createCache(cacheConfiguration(null, "cache4", PARTITIONED, ATOMIC, 2, false));

        checkCacheDiscoveryDataConsistent();

        ccfgs = staticConfigurations1(true);
        startGrid(3);

        checkCacheDiscoveryDataConsistent();

        srv0.createCache(cacheConfiguration(GROUP1, "cache5", PARTITIONED, ATOMIC, 2, false));

        ccfgs = staticConfigurations1(true);
        startGrid(4);

        checkCacheDiscoveryDataConsistent();

        for (int i = 0; i < 5; i++)
            checkCacheGroup(i, GROUP1, true);

        srv0.destroyCache("cache1");
        srv0.destroyCache("cache2");
        srv0.destroyCache("cache3");

        checkCacheDiscoveryDataConsistent();

        ccfgs = staticConfigurations1(true);
        startGrid(5);

        checkCacheDiscoveryDataConsistent();

        for (int i = 0; i < 6; i++)
            checkCacheGroup(i, GROUP1, true);

        srv0.destroyCache("cache1");
        srv0.destroyCache("cache2");
        srv0.destroyCache("cache3");
        srv0.destroyCache("cache4");
        srv0.destroyCache("cache5");

        ccfgs = staticConfigurations1(true);
        startGrid(6);

        checkCacheDiscoveryDataConsistent();

        srv0.createCache(cacheConfiguration(null, "cache4", PARTITIONED, ATOMIC, 2, false));
        srv0.createCache(cacheConfiguration(GROUP1, "cache5", PARTITIONED, ATOMIC, 2, false));

        checkCacheDiscoveryDataConsistent();

        ccfgs = staticConfigurations1(false);
        startGrid(7);

        checkCacheDiscoveryDataConsistent();

        awaitPartitionMapExchange();
    }

    /**
     * @param cnt Caches number.
     * @param grp Cache groups.
     * @param baseName Caches name prefix.
     * @return Cache configurations.
     */
    private CacheConfiguration[] cacheConfigurations(int cnt, String grp, String baseName) {
        CacheConfiguration[] ccfgs = new CacheConfiguration[cnt];

        for (int i = 0; i < cnt; i++) {
            ccfgs[i] = cacheConfiguration(grp,
                baseName + i, PARTITIONED,
                i % 2 == 0 ? TRANSACTIONAL : ATOMIC,
                2,
                false).setAffinity(new RendezvousAffinityFunction(false, 256));
        }

        return ccfgs;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartManyCaches() throws Exception {
        final int CACHES = SF.apply(5_000);

        final int NODES = 4;

        for (int i = 0; i < NODES; i++) {
            ccfgs = cacheConfigurations(CACHES, GROUP1, "testCache1-");

            boolean client = i == NODES - 1;

            if (client)
                startClientGrid(i);
            else
                startGrid(i);

        }

        Ignite client = ignite(NODES - 1);

        client.createCaches(Arrays.asList(cacheConfigurations(CACHES, GROUP2, "testCache2-")));

        checkCacheDiscoveryDataConsistent();

        for (int i = 0; i < NODES; i++) {
            log.info("Check node: " + i);

            for (int c = 0; c < 10; c++) {
                int cache = ThreadLocalRandom.current().nextInt(CACHES);

                checkCache(i, "testCache1-" + cache, 1);
                checkCache(i, "testCache2-" + cache, 1);
            }
        }

        log.info("Stop nodes.");

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                stopGrid(idx);
            }
        }, NODES, "stopThread");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalance1() throws Exception {
        Ignite srv0 = startGrid(0);

        IgniteCache<Object, Object> srv0Cache1 =
            srv0.createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 2, false));
        IgniteCache<Object, Object> srv0Cache2 =
            srv0.createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 2, false));
        IgniteCache<Object, Object> srv0Cache3 =
            srv0.createCache(cacheConfiguration(GROUP2, "c3", PARTITIONED, TRANSACTIONAL, 2, false));
        IgniteCache<Object, Object> srv0Cache4 =
            srv0.createCache(cacheConfiguration(GROUP2, "c4", PARTITIONED, TRANSACTIONAL, 2, false));
        IgniteCache<Object, Object> srv0Cache5 =
            srv0.createCache(cacheConfiguration(GROUP3, "c5", PARTITIONED, TRANSACTIONAL_SNAPSHOT, 2, false));
        IgniteCache<Object, Object> srv0Cache6 =
            srv0.createCache(cacheConfiguration(GROUP3, "c6", PARTITIONED, TRANSACTIONAL_SNAPSHOT, 2, false));

        final int ITEMS = 1_000;

        for (int i = 0; i < ITEMS; i++) {
            srv0Cache1.put(new Key1(i), i);

            srv0Cache3.put(new Key1(i), i);
            srv0Cache4.put(new Key1(i), -i);

            srv0Cache5.put(new Key1(i), i);
            srv0Cache6.put(new Key1(i), -i);
        }

        assertEquals(ITEMS, srv0Cache1.size());
        assertEquals(ITEMS, srv0Cache1.localSize());
        assertEquals(0, srv0Cache2.size());
        assertEquals(ITEMS, srv0Cache3.size());
        assertEquals(ITEMS, srv0Cache4.localSize());
        assertEquals(ITEMS, srv0Cache5.size());
        assertEquals(ITEMS, srv0Cache6.localSize());

        startGrid(1);

        awaitPartitionMapExchange();

        for (int i = 0; i < 2; i++) {
            Ignite node = ignite(i);

            IgniteCache<Object, Object> cache1 = node.cache("c1");
            IgniteCache<Object, Object> cache2 = node.cache("c2");
            IgniteCache<Object, Object> cache3 = node.cache("c3");
            IgniteCache<Object, Object> cache4 = node.cache("c4");
            IgniteCache<Object, Object> cache5 = node.cache("c5");
            IgniteCache<Object, Object> cache6 = node.cache("c6");

            assertEquals(ITEMS * 2, cache1.size(CachePeekMode.ALL));
            assertEquals(ITEMS, cache1.localSize(CachePeekMode.ALL));
            assertEquals(0, cache2.size(CachePeekMode.ALL));
            assertEquals(0, cache2.localSize(CachePeekMode.ALL));

            assertEquals(ITEMS * 2, cache3.size(CachePeekMode.ALL));
            assertEquals(ITEMS, cache3.localSize(CachePeekMode.ALL));

            assertEquals(ITEMS * 2, cache4.size(CachePeekMode.ALL));
            assertEquals(ITEMS, cache4.localSize(CachePeekMode.ALL));

            assertEquals(ITEMS * 2, cache5.size(CachePeekMode.ALL));
            assertEquals(ITEMS, cache5.localSize(CachePeekMode.ALL));

            assertEquals(ITEMS * 2, cache6.size(CachePeekMode.ALL));
            assertEquals(ITEMS, cache6.localSize(CachePeekMode.ALL));

            for (int k = 0; k < ITEMS; k++) {
                assertEquals(i, cache1.localPeek(new Key1(i)));
                assertNull(cache2.localPeek(new Key1(i)));
                assertEquals(i, cache3.localPeek(new Key1(i)));
                assertEquals(-i, cache4.localPeek(new Key1(i)));
                assertEquals(i, cache5.localPeek(new Key1(i)));
                assertEquals(-i, cache6.localPeek(new Key1(i)));
            }
        }

        for (int i = 0; i < ITEMS * 2; i++)
            srv0Cache2.put(new Key1(i), i + 1);

        Ignite srv2 = startGrid(2);

        awaitPartitionMapExchange();

        for (int i = 0; i < 3; i++) {
            Ignite node = ignite(i);

            IgniteCache<Object, Object> cache1 = node.cache("c1");
            IgniteCache<Object, Object> cache2 = node.cache("c2");
            IgniteCache<Object, Object> cache3 = node.cache("c3");
            IgniteCache<Object, Object> cache4 = node.cache("c4");
            IgniteCache<Object, Object> cache5 = node.cache("c5");
            IgniteCache<Object, Object> cache6 = node.cache("c6");

            assertEquals(ITEMS * 3, cache1.size(CachePeekMode.ALL));
            assertEquals(ITEMS, cache1.localSize(CachePeekMode.ALL));
            assertEquals(ITEMS * 6, cache2.size(CachePeekMode.ALL));
            assertEquals(ITEMS * 2, cache2.localSize(CachePeekMode.ALL));
            assertEquals(ITEMS, cache3.localSize(CachePeekMode.ALL));
            assertEquals(ITEMS, cache4.localSize(CachePeekMode.ALL));
            assertEquals(ITEMS, cache5.localSize(CachePeekMode.ALL));
            assertEquals(ITEMS, cache6.localSize(CachePeekMode.ALL));
        }

        IgniteCache<Object, Object> srv2Cache1 = srv2.cache("c1");
        IgniteCache<Object, Object> srv2Cache2 = srv2.cache("c2");

        for (int i = 0; i < ITEMS; i++)
            assertEquals(i, srv2Cache1.localPeek(new Key1(i)));

        for (int i = 0; i < ITEMS * 2; i++)
            assertEquals(i + 1, srv2Cache2.localPeek(new Key1(i)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalance2() throws Exception {
        Ignite srv0 = startGrid(0);

        IgniteCache<Object, Object> srv0Cache1 =
            srv0.createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 0, false));
        IgniteCache<Object, Object> srv0Cache2 =
            srv0.createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 0, false));

        Affinity aff = srv0.affinity("c1");

        final int ITEMS = 2_000;

        Map<Integer, Integer> c1Data = new HashMap<>();
        Map<Integer, Integer> c2Data = new HashMap<>();

        for (int i = 0; i < ITEMS; i++) {
            srv0Cache1.put(i, i);
            c1Data.put(i, i);

            if (i % 2 == 0) {
                srv0Cache2.put(i, i);
                c2Data.put(i, i);
            }
        }

        assertEquals(ITEMS, srv0Cache1.size());
        assertEquals(ITEMS / 2, srv0Cache2.size());

        Ignite srv1 = startGrid(1);

        awaitPartitionMapExchange();

        assertEquals(ITEMS, srv0Cache1.size());
        assertEquals(ITEMS / 2, srv0Cache2.size());

        checkCacheData(c1Data, "c1");
        checkCacheData(c2Data, "c2");

        Set<Integer> srv1Parts = new HashSet<>();

        for (Integer p : aff.primaryPartitions(srv1.cluster().localNode()))
            srv1Parts.add(p);

        CacheGroupContext grpSrv0 = cacheGroup(srv0, GROUP1);
        CacheGroupContext grpSrv1 = cacheGroup(srv1, GROUP1);

        for (int p = 0; p < aff.partitions(); p++) {
            if (srv1Parts.contains(p)) {
                GridIterator<CacheDataRow> it = grpSrv0.offheap().partitionIterator(p);
                assertFalse(it.hasNext());

                it = grpSrv1.offheap().partitionIterator(p);
                assertTrue(it.hasNext());
            }
            else {
                GridIterator<CacheDataRow> it = grpSrv0.offheap().partitionIterator(p);
                assertTrue(it.hasNext());

                it = grpSrv1.offheap().partitionIterator(p);
                assertFalse(it.hasNext());
            }
        }

        c1Data = new HashMap<>();
        c2Data = new HashMap<>();

        for (int i = 0; i < ITEMS; i++) {
            srv0Cache1.put(i, i + 1);
            c1Data.put(i, i + 1);

            if (i % 2 == 0) {
                srv0Cache2.put(i, i + 1);
                c2Data.put(i, i + 1);
            }
        }

        checkCacheData(c1Data, "c1");
        checkCacheData(c2Data, "c2");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoKeyIntersectTx() throws Exception {
        testNoKeyIntersect(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoKeyIntersectMvccTx() throws Exception {
        testNoKeyIntersect(TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoKeyIntersectAtomic() throws Exception {
        testNoKeyIntersect(ATOMIC);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private void testNoKeyIntersect(CacheAtomicityMode atomicityMode) throws Exception {
        startGrid(0);

        testNoKeyIntersect(atomicityMode, false);

        testNoKeyIntersect(atomicityMode, true);

        startGridsMultiThreaded(1, 4);

        testNoKeyIntersect(atomicityMode, false);

        testNoKeyIntersect(atomicityMode, true);
    }

    /**
     * @param keys Keys.
     * @param rnd Random.
     * @return Added key.
     */
    private Integer addKey(Set<Integer> keys, ThreadLocalRandom rnd) {
        for (;;) {
            Integer key = rnd.nextInt(100_000);

            if (keys.add(key))
                return key;
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param heapCache On heap cache flag.
     * @throws Exception If failed.
     */
    private void testNoKeyIntersect(CacheAtomicityMode atomicityMode, boolean heapCache) throws Exception {
        Ignite srv0 = ignite(0);

        try {
            IgniteCache cache1 = srv0.
                createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, atomicityMode, 1, heapCache));

            Set<Integer> keys = new LinkedHashSet<>(30);

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < 10; i++) {
                Integer key = addKey(keys, rnd);

                cache1.put(key, key);
                cache1.put(new Key1(key), new Value1(key));
                cache1.put(new Key2(key), new Value2(key));
            }

            assertEquals(30, cache1.size());

            IgniteCache cache2 = srv0.
                createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, atomicityMode, 1, heapCache));

            assertEquals(30, cache1.size());
            assertEquals(0, cache2.size());

            for (Integer key : keys) {
                assertNull(cache2.get(key));
                assertNull(cache2.get(new Key1(key)));
                assertNull(cache2.get(new Key2(key)));

                cache2.put(key, key + 1);
                cache2.put(new Key1(key), new Value1(key + 1));
                cache2.put(new Key2(key), new Value2(key + 1));
            }

            assertEquals(30, cache1.size());
            assertEquals(30, cache2.size());

            for (int i = 0; i < 10; i++) {
                Integer key = addKey(keys, rnd);

                cache2.put(key, key + 1);
                cache2.put(new Key1(key), new Value1(key + 1));
                cache2.put(new Key2(key), new Value2(key + 1));
            }

            assertEquals(30, cache1.size());
            assertEquals(60, cache2.size());

            int i = 0;

            for (Integer key : keys) {
                if (i++ < 10) {
                    assertEquals(key, cache1.get(key));
                    assertEquals(new Value1(key), cache1.get(new Key1(key)));
                    assertEquals(new Value2(key), cache1.get(new Key2(key)));
                }
                else {
                    assertNull(cache1.get(key));
                    assertNull(cache1.get(new Key1(key)));
                    assertNull(cache1.get(new Key2(key)));
                }

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));
            }

            IgniteCache cache3 = srv0.
                createCache(cacheConfiguration(GROUP1, "c3", PARTITIONED, atomicityMode, 1, heapCache));

            assertEquals(30, cache1.size());
            assertEquals(60, cache2.size());
            assertEquals(0, cache3.size());

            for (Integer key : keys) {
                assertNull(cache3.get(key));
                assertNull(cache3.get(new Key1(key)));
                assertNull(cache3.get(new Key2(key)));
            }

            for (Integer key : keys) {
                cache3.put(key, key);
                cache3.put(new Key1(key), new Value1(key));
                cache3.put(new Key2(key), new Value2(key));
            }

            i = 0;

            for (Integer key : keys) {
                if (i++ < 10) {
                    assertEquals(key, cache1.get(key));
                    assertEquals(new Value1(key), cache1.get(new Key1(key)));
                    assertEquals(new Value2(key), cache1.get(new Key2(key)));
                }
                else {
                    assertNull(cache1.get(key));
                    assertNull(cache1.get(new Key1(key)));
                    assertNull(cache1.get(new Key2(key)));
                }

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            i = 0;

            for (Integer key : keys) {
                if (i++ == 3)
                    break;

                cache1.remove(key);
                cache1.remove(new Key1(key));
                cache1.remove(new Key2(key));

                assertNull(cache1.get(key));
                assertNull(cache1.get(new Key1(key)));
                assertNull(cache1.get(new Key2(key)));

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            cache1.removeAll();

            for (Integer key : keys) {
                assertNull(cache1.get(key));
                assertNull(cache1.get(new Key1(key)));
                assertNull(cache1.get(new Key2(key)));

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            cache2.removeAll();

            for (Integer key : keys) {
                assertNull(cache1.get(key));
                assertNull(cache1.get(new Key1(key)));
                assertNull(cache1.get(new Key2(key)));

                assertNull(cache2.get(key));
                assertNull(cache2.get(new Key1(key)));
                assertNull(cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            if (atomicityMode == TRANSACTIONAL)
                testNoKeyIntersectTxLocks(cache1, cache2);
        }
        finally {
            srv0.destroyCaches(Arrays.asList("c1", "c2", "c3"));
        }
    }

    /**
     * @param cache1 Cache1.
     * @param cache2 Cache2.
     * @throws Exception If failed.
     */
    private void testNoKeyIntersectTxLocks(final IgniteCache cache1, final IgniteCache cache2) throws Exception {
        final Ignite node = (Ignite)cache1.unwrap(Ignite.class);

        for (int i = 0; i < 5; i++) {
            final Integer key = ThreadLocalRandom.current().nextInt(1000);

            Lock lock = cache1.lock(key);

            lock.lock();

            try {
                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                    @Override public Object call() throws Exception {
                        Lock lock1 = cache1.lock(key);

                        assertFalse(lock1.tryLock());

                        Lock lock2 = cache2.lock(key);

                        assertTrue(lock2.tryLock());

                        lock2.unlock();

                        return null;
                    }
                }, "lockThread");

                fut.get(10_000);
            }
            finally {
                lock.unlock();
            }

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache1.put(key, 1);

                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache2.put(key, 2);

                            tx.commit();
                        }

                        assertEquals(2, cache2.get(key));

                        return null;
                    }
                }, "txThread");

                fut.get(10_000);

                tx.commit();
            }

            assertEquals(1, cache1.get(key));
            assertEquals(2, cache2.get(key));

            try (Transaction tx = node.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                Integer val = (Integer)cache1.get(key);

                cache1.put(key, val + 10);

                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache2.put(key, 3);

                            tx.commit();
                        }

                        assertEquals(3, cache2.get(key));

                        return null;
                    }
                }, "txThread");

                fut.get(10_000);

                tx.commit();
            }

            assertEquals(11, cache1.get(key));
            assertEquals(3, cache2.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheApiTxPartitioned() throws Exception {
        cacheApiTest(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7952")
    @Test
    public void testCacheApiMvccTxPartitioned() throws Exception {
        cacheApiTest(PARTITIONED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheApiTxReplicated() throws Exception {
        cacheApiTest(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7952")
    @Test
    public void testCacheApiMvccTxReplicated() throws Exception {
        cacheApiTest(REPLICATED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheApiAtomicPartitioned() throws Exception {
        cacheApiTest(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheApiAtomicReplicated() throws Exception {
        cacheApiTest(REPLICATED, ATOMIC);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private void cacheApiTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        startGridsMultiThreaded(4);

        startClientGrid(4);

        int[] backups = cacheMode == REPLICATED ? new int[]{Integer.MAX_VALUE} : new int[]{0, 1, 2, 3};

        for (int backups0 : backups)
            cacheApiTest(cacheMode, atomicityMode, backups0, false, false, false);

        int backups0 = cacheMode == REPLICATED ? Integer.MAX_VALUE :
            backups[ThreadLocalRandom.current().nextInt(backups.length)];

        cacheApiTest(cacheMode, atomicityMode, backups0, true, false, false);

        if (cacheMode == PARTITIONED) {
            // Here the f variable is used as a bit set where 2 last bits
            // determine whether a near cache is used on server/client side.
            // The case without near cache is already tested at this point.
            for (int f : new int[]{1, 2, 3}) {
                cacheApiTest(cacheMode, atomicityMode, backups0, false, nearSrv(f), nearClient(f));
                cacheApiTest(cacheMode, atomicityMode, backups0, true, nearSrv(f), nearClient(f));
            }
        }
    }

    /**
     * @param flag Flag.
     * @return {@code True} if near cache should be used on a client side.
     */
    private boolean nearClient(int flag) {
        return (flag & 0b01) == 0b01;
    }

    /**
     * @param flag Flag.
     * @return {@code True} if near cache should be used on a server side.
     */
    private boolean nearSrv(int flag) {
        return (flag & 0b10) == 0b10;
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Number of backups.
     * @param heapCache On heap cache flag.
     * @param nearSrv {@code True} if near cache should be used on a server side.
     * @param nearClient {@code True} if near cache should be used on a client side.
     * @throws Exception If failed.
     */
    private void cacheApiTest(CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups,
        boolean heapCache,
        boolean nearSrv,
        boolean nearClient) throws Exception {
        Ignite srv0 = ignite(0);

        NearCacheConfiguration nearCfg = nearSrv ? new NearCacheConfiguration() : null;

        srv0.createCache(cacheConfiguration(GROUP1, "cache-0", cacheMode, atomicityMode, backups, heapCache)
            .setNearConfiguration(nearCfg));

        srv0.createCache(cacheConfiguration(GROUP1, "cache-1", cacheMode, atomicityMode, backups, heapCache));

        srv0.createCache(cacheConfiguration(GROUP2, "cache-2", cacheMode, atomicityMode, backups, heapCache)
            .setNearConfiguration(nearCfg));

        srv0.createCache(cacheConfiguration(null, "cache-3", cacheMode, atomicityMode, backups, heapCache));

        if (nearClient) {
            Ignite clientNode = ignite(4);

            clientNode.createNearCache("cache-0", new NearCacheConfiguration());
            clientNode.createNearCache("cache-2", new NearCacheConfiguration());
        }

        try {
            for (final Ignite node : Ignition.allGrids()) {
                List<Callable<?>> ops = new ArrayList<>();

                for (int i = 0; i < 4; i++)
                    ops.add(testSet(node.cache("cache-" + i), cacheMode, atomicityMode, backups, heapCache, node));

                // Async operations.
                GridTestUtils.runMultiThreaded(ops, "cacheApiTest");
            }
        }
        finally {
            for (int i = 0; i < 4; i++)
                srv0.destroyCache("cache-" + i);
        }
    }

    /**
     * @param cache Cache.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Number of backups.
     * @param heapCache On heap cache flag.
     * @param node Ignite node.
     * @return Callable for the test operations.
     */
    private Callable<?> testSet(
        final IgniteCache<Object, Object> cache,
        final CacheMode cacheMode,
        final CacheAtomicityMode atomicityMode,
        final int backups,
        final boolean heapCache,
        final Ignite node) {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Test cache [node=" + node.name() +
                    ", cache=" + cache.getName() +
                    ", mode=" + cacheMode +
                    ", atomicity=" + atomicityMode +
                    ", backups=" + backups +
                    ", heapCache=" + heapCache +
                    ']');

                cacheApiTest(cache);

                return null;
            }
        };
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void cacheApiTest(IgniteCache cache) throws Exception {
        cachePutAllGetAllContainsAll(cache);

        cachePutAllGetAllContainsAllAsync(cache);

        cachePutRemove(cache);

        cachePutRemoveAsync(cache);

        cachePutGetContains(cache);

        cachePutGetContainsAsync(cache);

        cachePutGetAndPut(cache);

        cachePutGetAndPutAsync(cache);

        cachePutGetAndRemove(cache);

        cachePutGetAndRemoveAsync(cache);

        cachePutGetAndReplace(cache);

        cachePutGetAndReplaceAsync(cache);

        cachePutIfAbsent(cache);

        cachePutIfAbsentAsync(cache);

        cachePutGetAndPutIfAbsent(cache);

        cachePutGetAndPutIfAbsentAsync(cache);

        cacheQuery(cache);

        cacheInvokeAll(cache);

        cacheInvoke(cache);

        cacheInvokeAllAsync(cache);

        cacheInvokeAsync(cache);

        cacheDataStreamer(cache);
    }

    /**
     * @param cache Cache.
     */
    private void tearDown(IgniteCache cache) {
        cache.clear();

        cache.removeAll();
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void cacheDataStreamer(final IgniteCache cache) throws Exception {
        final int keys = 400;
        final int loaders = 4;

        final Integer[] data = generateData(keys * loaders);

        // Stream through a client node.
        Ignite clientNode = ignite(4);

        List<Callable<?>> cls = new ArrayList<>(loaders);

        for (final int i : sequence(loaders)) {
            final IgniteDataStreamer ldr = clientNode.dataStreamer(cache.getName());
            ldr.allowOverwrite(true); // TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11793
            ldr.autoFlushFrequency(0);

            cls.add(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    List<IgniteFuture> futs = new ArrayList<>(keys);

                    for (int j = 0, size = keys * loaders; j < size; j++) {
                        if (j % loaders == i)
                            futs.add(ldr.addData(j, data[j]));

                        if (j % (100 * loaders) == 0)
                            ldr.flush();
                    }

                    ldr.flush();

                    for (IgniteFuture fut : futs)
                        fut.get();

                    return null;
                }
            });
        }

        GridTestUtils.runMultiThreaded(cls, "loaders");

        Set<Integer> keysSet = sequence(data.length);

        for (Cache.Entry<Integer, Integer> entry : (IgniteCache<Integer, Integer>)cache) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutAllGetAllContainsAll(IgniteCache cache) {
        int keys = 100;

        Map<Integer, Integer> data = generateDataMap(keys);

        cache.putAll(data);

        Map data0 = cache.getAll(data.keySet());

        assertEquals(data.size(), data0.size());

        for (Map.Entry<Integer, Integer> entry : data.entrySet())
            assertEquals(entry.getValue(), data0.get(entry.getKey()));

        assertTrue(cache.containsKeys(data.keySet()));

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutAllGetAllContainsAllAsync(IgniteCache cache) {
        int keys = 100;

        Map<Integer, Integer> data = generateDataMap(keys);

        cache.putAllAsync(data).get(ASYNC_TIMEOUT);

        Map data0 = (Map)cache.getAllAsync(data.keySet()).get(ASYNC_TIMEOUT);

        assertEquals(data.size(), data0.size());

        for (Map.Entry<Integer, Integer> entry : data.entrySet())
            assertEquals(entry.getValue(), data0.get(entry.getKey()));

        assertTrue((Boolean)cache.containsKeysAsync(data.keySet()).get(ASYNC_TIMEOUT));

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutRemove(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.put(key, val);

        assertTrue(cache.remove(key));

        assertNull(cache.get(key));

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutRemoveAsync(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.putAsync(key, val).get(ASYNC_TIMEOUT);

        assertTrue((Boolean)cache.removeAsync(key).get(ASYNC_TIMEOUT));

        assertNull(cache.get(key));

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetContains(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.put(key, val);

        Object val0 = cache.get(key);

        assertEquals(val, val0);

        assertTrue(cache.containsKey(key));

        Integer key2 = rnd.nextInt();

        while (key2.equals(key))
            key2 = rnd.nextInt();

        assertFalse(cache.containsKey(key2));

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetContainsAsync(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.putAsync(key, val).get(ASYNC_TIMEOUT);

        Object val0 = cache.getAsync(key).get(ASYNC_TIMEOUT);

        assertEquals(val, val0);

        assertTrue((Boolean)cache.containsKeyAsync(key).get(ASYNC_TIMEOUT));

        Integer key2 = rnd.nextInt();

        while (key2.equals(key))
            key2 = rnd.nextInt();

        assertFalse((Boolean)cache.containsKeyAsync(key2).get(ASYNC_TIMEOUT));

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetAndPut(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val1 = rnd.nextInt();
        Integer val2 = rnd.nextInt();

        cache.put(key, val1);

        Object val0 = cache.getAndPut(key, val2);

        assertEquals(val1, val0);

        val0 = cache.get(key);

        assertEquals(val2, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetAndPutAsync(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val1 = rnd.nextInt();
        Integer val2 = rnd.nextInt();

        cache.put(key, val1);

        Object val0 = cache.getAndPutAsync(key, val2).get(ASYNC_TIMEOUT);

        assertEquals(val1, val0);

        val0 = cache.get(key);

        assertEquals(val2, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetAndReplace(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val1 = rnd.nextInt();
        Integer val2 = rnd.nextInt();

        Object val0 = cache.getAndReplace(key, val1);

        assertEquals(null, val0);

        val0 = cache.get(key);

        assertEquals(null, val0);

        cache.put(key, val1);

        val0 = cache.getAndReplace(key, val2);

        assertEquals(val1, val0);

        val0 = cache.get(key);

        assertEquals(val2, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetAndReplaceAsync(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val1 = rnd.nextInt();
        Integer val2 = rnd.nextInt();

        Object val0 = cache.getAndReplaceAsync(key, val1).get(ASYNC_TIMEOUT);

        assertEquals(null, val0);

        val0 = cache.get(key);

        assertEquals(null, val0);

        cache.put(key, val1);

        val0 = cache.getAndReplaceAsync(key, val2).get(ASYNC_TIMEOUT);

        assertEquals(val1, val0);

        val0 = cache.get(key);

        assertEquals(val2, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetAndRemove(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.put(key, val);

        Object val0 = cache.getAndRemove(key);

        assertEquals(val, val0);

        val0 = cache.get(key);

        assertNull(val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetAndRemoveAsync(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.put(key, val);

        Object val0 = cache.getAndRemoveAsync(key).get(ASYNC_TIMEOUT);

        assertEquals(val, val0);

        val0 = cache.get(key);

        assertNull(val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutIfAbsent(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val1 = rnd.nextInt();
        Integer val2 = rnd.nextInt();

        assertTrue(cache.putIfAbsent(key, val1));

        Object val0 = cache.get(key);

        assertEquals(val1, val0);

        assertFalse(cache.putIfAbsent(key, val2));

        val0 = cache.get(key);

        assertEquals(val1, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutIfAbsentAsync(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val1 = rnd.nextInt();
        Integer val2 = rnd.nextInt();

        assertTrue((Boolean)cache.putIfAbsentAsync(key, val1).get(ASYNC_TIMEOUT));

        Object val0 = cache.get(key);

        assertEquals(val1, val0);

        assertFalse((Boolean)cache.putIfAbsentAsync(key, val2).get(ASYNC_TIMEOUT));

        val0 = cache.get(key);

        assertEquals(val1, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetAndPutIfAbsent(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val1 = rnd.nextInt();
        Integer val2 = rnd.nextInt();

        cache.put(key, val1);

        Object val0 = cache.getAndPutIfAbsent(key, val2);

        assertEquals(val1, val0);

        val0 = cache.get(key);

        assertEquals(val1, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetAndPutIfAbsentAsync(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val1 = rnd.nextInt();
        Integer val2 = rnd.nextInt();

        cache.put(key, val1);

        Object val0 = cache.getAndPutIfAbsentAsync(key, val2).get(ASYNC_TIMEOUT);

        assertEquals(val1, val0);

        val0 = cache.get(key);

        assertEquals(val1, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cacheQuery(IgniteCache cache) {
        int keys = 100;

        Map<Integer, Integer> data = generateDataMap(keys);

        cache.putAll(data);

        ScanQuery<Integer, Integer> qry = new ScanQuery<>(new IgniteBiPredicate<Integer, Integer>() {
            @Override public boolean apply(Integer key, Integer val) {
                return key % 2 == 0;
            }
        });

        List<Cache.Entry<Integer, Integer>> all = cache.query(qry).getAll();

        assertEquals(all.size(), data.size() / 2);

        for (Cache.Entry<Integer, Integer> entry : all) {
            assertEquals(0, entry.getKey() % 2);
            assertEquals(entry.getValue(), data.get(entry.getKey()));
        }

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cacheInvokeAll(IgniteCache cache) {
        int keys = 100;
        Map<Integer, Integer> data = generateDataMap(keys);

        cache.putAll(data);

        Random rnd = ThreadLocalRandom.current();

        int one = rnd.nextInt();
        int two = rnd.nextInt();

        Map<Integer, CacheInvokeResult<Integer>> res = cache.invokeAll(data.keySet(), new CacheEntryProcessor<Integer, Integer, Integer>() {
            @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments) throws EntryProcessorException {
                Object expected = ((Map)arguments[0]).get(entry.getKey());

                assertEquals(expected, entry.getValue());

                // Some calculation.
                return (Integer)arguments[1] + (Integer)arguments[2];
            }
        }, data, one, two);

        assertEquals(keys, res.size());
        assertEquals(one + two, (Object)res.get(0).get());

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cacheInvoke(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.put(key, val);

        int one = rnd.nextInt();
        int two = rnd.nextInt();

        Object res = cache.invoke(key, new CacheEntryProcessor<Integer, Integer, Integer>() {
            @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments) throws EntryProcessorException {
                assertEquals(arguments[0], entry.getValue());

                // Some calculation.
                return (Integer)arguments[1] + (Integer)arguments[2];
            }
        }, val, one, two);

        assertEquals(one + two, res);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cacheInvokeAllAsync(IgniteCache cache) {
        int keys = 100;
        Map<Integer, Integer> data = generateDataMap(keys);

        cache.putAll(data);

        Random rnd = ThreadLocalRandom.current();

        int one = rnd.nextInt();
        int two = rnd.nextInt();

        Object res0 = cache.invokeAllAsync(data.keySet(), new CacheEntryProcessor<Integer, Integer, Integer>() {
            @Override public Integer process(MutableEntry<Integer, Integer> entry,
                Object... arguments) throws EntryProcessorException {
                Object expected = ((Map)arguments[0]).get(entry.getKey());

                assertEquals(expected, entry.getValue());

                // Some calculation.
                return (Integer)arguments[1] + (Integer)arguments[2];
            }
        }, data, one, two).get(ASYNC_TIMEOUT);

        Map<Integer, CacheInvokeResult<Integer>> res = (Map<Integer, CacheInvokeResult<Integer>>)res0;

        assertEquals(keys, res.size());
        assertEquals(one + two, (Object)res.get(0).get());

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cacheInvokeAsync(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.put(key, val);

        int one = rnd.nextInt();
        int two = rnd.nextInt();

        Object res = cache.invokeAsync(key, new CacheEntryProcessor<Integer, Integer, Integer>() {
            @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments) throws EntryProcessorException {
                assertEquals(arguments[0], entry.getValue());

                // Some calculation.
                return (Integer)arguments[1] + (Integer)arguments[2];
            }
        }, val, one, two).get(ASYNC_TIMEOUT);

        assertEquals(one + two, res);

        tearDown(cache);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheAtomicPartitioned() throws Exception {
        loadCache(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheAtomicReplicated() throws Exception {
        loadCache(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheTxPartitioned() throws Exception {
        loadCache(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7954")
    @Test
    public void testLoadCacheMvccTxPartitioned() throws Exception {
        loadCache(PARTITIONED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheTxReplicated() throws Exception {
        loadCache(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7954")
    @Test
    public void testLoadCacheMvccTxReplicated() throws Exception {
        loadCache(REPLICATED, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheAtomicLocal() throws Exception {
        loadCache(LOCAL, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadCacheTxLocal() throws Exception {
        loadCache(LOCAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9530")
    @Test
    public void testLoadCacheMvccTxLocal() throws Exception {
        loadCache(LOCAL, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private void loadCache(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 100;

        boolean loc = cacheMode == LOCAL;

        Map<Integer, Integer> data1 = generateDataMap(keys);
        Map<Integer, Integer> data2 = generateDataMap(keys);

        Factory<? extends CacheStore<Integer, Integer>> fctr1 =
            FactoryBuilder.factoryOf(new MapBasedStore<>(data1));

        Factory<? extends CacheStore<Integer, Integer>> fctr2 =
            FactoryBuilder.factoryOf(new MapBasedStore<>(data2));

        CacheConfiguration ccfg1 = cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 1, false)
            .setCacheStoreFactory(fctr1);

        CacheConfiguration ccfg2 = cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 1, false)
            .setCacheStoreFactory(fctr2);

        Ignite node = startGrids(loc ? 1 : 4);

        node.createCaches(F.asList(ccfg1, ccfg2));

        IgniteCache<Integer, Integer> cache1 = node.cache(CACHE1);
        IgniteCache<Integer, Integer> cache2 = node.cache(CACHE2);

        cache1.loadCache(null);

        checkCacheData(data1, CACHE1);

        assertEquals(0, cache2.size());

        cache2.loadCache(null);

        checkCacheData(data2, CACHE2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentOperationsSameKeys() throws Exception {
        final int SRVS = 4;
        final int CLIENTS = 4;
        final int NODES = SRVS + CLIENTS;

        startGrid(0);

        Ignite srv0 = startGridsMultiThreaded(1, SRVS - 1);

        startClientGridsMultiThreaded(SRVS, CLIENTS);

        srv0.createCache(cacheConfiguration(GROUP1, "a0", PARTITIONED, ATOMIC, 1, false));
        srv0.createCache(cacheConfiguration(GROUP1, "a1", PARTITIONED, ATOMIC, 1, false));
        srv0.createCache(cacheConfiguration(GROUP1, "t0", PARTITIONED, TRANSACTIONAL, 1, false));
        srv0.createCache(cacheConfiguration(GROUP1, "t1", PARTITIONED, TRANSACTIONAL, 1, false));

        final List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 50; i++)
            keys.add(i);

        final AtomicBoolean err = new AtomicBoolean();

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture fut1 = updateFuture(NODES, "a0", keys, false, stop, err);
        IgniteInternalFuture fut2 = updateFuture(NODES, "a1", keys, true, stop, err);
        IgniteInternalFuture fut3 = updateFuture(NODES, "t0", keys, false, stop, err);
        IgniteInternalFuture fut4 = updateFuture(NODES, "t1", keys, true, stop, err);

        try {
            for (int i = 0; i < 15 && !stop.get(); i++)
                U.sleep(1_000);
        }
        finally {
            stop.set(true);
        }

        fut1.get();
        fut2.get();
        fut3.get();
        fut4.get();

        assertFalse("Unexpected error, see log for details", err.get());
    }

    /**
     * @param nodes Total number of nodes.
     * @param cacheName Cache name.
     * @param keys Keys to update.
     * @param reverse {@code True} if update in reverse order.
     * @param stop Stop flag.
     * @param err Error flag.
     * @return Update future.
     */
    private IgniteInternalFuture updateFuture(final int nodes,
        final String cacheName,
        final List<Integer> keys,
        final boolean reverse,
        final AtomicBoolean stop,
        final AtomicBoolean err) {
        final AtomicInteger idx = new AtomicInteger();

        return GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    Ignite node = ignite(idx.getAndIncrement() % nodes);

                    log.info("Start thread [node=" + node.name() + ']');

                    IgniteCache cache = node.cache(cacheName);

                    Map<Integer, Integer> map = new LinkedHashMap<>();

                    if (reverse) {
                        for (int i = keys.size() - 1; i >= 0; i--)
                            map.put(keys.get(i), 2);
                    }
                    else {
                        for (Integer key : keys)
                            map.put(key, 1);
                    }

                    while (!stop.get())
                        cache.putAll(map);
                }
                catch (Exception e) {
                    err.set(true);

                    log.error("Unexpected error: " + e, e);

                    stop.set(true);
                }

                return null;
            }
        }, nodes * 2, "update-" + cacheName + "-" + reverse);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentOperationsAndCacheDestroy() throws Exception {
        final int SRVS = 4;
        final int CLIENTS = 4;
        final int NODES = SRVS + CLIENTS;

        startGrid(0);

        Ignite srv0 = startGridsMultiThreaded(1, SRVS - 1);

        startClientGridsMultiThreaded(SRVS, CLIENTS);

        final int CACHES = 8;

        final int grp1Backups = ThreadLocalRandom.current().nextInt(3);
        final int grp2Backups = ThreadLocalRandom.current().nextInt(3);

        log.info("Start test [grp1Backups=" + grp1Backups + ", grp2Backups=" + grp2Backups + ']');

        for (int i = 0; i < CACHES; i++) {
            srv0.createCache(
                cacheConfiguration(GROUP1, GROUP1 + "-" + i, PARTITIONED, ATOMIC, grp1Backups, i % 2 == 0));

            srv0.createCache(
                cacheConfiguration(GROUP2, GROUP2 + "-" + i, PARTITIONED, TRANSACTIONAL, grp2Backups, i % 2 == 0));

            // TODO IGNITE-7164: add Mvcc cache to test.
        }

        final AtomicInteger idx = new AtomicInteger();

        final AtomicBoolean err = new AtomicBoolean();

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture opFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    Ignite node = ignite(idx.getAndIncrement() % NODES);

                    log.info("Start thread [node=" + node.name() + ']');

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        try {
                            String grp = rnd.nextBoolean() ? GROUP1 : GROUP2;
                            int cacheIdx = rnd.nextInt(CACHES);

                            IgniteCache cache = node.cache(grp + "-" + cacheIdx);

                            for (int i = 0; i < 10; i++)
                                cacheOperation(rnd, cache);
                        }
                        catch (Exception e) {
                            if (X.hasCause(e, CacheStoppedException.class)) {
                                // Cache operation can be blocked on
                                // awaiting new topology version and cancelled with CacheStoppedException cause.

                                continue;
                            }

                            throw e;
                        }
                    }
                }
                catch (Exception e) {
                    err.set(true);

                    log.error("Unexpected error(1): " + e, e);

                    stop.set(true);
                }
            }
        }, (SRVS + CLIENTS) * 2, "op-thread");

        IgniteInternalFuture cacheFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                int cntr = 0;

                while (!stop.get()) {
                    try {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        String grp;
                        int backups;

                        if (rnd.nextBoolean()) {
                            grp = GROUP1;
                            backups = grp1Backups;
                        }
                        else {
                            grp = GROUP2;
                            backups = grp2Backups;
                        }

                        Ignite node = ignite(rnd.nextInt(NODES));

                        log.info("Create cache [node=" + node.name() + ", grp=" + grp + ']');

                        IgniteCache cache = node.createCache(cacheConfiguration(grp, "tmpCache-" + cntr++,
                            PARTITIONED,
                            rnd.nextBoolean() ? ATOMIC : TRANSACTIONAL,
                            backups,
                            rnd.nextBoolean()));

                        for (int i = 0; i < 10; i++)
                            cacheOperation(rnd, cache);

                        log.info("Destroy cache [node=" + node.name() + ", grp=" + grp + ']');

                        node.destroyCache(cache.getName());
                    }
                    catch (Exception e) {
                        if (X.hasCause(e, CacheStoppedException.class)) {
                            // Cache operation can be blocked on
                            // awaiting new topology version and cancelled with CacheStoppedException cause.

                            continue;
                        }

                        err.set(true);

                        log.error("Unexpected error(2): " + e, e);

                        stop.set(true);
                    }
                }
            }
        }, "cache-destroy-thread");

        try {
            for (int i = 0; i < 30 && !stop.get(); i++)
                U.sleep(1_000);
        }
        finally {
            stop.set(true);
        }

        opFut.get();
        cacheFut.get();

        assertFalse("Unexpected error, see log for details", err.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStaticConfigurationsValidation() throws Exception {
        ccfgs = new CacheConfiguration[2];

        ccfgs[0] = new CacheConfiguration(CACHE1);
        ccfgs[0].setGroupName(GROUP1);
        ccfgs[0].setAffinity(new RendezvousAffinityFunction(false, 1024));

        ccfgs[1] = new CacheConfiguration(CACHE2);
        ccfgs[1].setGroupName(GROUP1);
        ccfgs[1].setAffinity(new RendezvousAffinityFunction(false, 512));

        try {
            startGrid(0);

            fail();
        }
        catch (IgniteCheckedException ignore) {
            // Expected exception.
        }

        ccfgs = new CacheConfiguration[3];

        ccfgs[0] = new CacheConfiguration(CACHE1);
        ccfgs[0].setGroupName(GROUP1);
        ccfgs[0].setAffinity(new RendezvousAffinityFunction(false, 16));

        ccfgs[1] = new CacheConfiguration(CACHE2);
        ccfgs[1].setGroupName(GROUP2);
        ccfgs[1].setAffinity(new RendezvousAffinityFunction(false, 512));

        ccfgs[2] = new CacheConfiguration("cache3");
        ccfgs[2].setGroupName(GROUP1);
        ccfgs[2].setAffinity(new RendezvousAffinityFunction(false, 16));

        startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdConflict() throws Exception {
        ccfgs = new CacheConfiguration[]{new CacheConfiguration("AaAaAa"), new CacheConfiguration("AaAaBB")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration("AaAaAa")};

        startGrid(0);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration("AaAaBB")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertFalse(ignite(0).cacheNames().contains("AaAaBB"));

        final Ignite ignite1 = startGrid(1);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite1.createCache(new CacheConfiguration("AaAaBB"));

                return null;
            }
        }, CacheException.class, null);

        assertFalse(ignite(0).cacheNames().contains("AaAaBB"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupIdConflict1() throws Exception {
        ccfgs = new CacheConfiguration[]{new CacheConfiguration(CACHE1).setGroupName("AaAaAa"),
            new CacheConfiguration(CACHE2).setGroupName("AaAaBB")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration(CACHE1).setGroupName("AaAaAa")};

        startGrid(0);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration(CACHE2).setGroupName("AaAaBB")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertFalse(ignite(0).cacheNames().contains(CACHE2));

        final Ignite ignite1 = startGrid(1);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite1.createCache(new CacheConfiguration(CACHE2).setGroupName("AaAaBB"));

                return null;
            }
        }, CacheException.class, null);

        assertFalse(ignite(0).cacheNames().contains(CACHE2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupIdConflict2() throws Exception {
        ccfgs = new CacheConfiguration[]{new CacheConfiguration("AaAaAa"),
            new CacheConfiguration(CACHE2).setGroupName("AaAaBB")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration("AaAaAa")};

        startGrid(0);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration(CACHE2).setGroupName("AaAaBB")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertFalse(ignite(0).cacheNames().contains(CACHE2));

        final Ignite ignite1 = startGrid(1);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite1.createCache(new CacheConfiguration(CACHE2).setGroupName("AaAaBB"));

                return null;
            }
        }, CacheException.class, null);

        assertFalse(ignite(0).cacheNames().contains(CACHE2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupIdConflict3() throws Exception {
        ccfgs = new CacheConfiguration[]{new CacheConfiguration(CACHE2).setGroupName("AaAaBB"),
            new CacheConfiguration("AaAaAa")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration(CACHE2).setGroupName("AaAaBB")};

        startGrid(0);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration("AaAaAa")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertFalse(ignite(0).cacheNames().contains("AaAaAa"));

        final Ignite ignite1 = startGrid(1);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite1.createCache(new CacheConfiguration("AaAaAa"));

                return null;
            }
        }, CacheException.class, null);

        assertFalse(ignite(0).cacheNames().contains("AaAaAa"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupNameConflict1() throws Exception {
        ccfgs = new CacheConfiguration[]{new CacheConfiguration("cache1"), new CacheConfiguration("cache2").setGroupName("cache1")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration("cache1")};

        startGrid(0);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration("cache2").setGroupName("cache1")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertFalse(ignite(0).cacheNames().contains("cache2"));

        final Ignite ignite1 = startGrid(1);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite1.createCache(new CacheConfiguration("cache2").setGroupName("cache1"));

                return null;
            }
        }, CacheException.class, null);

        assertFalse(ignite(0).cacheNames().contains("cache2"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupNameConflict2() throws Exception {
        ccfgs = new CacheConfiguration[]{new CacheConfiguration("cache2").setGroupName("cache1"), new CacheConfiguration("cache1")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration("cache2").setGroupName("cache1")};

        startGrid(0);

        ccfgs = new CacheConfiguration[]{new CacheConfiguration("cache1")};

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertFalse(ignite(0).cacheNames().contains("cache1"));

        final Ignite ignite1 = startGrid(1);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite1.createCache(new CacheConfiguration("cache1"));

                return null;
            }
        }, CacheException.class, null);

        assertFalse(ignite(0).cacheNames().contains("cache1"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfigurationConsistencyValidation() throws Exception {
        startGrids(2);

        startClientGrid(2);

        ignite(0).createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1, false));

        for (int i = 0; i < 3; i++) {
            try {
                ignite(i).createCache(cacheConfiguration(GROUP1, "c2", REPLICATED, ATOMIC, Integer.MAX_VALUE, false));

                fail();
            }
            catch (CacheException e) {
                assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Cache mode mismatch for caches related to the same group [groupName=grp1"));
            }

            try {
                ignite(i).createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 2, false));

                fail();
            }
            catch (CacheException e) {
                assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Backups mismatch for caches related to the same group [groupName=grp1"));
            }

            try {
                ignite(i).createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, TRANSACTIONAL_SNAPSHOT, 1, false));

                fail();
            }
            catch (CacheException e) {
                assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Atomicity mode mismatch for caches related to the same group [groupName=grp1"));
            }
        }
    }

    /**
     * @return Cache configurations.
     */
    private CacheConfiguration[] interceptorConfigurations() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[6];

        ccfgs[0] = cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 2, false).setInterceptor(new Interceptor1());
        ccfgs[1] = cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 2, false).setInterceptor(new Interceptor2());
        ccfgs[2] = cacheConfiguration(GROUP1, "c3", PARTITIONED, TRANSACTIONAL, 2, false).setInterceptor(new Interceptor1());
        ccfgs[3] = cacheConfiguration(GROUP1, "c4", PARTITIONED, TRANSACTIONAL, 2, false).setInterceptor(new Interceptor2());
        ccfgs[4] = cacheConfiguration(GROUP1, "c5", PARTITIONED, ATOMIC, 2, false);
        ccfgs[5] = cacheConfiguration(GROUP1, "c6", PARTITIONED, TRANSACTIONAL, 2, false);

        //TODO IGNITE-9323: Check Mvcc mode.

        return ccfgs;
    }

    /**
     * Tests caches in the same group with different {@link CacheInterceptor}s.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInterceptors() throws Exception {
        for (int i = 0; i < 4; i++) {
            ccfgs = interceptorConfigurations();

            startGrid(i);
        }

        Ignite node = ignite(0);

        checkInterceptorPut(node.cache("c1"), "v1");
        checkInterceptorPut(node.cache("c2"), "v2");
        checkInterceptorPut(node.cache("c3"), "v1");
        checkInterceptorPut(node.cache("c4"), "v2");

        checkCache(0, "c5", 10);
        checkCache(0, "c6", 10);
    }

    /**
     * @param cache Cache.
     * @param expVal Expected value.
     */
    private void checkInterceptorPut(IgniteCache cache, String expVal) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10; i++) {
            Integer key = rnd.nextInt();

            cache.put(key, i);

            assertEquals(expVal, cache.get(key));
        }
    }

    /**
     * @return Cache configurations.
     */
    private CacheConfiguration[] cacheStoreConfigurations() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[6];

        ccfgs[0] = cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 2, false).
            setCacheStoreFactory(new StoreFactory1()).setReadThrough(true).setWriteThrough(true);

        ccfgs[1] = cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 2, false).
            setCacheStoreFactory(new StoreFactory2()).setReadThrough(true).setWriteThrough(true);

        ccfgs[2] = cacheConfiguration(GROUP1, "c3", PARTITIONED, TRANSACTIONAL, 2, false).
            setCacheStoreFactory(new StoreFactory1()).setReadThrough(true).setWriteThrough(true);

        ccfgs[3] = cacheConfiguration(GROUP1, "c4", PARTITIONED, TRANSACTIONAL, 2, false).
            setCacheStoreFactory(new StoreFactory2()).setReadThrough(true).setWriteThrough(true);

        ccfgs[4] = cacheConfiguration(GROUP1, "c5", PARTITIONED, ATOMIC, 2, false);
        ccfgs[5] = cacheConfiguration(GROUP1, "c6", PARTITIONED, TRANSACTIONAL, 2, false);

        //TODO IGNITE-8582: Check Mvcc mode.

        return ccfgs;
    }

    /**
     * Tests caches in the same group with different {@link CacheStore}s.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStores() throws Exception {
        for (int i = 0; i < 4; i++) {
            ccfgs = cacheStoreConfigurations();

            startGrid(i);
        }

        Ignite node = ignite(0);

        checkStorePut(node.cache("c1"), Store1.map);
        assertTrue(Store2.map.isEmpty());

        checkStorePut(node.cache("c3"), Store1.map);
        assertTrue(Store2.map.isEmpty());

        Store1.map.clear();

        checkStorePut(node.cache("c2"), Store2.map);
        assertTrue(Store1.map.isEmpty());

        checkStorePut(node.cache("c4"), Store2.map);
        assertTrue(Store1.map.isEmpty());

        Store2.map.clear();

        checkCache(0, "c5", 10);
        checkCache(0, "c6", 10);

        assertTrue(Store1.map.isEmpty());
        assertTrue(Store2.map.isEmpty());
    }

    /**
     * @param cache Cache.
     * @param storeMap Cache store data.
     */
    private void checkStorePut(IgniteCache cache, ConcurrentHashMap<Object, Object> storeMap) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10; i++) {
            Integer key = rnd.nextInt();

            storeMap.put(key, i);

            assertEquals(i, cache.get(key));

            cache.put(key, 10_000);

            assertEquals(10_000, cache.get(key));
            assertEquals(10_000, storeMap.get(key));
        }
    }

    /**
     * @return Cache configurations.
     */
    private CacheConfiguration[] mapperConfigurations() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[6];

        ccfgs[0] = cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 2, false).setAffinityMapper(new Mapper1());
        ccfgs[1] = cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 2, false).setAffinityMapper(new Mapper2());
        ccfgs[2] = cacheConfiguration(GROUP1, "c3", PARTITIONED, TRANSACTIONAL, 2, false).setAffinityMapper(new Mapper1());
        ccfgs[3] = cacheConfiguration(GROUP1, "c4", PARTITIONED, TRANSACTIONAL, 2, false).setAffinityMapper(new Mapper2());
        ccfgs[4] = cacheConfiguration(GROUP1, "c5", PARTITIONED, ATOMIC, 2, false);
        ccfgs[5] = cacheConfiguration(GROUP1, "c6", PARTITIONED, TRANSACTIONAL, 2, false);

        return ccfgs;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityMappers() throws Exception {
        for (int i = 0; i < 4; i++) {
            ccfgs = mapperConfigurations();

            startGrid(i);
        }

        for (int i = 0; i < 4; i++)
            checkAffinityMappers(ignite(i));

        startClientGrid(4);

        checkAffinityMappers(ignite(4));

        for (int i = 0; i < 5; i++) {
            checkCache(i, "c1", 10);
            checkCache(i, "c2", 10);
            checkCache(i, "c3", 10);
            checkCache(i, "c4", 10);
            checkCache(i, "c5", 10);
            checkCache(i, "c6", 10);
        }
    }

    /**
     * @param node Node.
     */
    private void checkAffinityMappers(Ignite node) {
        Affinity aff1 = node.affinity("c1");
        Affinity aff2 = node.affinity("c2");
        Affinity aff3 = node.affinity("c3");
        Affinity aff4 = node.affinity("c4");
        Affinity aff5 = node.affinity("c5");
        Affinity aff6 = node.affinity("c6");

        RendezvousAffinityFunction func = new RendezvousAffinityFunction();

        for (int i = 0; i < 100; i++) {
            MapperTestKey1 k = new MapperTestKey1(i, i + 10);

            assertEquals(i, aff1.partition(k));
            assertEquals(i, aff3.partition(k));
            assertEquals(i + 10, aff2.partition(k));
            assertEquals(i + 10, aff4.partition(k));

            int part;

            if (node.configuration().getMarshaller() instanceof BinaryMarshaller)
                part = func.partition(node.binary().toBinary(k));
            else
                part = func.partition(k);

            assertEquals(part, aff5.partition(k));
            assertEquals(part, aff6.partition(k));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueriesMultipleGroups1() throws Exception {
        continuousQueriesMultipleGroups(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueriesMultipleGroups2() throws Exception {
        continuousQueriesMultipleGroups(4);
    }

    /**
     * @param srvs Number of server nodes.
     * @throws Exception If failed.
     */
    private void continuousQueriesMultipleGroups(int srvs) throws Exception {
        Ignite srv0 = startGrids(srvs);

        Ignite client = startClientGrid(srvs);

        client.createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1, false));
        client.createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, TRANSACTIONAL, 1, false));
        client.createCache(cacheConfiguration(GROUP1, "c3", PARTITIONED, ATOMIC, 1, false));

        client.createCache(cacheConfiguration(GROUP2, "c4", PARTITIONED, TRANSACTIONAL, 1, false));
        client.createCache(cacheConfiguration(GROUP2, "c5", PARTITIONED, ATOMIC, 1, false));
        client.createCache(cacheConfiguration(GROUP2, "c6", PARTITIONED, TRANSACTIONAL, 1, false));

        client.createCache(cacheConfiguration(null, "c7", PARTITIONED, ATOMIC, 1, false));
        client.createCache(cacheConfiguration(null, "c8", PARTITIONED, TRANSACTIONAL, 1, false));

        client.createCache(cacheConfiguration(GROUP3, "c9", PARTITIONED, TRANSACTIONAL_SNAPSHOT, 1, false));

        String[] cacheNames = {"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"};

        AtomicInteger c1 = registerListener(client, "c1");

        for (String cache : cacheNames)
            srv0.cache(cache).put(1, 1);

        waitForEvents(c1, 1);

        for (String cache : cacheNames)
            srv0.cache(cache).put(1, 1);

        waitForEvents(c1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdSort() throws Exception {
        Ignite node = startGrid(0);

        final List<IgniteCache> caches = new ArrayList<>(3);

        caches.add(node.createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1, false)
            .setAffinity(new RendezvousAffinityFunction(false, 8))));
        caches.add(node.createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 1, false)
            .setAffinity(new RendezvousAffinityFunction(false, 8))));
        caches.add(node.createCache(cacheConfiguration(GROUP1, "c3", PARTITIONED, ATOMIC, 1, false)
            .setAffinity(new RendezvousAffinityFunction(false, 8))));

        Affinity aff = node.affinity("c1");

        final List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 1_000_000; i++) {
            if (aff.partition(i) == 0) {
                keys.add(i);

                if (keys.size() >= 10_000)
                    break;
            }
        }

        assertEquals(10_000, keys.size());

        final long stopTime = System.currentTimeMillis() + 10_000;

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (System.currentTimeMillis() < stopTime) {
                    for (int i = 0; i < 100; i++) {
                        IgniteCache cache = caches.get(rnd.nextInt(3));

                        Integer key = keys.get(rnd.nextInt(10_000));

                        if (rnd.nextFloat() > 0.8f)
                            cache.remove(key);
                        else
                            cache.put(key, key);
                    }
                }

                return null;
            }
        }, 5, "update-thread");

        CacheGroupContext grp = cacheGroup(node, GROUP1);

        Integer cacheId = null;

        GridIterator<CacheDataRow> it = grp.offheap().partitionIterator(0);

        int c = 0;

        while (it.hasNext()) {
            CacheDataRow row = it.next();

            if (cacheId == null || cacheId != row.cacheId()) {
                cacheId = row.cacheId();

                c++;
            }
        }

        assertEquals(3, c);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataCleanup() throws Exception {
        Ignite node = startGrid(0);

        IgniteCache cache0 = node.createCache(cacheConfiguration(GROUP1, "c0", PARTITIONED, ATOMIC, 1, false));

        for (int i = 0; i < 100; i++)
            assertNull(cache0.get(i));

        for (int i = 0; i < 100; i++)
            cache0.put(i, i);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1, false));
        ccfgs.add(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1, true));
        ccfgs.add(cacheConfiguration(GROUP1, "c1", PARTITIONED, TRANSACTIONAL, 1, false));
        ccfgs.add(cacheConfiguration(GROUP1, "c1", PARTITIONED, TRANSACTIONAL, 1, true));

        for (CacheConfiguration ccfg : ccfgs) {
            IgniteCache cache = node.createCache(ccfg);

            for (int i = 0; i < 100; i++)
                assertNull(cache.get(i));

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            for (int i = 0; i < 100; i++)
                assertEquals(i, cache.get(i));

            node.destroyCache(ccfg.getName());

            cache = node.createCache(ccfg);

            for (int i = 0; i < 100; i++)
                assertNull(cache.get(i));

            node.destroyCache(ccfg.getName());
        }

        for (int i = 0; i < 100; i++)
            assertEquals(i, cache0.get(i));

        node.destroyCache(cache0.getName());

        cache0 = node.createCache(cacheConfiguration(GROUP1, "c0", PARTITIONED, ATOMIC, 1, false));

        for (int i = 0; i < 100; i++)
            assertNull(cache0.get(i));

        for (int i = 0; i < 100; i++)
            cache0.put(i, i);

        for (int i = 0; i < 100; i++)
            assertEquals(i, cache0.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartsAndCacheCreateDestroy() throws Exception {
        final int SRVS = 5;

        startGrids(SRVS);

        final Ignite clientNode = startClientGrid(SRVS);

        final int CACHES = SF.applyLB(10, 2);

        final AtomicReferenceArray<IgniteCache> caches = new AtomicReferenceArray<>(CACHES);

        for (int i = 0; i < CACHES; i++) {
            CacheAtomicityMode atomicityMode = i % 2 == 0 ? ATOMIC : TRANSACTIONAL;

            caches.set(i,
                clientNode.createCache(cacheConfiguration(GROUP1, "c" + i, PARTITIONED, atomicityMode, 0, false)));
        }

        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicInteger cacheCntr = new AtomicInteger();

        try {
            final int ITERATIONS_COUNT = SF.applyLB(10, 1);
            for (int i = 0; i < ITERATIONS_COUNT; i++) {
                stop.set(false);

                final AtomicReference<Exception> err = new AtomicReference<>();

                log.info("Iteration: " + i);

                IgniteInternalFuture<?> restartFut = GridTestUtils.runAsync(new Runnable() {
                    @Override public void run() {
                        try {
                            ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            while (!stop.get()) {
                                int node = rnd.nextInt(SRVS);

                                log.info("Stop node: " + node);

                                stopGrid(node);

                                U.sleep(500);

                                log.info("Start node: " + node);

                                startGrid(node);

                                try {
                                    if (rnd.nextBoolean())
                                        awaitPartitionMapExchange();
                                }
                                catch (Exception ignore) {
                                    // No-op.
                                }
                            }
                        }
                        catch (Exception e) {
                            log.error("Unexpected error: " + e, e);

                            err.set(e);

                            stop.set(true);
                        }
                    }
                });

                IgniteInternalFuture<?> cacheFut = GridTestUtils.runAsync(new Runnable() {
                    @Override public void run() {
                        try {
                            ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            while (!stop.get()) {
                                int idx = rnd.nextInt(CACHES);

                                IgniteCache cache = caches.get(idx);

                                if (cache != null && caches.compareAndSet(idx, cache, null)) {
                                    log.info("Destroy cache: " + cache.getName());

                                    clientNode.destroyCache(cache.getName());

                                    CacheAtomicityMode atomicityMode = rnd.nextBoolean() ? ATOMIC : TRANSACTIONAL;

                                    String name = "newName-" + cacheCntr.incrementAndGet();

                                    cache = clientNode.createCache(
                                        cacheConfiguration(GROUP1, name, PARTITIONED, atomicityMode, 0, false));

                                    caches.set(idx, cache);
                                }
                            }
                        }
                        catch (Exception e) {
                            log.error("Unexpected error: " + e, e);

                            err.set(e);

                            stop.set(true);
                        }
                    }
                });

                IgniteInternalFuture opFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                    @Override public void run() {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (!stop.get()) {
                            try {
                                int idx = rnd.nextInt(CACHES);

                                IgniteCache cache = caches.get(idx);

                                if (cache != null && caches.compareAndSet(idx, cache, null)) {
                                    try {
                                        for (int i = 0; i < 10; i++)
                                            cacheOperation(rnd, cache);
                                    }
                                    catch (Exception e) {
                                        if (X.hasCause(e, CacheStoppedException.class)) {
                                            // Cache operation can be blocked on
                                            // awaiting new topology version and cancelled with CacheStoppedException cause.

                                            continue;
                                        }

                                        throw e;
                                    }
                                    finally {
                                        caches.set(idx, cache);
                                    }
                                }
                            }
                            catch (Exception e) {
                                err.set(e);

                                log.error("Unexpected error: " + e, e);

                                stop.set(true);
                            }
                        }
                    }
                }, 8, "op-thread");

                Thread.sleep(SF.applyLB(10_000, 1_000));

                stop.set(true);

                restartFut.get();
                cacheFut.get();
                opFut.get();

                assertNull("Unexpected error during test, see log for details", err.get());

                awaitPartitionMapExchange();

                Set<Integer> cacheIds = new HashSet<>();

                for (int c = 0; c < CACHES; c++) {
                    IgniteCache cache = caches.get(c);

                    assertNotNull(cache);

                    assertTrue(cacheIds.add(CU.cacheId(cache.getName())));
                }

                for (int n = 0; n < SRVS; n++) {
                    CacheGroupContext grp = cacheGroup(ignite(n), GROUP1);

                    assertNotNull(grp);

                    for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
                        IntMap<Object> cachesMap = GridTestUtils.getFieldValue(part, "cacheMaps");

                        assertTrue(cachesMap.size() <= cacheIds.size());

                        cachesMap.forEach((cacheId, v) -> assertTrue(cachesMap.containsKey(cacheId)));
                    }
                }
            }
        }
        finally {
            stop.set(true);
        }
    }

    /**
     * @param cntr Counter.
     * @param expEvts Expected events number.
     * @throws Exception If failed.
     */
    private void waitForEvents(final AtomicInteger cntr, final int expEvts) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                if (cntr.get() < expEvts)
                    log.info("Wait for events [rcvd=" + cntr.get() + ", exp=" + expEvts + ']');

                return false;
            }
        }, 5000);

        assertEquals(expEvts, cntr.get());
        assertTrue(cntr.compareAndSet(expEvts, 0));
    }

    /**
     * @param node Node.
     * @param cacheName Cache name.
     * @return Received events counter.
     */
    private AtomicInteger registerListener(Ignite node, String cacheName) {
        ContinuousQuery qry = new ContinuousQuery();

        final AtomicInteger cntr = new AtomicInteger();

        qry.setLocalListener(new CacheEntryUpdatedListener() {
            @Override public void onUpdated(Iterable iterable) {
                for (Object evt : iterable)
                    cntr.incrementAndGet();
            }
        });

        node.cache(cacheName).query(qry);

        return cntr;
    }

    /**
     *
     */
    static class Mapper1 implements AffinityKeyMapper {
        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            if (key instanceof MapperTestKey1)
                return ((MapperTestKey1)key).p1;
            else if (key instanceof BinaryObject)
                ((BinaryObject) key).field("p1");

            return key;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }
    }

    /**
     *
     */
    static class Mapper2 implements AffinityKeyMapper {
        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            if (key instanceof MapperTestKey1)
                return ((MapperTestKey1)key).p2;
            else if (key instanceof BinaryObject)
                ((BinaryObject) key).field("p2");

            return key;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }
    }

    /**
     *
     */
    static class MapperTestKey1 {
        /** */
        final int p1;

        /** */
        final int p2;

        /**
         * @param p1 Field1.
         * @param p2 Field2.
         */
        public MapperTestKey1(int p1, int p2) {
            this.p1 = p1;
            this.p2 = p2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            MapperTestKey1 testKey1 = (MapperTestKey1)o;

            return p1 == testKey1.p1 && p2 == testKey1.p2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = p1;
            res = 31 * res + p2;
            return res;
        }
    }

    /**
     *
     */
    static class Interceptor1 extends CacheInterceptorAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Object onBeforePut(Cache.Entry<Object, Object> entry, Object newVal) {
            return "v1";
        }
    }

    /**
     *
     */
    static class Interceptor2 extends CacheInterceptorAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Object onBeforePut(Cache.Entry<Object, Object> entry, Object newVal) {
            return "v2";
        }
    }

    /**
     *
     */
    static class StoreFactory1 implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new Store1();
        }
    }

    /**
     *
     */
    static class Store1 extends CacheStoreAdapter {
        /** */
        static ConcurrentHashMap<Object, Object> map = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
            map.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            map.remove(key);
        }
    }

    /**
     *
     */
    static class StoreFactory2 implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new Store2();
        }
    }

    /**
     *
     */
    static class Store2 extends CacheStoreAdapter {
        /** */
        static ConcurrentHashMap<Object, Object> map = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
            map.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            map.remove(key);
        }
    }

    /**
     * @param rnd Random.
     * @param cache Cache.
     */
    private void cacheOperation(ThreadLocalRandom rnd, IgniteCache cache) {
        final int KEYS = 10_000;

        Integer key = rnd.nextInt(KEYS);

        switch (rnd.nextInt(6)) {
            case 0:
                cache.put(key, 1);

                break;

            case 1:
                cache.get(key);

                break;

            case 2:
                cache.remove(key);

                break;

            case 3:
                cache.localPeek(key);

                break;

            case 4:
                Set<Integer> keys = new HashSet<>();

                for (int i = 0; i < 5; i++)
                    keys.add(rnd.nextInt(KEYS));

                cache.getAll(keys);

                break;

            case 5:
                Map<Integer, Integer> map = new TreeMap<>();

                for (int i = 0; i < 5; i++)
                    map.put(rnd.nextInt(KEYS), i);

                cache.putAll(map);

                break;
        }
    }

    /**
     *
     */
    static class Key1 implements Serializable {
        /** */
        private int id;

        /**
         * @param id ID.
         */
        Key1(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key1 key = (Key1)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class Key2 implements Serializable {
        /** */
        private int id;

        /**
         * @param id ID.
         */
        Key2(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key2 key = (Key2)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class Value1 implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public Value1(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value1 val1 = (Value1)o;

            return val == val1.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     *
     */
    static class Value2 implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public Value2(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value2 val2 = (Value2)o;

            return val == val2.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     * @param grpName Cache group name.
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Backups number.
     * @param heapCache On heap cache flag.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(
        String grpName,
        String name,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups,
        boolean heapCache
    ) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(grpName);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setOnheapCacheEnabled(heapCache);

        return ccfg;
    }

    /**
     * @param idx Node index.
     * @param grpName Cache group name.
     * @param expGrp {@code True} if cache group should be created.
     * @throws IgniteCheckedException If failed.
     */
    private void checkCacheGroup(int idx, final String grpName, final boolean expGrp) throws IgniteCheckedException {
        final IgniteKernal node = (IgniteKernal)ignite(idx);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return expGrp == (cacheGroup(node, grpName) != null);
            }
        }, 1000));

        assertNotNull(node.context().cache().cache(CU.UTILITY_CACHE_NAME));
    }

    /**
     * @param node Node.
     * @param grpName Cache group name.
     * @return Cache group.
     */
    private CacheGroupContext cacheGroup(Ignite node, String grpName) {
        for (CacheGroupContext grp : ((IgniteKernal)node).context().cache().cacheGroups()) {
            if (grpName.equals(grp.name()))
                return grp;
        }

        return null;
    }

    /**
     *
     */
    static class MapBasedStore<K,V> implements CacheStore<K,V>, Serializable {
        /** */
        private final Map<K, V> src;

        /**
         * @param src Source map.
         */
        MapBasedStore(Map<K, V> src) {
            this.src = src;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) throws CacheLoaderException {
            for (Map.Entry<K, V> e : src.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public V load(K key) throws CacheLoaderException {
            return src.get(key);
        }

        /** {@inheritDoc} */
        @Override public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
            return F.view(src, new ContainsPredicate<>(Sets.newHashSet(keys)));
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            throw new UnsupportedOperationException();
        }
    }
}
