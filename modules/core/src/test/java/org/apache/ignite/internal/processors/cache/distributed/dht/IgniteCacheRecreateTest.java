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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests handling of ppending cache messages/operations when the required cache was re created.
 * */
public class IgniteCacheRecreateTest extends GridCommonAbstractTest {
    /** Cache name to be used in tests. */
    private static final String CACHE_NAME = "test-recreate-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);

        startClientGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid(0).destroyCache(CACHE_NAME);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicPutAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            ATOMIC,
            GridNearAtomicAbstractUpdateRequest.class,
            (cache, keys) -> cache.put(keys.get(0), 42));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicGetAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            ATOMIC,
            GridNearSingleGetRequest.class,
            (cache, keys) -> cache.get(keys.get(0)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicPutAllAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            ATOMIC,
            GridNearAtomicFullUpdateRequest.class,
            (cache, keys) -> {
                Map<Integer, Integer> vals = new TreeMap<>();
                vals.put(keys.get(0), 24);
                vals.put(keys.get(1), 42);

                cache.putAll(vals);
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicGetAllAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            ATOMIC,
            GridNearGetRequest.class,
            (cache, keys) -> {
                Set<Integer> vals = new TreeSet<>();
                vals.add(keys.get(0));
                vals.add(keys.get(1));

                cache.getAll(vals);
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testImplicitInvokeAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            ATOMIC,
            GridNearAtomicAbstractUpdateRequest.class,
            (cache, keys) -> {
                cache.invoke(keys.get(0), new CacheEntryProcessor<Integer, Integer, Integer>() {
                    @Override public Integer process(
                        MutableEntry<Integer, Integer> entry,
                        Object... arguments
                    ) throws EntryProcessorException {
                        if (entry.exists())
                            return entry.getValue();

                        entry.setValue(123);

                        return entry.getValue();
                    }
                });
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testImplicitOptimisticTxPutAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            TRANSACTIONAL,
            GridNearTxPrepareRequest.class,
            (cache, keys) -> cache.put(keys.get(0), 42));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testImplicitOptimisticTxGetAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            TRANSACTIONAL,
            GridNearSingleGetRequest.class,
            (cache, keys) -> cache.get(keys.get(0)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testImplicitOptimisticTxPutAllAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            TRANSACTIONAL,
            GridNearTxPrepareRequest.class,
            (cache, keys) -> {
                Map<Integer, Integer> vals = new TreeMap<>();
                vals.put(keys.get(0), 24);
                vals.put(keys.get(1), 42);

                cache.putAll(vals);
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testImplicitTxInvokeAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            TRANSACTIONAL,
            GridNearTxPrepareRequest.class,
            (cache, keys) -> {
                cache.invoke(keys.get(0), new CacheEntryProcessor<Integer, Integer, Integer>() {
                    @Override public Integer process(
                        MutableEntry<Integer, Integer> entry,
                        Object... arguments
                    ) throws EntryProcessorException {
                        if (entry.exists())
                            return entry.getValue();

                        entry.setValue(123);

                        return entry.getValue();
                    }
                });
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxPutAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            TRANSACTIONAL,
            GridNearLockRequest.class,
            (cache, keys) -> {
                try (Transaction tx = grid(1).transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
                    cache.put(keys.get(0), 42);

                    tx.commit();
                }
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxPutAllAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            TRANSACTIONAL,
            GridNearLockRequest.class,
            (cache, keys) -> {
                try (Transaction tx = grid(1).transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
                    Map<Integer, Integer> vals = new TreeMap<>();
                    vals.put(keys.get(0), 24);
                    vals.put(keys.get(1), 42);

                    cache.putAll(vals);

                    tx.commit();
                }
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxGetAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            TRANSACTIONAL,
            GridNearLockRequest.class,
            (cache, keys) -> {
                try (Transaction tx = grid(1).transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
                    cache.get(keys.get(0));

                    tx.commit();
                }
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicScanAndCacheRecreate() throws Exception {
        testCacheOperationAndCacheRecreate(
            ATOMIC,
            GridCacheQueryRequest.class,
            (cache, keys) -> {
                ScanQuery<Integer, Integer> scanQuery = new ScanQuery<>();
                scanQuery.setPageSize(1);

                try (QueryCursor qry = cache.query(scanQuery)) {
                    for (Object o : qry.getAll()) {
                        IgniteBiTuple<Integer, Integer> tuple = (IgniteBiTuple<Integer, Integer>)o;

                        throw new RuntimeException("Succesfully read unexpected value [k=" + tuple.getKey() + ", v=" + tuple.getValue());
                    }
                }
            });
    }

    /**
     *
     * @param mode Cache atomicity mode.
     * @param clazz Cache message type to be blocked before re-creating a cache.
     * @param cacheOp Cache operation.
     * @throws Exception If failed.
     */
    private void testCacheOperationAndCacheRecreate(
        CacheAtomicityMode mode,
        Class<? extends GridCacheIdMessage> clazz,
        IgniteBiInClosure<IgniteCache<Integer, Integer>, List<Integer>> cacheOp
    ) throws Exception {
        IgniteEx g0 = grid(0);
        IgniteEx client = grid(1);

        // Initial loading.
        IgniteCache<Integer, Integer> clientCache = createCache(client, mode);
        for (int i = 0; i < 100; i++)
            clientCache.put(i, i);

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        TestRecordingCommunicationSpi crdSpi = TestRecordingCommunicationSpi.spi(g0);

        // Block cache operation.
        clientSpi.blockMessages((node, msg) -> {
            if (clazz.isAssignableFrom(msg.getClass())) {
                GridCacheIdMessage msg0 = (GridCacheIdMessage)msg;

                if (msg0.cacheId() == 0 || msg0.cacheId() == CU.cacheId(CACHE_NAME))
                    return true;
            }

            return false;
        });

        // Block notifying the client node about upcoming changes.
        crdSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsFullMessage)
                return true;

            return false;
        });

        List<Integer> primaryKeys = primaryKeys(g0.cache(CACHE_NAME), 2, 1);

        // Initiate cache operation.
        IgniteInternalFuture<?> updFut = runAsync(() -> cacheOp.apply(clientCache, primaryKeys));

        // Wait for operation is initiated on the client node.
        clientSpi.waitForBlocked();

        // Destoy the existing cache and re-create it once again in order to deliver the blocked cache message to the server node
        // when the reqired cache is destroyed and new cache handlers are registered.
        g0.destroyCache(clientCache.getName());

        // Create a new cache with the same name.
        IgniteCache newCache = createCache(g0, mode);

        // Upload new values.
        for (int i = 0; i < 100; i++)
            newCache.put(i, i + 1_000);

        // Unblock cache operation.
        clientSpi.stopBlock();

        try {
            updFut.get(10, TimeUnit.SECONDS);

            fail("Exception was not thrown.");
        }
        catch (Exception e) {
            assertTrue("Unexpected exception [err=" + e + ']', X.hasCause(e, CacheException.class));
        }
        finally {
            crdSpi.stopBlock();
        }
    }

    /**
     * Creates a cache using the given node as initiator node and the given atomicity mode.
     *
     * @param ignite Node to be used to initiate creating a new cache.
     * @param mode Cache atomicity mode.
     * @return Ignite cache.
     */
    private IgniteCache<Integer, Integer> createCache(IgniteEx ignite, CacheAtomicityMode mode) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(CACHE_NAME);

        cfg.setBackups(1)
            .setReadFromBackup(false)
            .setAtomicityMode(mode)
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        return ignite.getOrCreateCache(cfg);
    }
}
