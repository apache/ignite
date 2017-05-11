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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.cache.event.EventType.CREATED;
import static javax.cache.event.EventType.REMOVED;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTest.ContinuousDeploy.ALL;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTest.ContinuousDeploy.CLIENT;
import static org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTest.ContinuousDeploy.SERVER;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheContinuousQueryRandomOperationsTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    private static final int KEYS = 50;

    /** */
    private static final int VALS = 10;

    /** */
    public static final int ITERATION_CNT = 100;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(getServerNodeCount());

        client = true;

        startGrid(getServerNodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilterAndFactoryProvided() throws Exception {
        final CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            false);

        grid(0).createCache(ccfg);

        try {
            final ContinuousQuery qry = new ContinuousQuery();

            qry.setRemoteFilterFactory(new Factory<CacheEntryEventFilter>() {
                @Override public CacheEntryEventFilter create() {
                    return null;
                }
            });

            qry.setRemoteFilter(new CacheEntryEventSerializableFilter() {
                @Override public boolean evaluate(CacheEntryEvent event) throws CacheEntryListenerException {
                    return false;
                }
            });

            qry.setLocalListener(new CacheEntryUpdatedListener() {
                @Override public void onUpdated(Iterable iterable) throws CacheEntryListenerException {
                    // No-op.
                }
            });

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return grid(0).cache(ccfg.getName()).query(qry);
                }
            }, IgniteException.class, null);

        }
        finally {
            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            false);

        doTestContinuousQuery(ccfg, CLIENT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            false);

        doTestContinuousQuery(ccfg, SERVER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            false);

        doTestContinuousQuery(ccfg, ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            false);

        doTestContinuousQuery(ccfg, SERVER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicatedAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            false);

        doTestContinuousQuery(ccfg, ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicatedClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            false);

        doTestContinuousQuery(ccfg, CLIENT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoBackups() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            false);

        doTestContinuousQuery(ccfg, SERVER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoBackupsAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            false);

        doTestContinuousQuery(ccfg, ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoBackupsClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            false);

        doTestContinuousQuery(ccfg, CLIENT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, SERVER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, SERVER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveAtomicWithoutBackup() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            false);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveAtomicWithoutBackupWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            false);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveAtomic() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            false);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveAtomicWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            true);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            false);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveTxWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            false);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveReplicatedTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL,
            false);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveReplicatedTxWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL,
            false);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveReplicatedAtomic() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            false);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleRemoveReplicatedAtomicWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            true);

        doTestNotModifyOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestNotModifyOperation(CacheConfiguration ccfg) throws Exception {
        singleOperation(ccfg);
        batchOperation(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void singleOperation(CacheConfiguration ccfg) throws Exception {
        IgniteCache<QueryTestKey, QueryTestValue> cache = grid(getClientIndex()).createCache(ccfg);

        try {
            ContinuousQuery<QueryTestKey, QueryTestValue> qry = new ContinuousQuery<>();

            final List<CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> evts =
                new CopyOnWriteArrayList<>();

            if (noOpFilterFactory() != null)
                qry.setRemoteFilterFactory(noOpFilterFactory());

            qry.setLocalListener(new CacheEntryUpdatedListener<QueryTestKey, QueryTestValue>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends QueryTestKey,
                    ? extends QueryTestValue>> events) throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> e : events)
                        evts.add(e);
                }
            });

            QueryTestKey key = new QueryTestKey(1);

            try (QueryCursor qryCur = cache.query(qry)) {
                for (int i = 0; i < ITERATION_CNT; i++) {
                    log.info("Start iteration: " + i);
                    // Not events.
                    cache.invoke(key, (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(true));

                    // Get events.
                    cache.put(key, new QueryTestValue(1));
                    cache.remove(key);

                    // Not events.
                    cache.invoke(key, (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(null, false));
                    cache.invoke(key, (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(null, false));
                    cache.invoke(key, (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(true));
                    cache.remove(key);

                    // Get events.
                    cache.put(key, new QueryTestValue(2));

                    // Not events.
                    cache.invoke(key, (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(true));

                    // Get events.
                    cache.invoke(key, (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(null, false));

                    // Not events.
                    cache.remove(key);

                    // Get events.
                    cache.put(key, new QueryTestValue(3));
                    cache.put(key, new QueryTestValue(4));

                    // Not events.
                    cache.invoke(key, (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(true));
                    cache.putIfAbsent(key, new QueryTestValue(5));
                    cache.putIfAbsent(key, new QueryTestValue(5));
                    cache.putIfAbsent(key, new QueryTestValue(5));
                    cache.invoke(key, (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(true));
                    cache.remove(key, new QueryTestValue(5));

                    // Get events.
                    cache.remove(key, new QueryTestValue(4));
                    cache.putIfAbsent(key, new QueryTestValue(5));

                    // Not events.
                    cache.replace(key, new QueryTestValue(3), new QueryTestValue(2));
                    cache.replace(key, new QueryTestValue(3), new QueryTestValue(2));
                    cache.replace(key, new QueryTestValue(3), new QueryTestValue(2));

                    // Get events.
                    cache.replace(key, new QueryTestValue(5), new QueryTestValue(6));

                    assert GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return evts.size() == 9;
                        }
                    }, 5_000);

                    checkSingleEvent(evts.get(0), CREATED, new QueryTestValue(1), null);
                    checkSingleEvent(evts.get(1), REMOVED, null, new QueryTestValue(1));
                    checkSingleEvent(evts.get(2), CREATED, new QueryTestValue(2), null);
                    checkSingleEvent(evts.get(3), REMOVED, null, new QueryTestValue(2));
                    checkSingleEvent(evts.get(4), CREATED, new QueryTestValue(3), null);
                    checkSingleEvent(evts.get(5), EventType.UPDATED, new QueryTestValue(4), new QueryTestValue(3));
                    checkSingleEvent(evts.get(6), REMOVED, null, new QueryTestValue(4));
                    checkSingleEvent(evts.get(7), CREATED, new QueryTestValue(5), null);
                    checkSingleEvent(evts.get(8), EventType.UPDATED, new QueryTestValue(6), new QueryTestValue(5));

                    evts.clear();

                    cache.remove(key);
                    cache.remove(key);

                    assert GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return evts.size() == 1;
                        }
                    }, 5_000);

                    evts.clear();

                    log.info("Finish iteration: " + i);
                }
            }
        }
        finally {
            grid(getClientIndex()).destroyCache(ccfg.getName());
        }
    }

    /**
     * @return No-op filter factory for batch operations.
     */
    protected Factory<? extends CacheEntryEventFilter<QueryTestKey, QueryTestValue>> noOpFilterFactory() {
        return null;
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void batchOperation(CacheConfiguration ccfg) throws Exception {
        IgniteCache<QueryTestKey, QueryTestValue> cache = grid(getClientIndex()).createCache(ccfg);

        try {
            ContinuousQuery<QueryTestKey, QueryTestValue> qry = new ContinuousQuery<>();

            final List<CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> evts =
                new CopyOnWriteArrayList<>();

            if (noOpFilterFactory() != null)
                qry.setRemoteFilterFactory(noOpFilterFactory());

            qry.setLocalListener(new CacheEntryUpdatedListener<QueryTestKey, QueryTestValue>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<? extends QueryTestKey,
                    ? extends QueryTestValue>> events) throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> e : events)
                        evts.add(e);
                }
            });

            Map<QueryTestKey, QueryTestValue> map = new TreeMap<>();

            for (int i = 0; i < KEYS; i++)
                map.put(new QueryTestKey(i), new QueryTestValue(i));

            try (QueryCursor qryCur = cache.query(qry)) {
                for (int i = 0; i < ITERATION_CNT / 2; i++) {
                    log.info("Start iteration: " + i);
                    // Not events.
                    cache.removeAll(map.keySet());
                    cache.invokeAll(map.keySet(), (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(null, false));
                    cache.invokeAll(map.keySet(), (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(true));

                    // Get events.
                    cache.putAll(map);

                    assert GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return evts.size() == KEYS;
                        }
                    }, 5_000);

                    checkEvents(evts, CREATED);

                    evts.clear();

                    // Not events.
                    cache.invokeAll(map.keySet(), (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(true));

                    U.sleep(100);

                    assertEquals(0, evts.size());

                    // Get events.
                    cache.invokeAll(map.keySet(), (EntryProcessor<QueryTestKey, QueryTestValue, ? extends Object>)
                        (Object)new EntrySetValueProcessor(null, false));

                    // Not events.
                    cache.removeAll(map.keySet());
                    cache.removeAll(map.keySet());

                    assert GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return evts.size() == KEYS;
                        }
                    }, 5_000);

                    checkEvents(evts, REMOVED);

                    evts.clear();

                    log.info("Finish iteration: " + i);
                }
            }
        }
        finally {
            grid(getClientIndex()).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param evts Events.
     * @param evtType Event type.
     */
    private void checkEvents(List<CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> evts,
        EventType evtType) {
        for (int key = 0; key < KEYS; key++) {
            QueryTestKey keyVal = new QueryTestKey(key);

            for (CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> e : evts) {
                if (e.getKey().equals(keyVal)) {
                    checkSingleEvent(e,
                        evtType,
                        evtType == CREATED ? new QueryTestValue(key) : null,
                        evtType == REMOVED ? new QueryTestValue(key) : null);

                    keyVal = null;

                    break;
                }
            }

            assertNull("Event for key not found.", keyVal);
        }
    }


    /**
     * @param event Event.
     * @param type Event type.
     * @param val Value.
     * @param oldVal Old value.
     */
    private void checkSingleEvent(
        CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> event,
        EventType type,
        QueryTestValue val,
        QueryTestValue oldVal) {
        assertEquals(event.getEventType(), type);
        assertEquals(event.getValue(), val);
        assertEquals(event.getOldValue(), oldVal);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, CLIENT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxClientExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, CLIENT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, SERVER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicatedClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, CLIENT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackups() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, SERVER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackupsAllNodes() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackupsExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, SERVER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackupsClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            false);

        doTestContinuousQuery(ccfg, CLIENT);
    }

    /**
     * @param ccfg Cache configuration.
     * @param deploy The place where continuous query will be started.
     * @throws Exception If failed.
     */
    protected void doTestContinuousQuery(CacheConfiguration<Object, Object> ccfg, ContinuousDeploy deploy)
        throws Exception {
        ignite(0).createCache(ccfg);

        try {
            long seed = System.currentTimeMillis();

            Random rnd = new Random(seed);

            log.info("Random seed: " + seed);

            List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues = new ArrayList<>();

            Collection<QueryCursor<?>> curs = new ArrayList<>();

            if (deploy == CLIENT) {
                ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

                final BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue = new ArrayBlockingQueue<>(50_000);

                qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
                    @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                        for (CacheEntryEvent<?, ?> evt : evts)
                            evtsQueue.add(evt);
                    }
                });

                evtsQueues.add(evtsQueue);

                QueryCursor<?> cur = grid(getClientIndex()).cache(ccfg.getName()).query(qry);

                curs.add(cur);
            }
            else if (deploy == SERVER) {
                ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

                final BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue = new ArrayBlockingQueue<>(50_000);

                qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
                    @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                        for (CacheEntryEvent<?, ?> evt : evts)
                            evtsQueue.add(evt);
                    }
                });

                evtsQueues.add(evtsQueue);

                QueryCursor<?> cur = grid(rnd.nextInt(getServerNodeCount())).cache(ccfg.getName()).query(qry);

                curs.add(cur);
            }
            else {
                for (int i = 0; i <= getServerNodeCount(); i++) {
                    ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

                    final BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue = new ArrayBlockingQueue<>(50_000);

                    qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
                        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                            for (CacheEntryEvent<?, ?> evt : evts)
                                evtsQueue.add(evt);
                        }
                    });

                    evtsQueues.add(evtsQueue);

                    QueryCursor<?> cur = ignite(i).cache(ccfg.getName()).query(qry);

                    curs.add(cur);
                }
            }

            ConcurrentMap<Object, Object> expData = new ConcurrentHashMap<>();

            Map<Integer, Long> partCntr = new ConcurrentHashMap<>();

            try {
                for (int i = 0; i < ITERATION_CNT; i++) {
                    if (i % 20 == 0)
                        log.info("Iteration: " + i);

                    for (int idx = 0; idx < getServerNodeCount(); idx++)
                        randomUpdate(rnd, evtsQueues, expData, partCntr, grid(idx).cache(ccfg.getName()));
                }
            }
            finally {
                for (QueryCursor<?> cur : curs)
                    cur.close();
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @return Client node index.
     */
    private int getClientIndex() {
        return getServerNodeCount() - 1;
    }

    /**
     * @return Count nodes.
     */
    protected int getServerNodeCount() {
        return NODES;
    }

    /**
     * @param rnd Random generator.
     * @param evtsQueues Events queue.
     * @param expData Expected cache data.
     * @param partCntr Partition counter.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void randomUpdate(
        Random rnd,
        List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues,
        ConcurrentMap<Object, Object> expData,
        Map<Integer, Long> partCntr,
        IgniteCache<Object, Object> cache)
        throws Exception {
        Object key = new QueryTestKey(rnd.nextInt(KEYS));
        Object newVal = value(rnd);
        Object oldVal = expData.get(key);

        int op = rnd.nextInt(13);

        Ignite ignite = cache.unwrap(Ignite.class);

        Map<Object, Long> expEvtCntrs = new ConcurrentHashMap<>();

        Transaction tx = null;

        if (cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL && rnd.nextBoolean())
            tx = ignite.transactions().txStart(txRandomConcurrency(rnd), txRandomIsolation(rnd));

        try {
            // log.info("Random operation [key=" + key + ", op=" + op + ']');

            switch (op) {
                case 0: {
                    cache.put(key, newVal);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                    waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, newVal, oldVal);

                    expData.put(key, newVal);

                    break;
                }

                case 1: {
                    cache.getAndPut(key, newVal);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                    waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, newVal, oldVal);

                    expData.put(key, newVal);

                    break;
                }

                case 2: {
                    cache.remove(key);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                    waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, null, oldVal);

                    expData.remove(key);

                    break;
                }

                case 3: {
                    cache.getAndRemove(key);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                    waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, null, oldVal);

                    expData.remove(key);

                    break;
                }

                case 4: {
                    cache.invoke(key, new EntrySetValueProcessor(newVal, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                    waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, newVal, oldVal);

                    expData.put(key, newVal);

                    break;
                }

                case 5: {
                    cache.invoke(key, new EntrySetValueProcessor(null, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                    waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, null, oldVal);

                    expData.remove(key);

                    break;
                }

                case 6: {
                    cache.putIfAbsent(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal == null) {
                        updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                        waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, newVal, null);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueues);

                    break;
                }

                case 7: {
                    cache.getAndPutIfAbsent(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal == null) {
                        updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                        waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, newVal, null);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueues);

                    break;
                }

                case 8: {
                    cache.replace(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal != null) {
                        updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                        waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, newVal, oldVal);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueues);

                    break;
                }

                case 9: {
                    cache.getAndReplace(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal != null) {
                        updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                        waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, newVal, oldVal);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(evtsQueues);

                    break;
                }

                case 10: {
                    if (oldVal != null) {
                        Object replaceVal = value(rnd);

                        boolean success = replaceVal.equals(oldVal);

                        if (success) {
                            cache.replace(key, replaceVal, newVal);

                            if (tx != null)
                                tx.commit();

                            updatePartitionCounter(cache, key, partCntr, expEvtCntrs);

                            waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), key, newVal, oldVal);

                            expData.put(key, newVal);
                        }
                        else {
                            cache.replace(key, replaceVal, newVal);

                            if (tx != null)
                                tx.commit();

                            checkNoEvent(evtsQueues);
                        }
                    }
                    else {
                        cache.replace(key, value(rnd), newVal);

                        if (tx != null)
                            tx.commit();

                        checkNoEvent(evtsQueues);
                    }

                    break;
                }

                case 11: {
                    SortedMap<Object, Object> vals = new TreeMap<>();

                    while (vals.size() < KEYS / 5)
                        vals.put(new QueryTestKey(rnd.nextInt(KEYS)), value(rnd));

                    cache.putAll(vals);

                    if (tx != null)
                        tx.commit();

                    for (Map.Entry<Object, Object> e : vals.entrySet())
                        updatePartitionCounter(cache, e.getKey(), partCntr, expEvtCntrs);

                    waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), vals, expData);

                    expData.putAll(vals);

                    break;
                }

                case 12: {
                    SortedMap<Object, Object> vals = new TreeMap<>();

                    while (vals.size() < KEYS / 5)
                        vals.put(new QueryTestKey(rnd.nextInt(KEYS)), newVal);

                    cache.invokeAll(vals.keySet(), new EntrySetValueProcessor(newVal, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    for (Map.Entry<Object, Object> e : vals.entrySet())
                        updatePartitionCounter(cache, e.getKey(), partCntr, expEvtCntrs);

                    waitAndCheckEvent(evtsQueues, partCntr, expEvtCntrs, affinity(cache), vals, expData);

                    for (Object o : vals.keySet())
                        expData.put(o, newVal);

                    break;
                }

                default:
                    fail("Op:" + op);
            }
        } finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     *  @param evtsQueues Queue.
     * @param partCntrs Counters.
     * @param aff Affinity.
     * @param vals Values.
     * @param expData Expected data.
     */
    private void waitAndCheckEvent(List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues,
        Map<Integer, Long> partCntrs,
        Map<Object, Long> evtCntrs,
        Affinity<Object> aff,
        SortedMap<Object, Object> vals,
        Map<Object, Object> expData)
        throws Exception {
        for (BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue : evtsQueues) {
            Map<Object, CacheEntryEvent> rcvEvts = new HashMap<>();

            for (int i = 0; i < vals.size(); i++) {
                CacheEntryEvent<?, ?> evt = evtsQueue.poll(5, SECONDS);

                rcvEvts.put(evt.getKey(), evt);
            }

            assertEquals(vals.size(), rcvEvts.size());

            for (Map.Entry<Object, Object> e : vals.entrySet()) {
                Object key = e.getKey();
                Object val = e.getValue();
                Object oldVal = expData.get(key);

                if (val == null && oldVal == null) {
                    checkNoEvent(evtsQueues);

                    continue;
                }

                CacheEntryEvent evt = rcvEvts.get(key);

                assertNotNull("Failed to wait for event [key=" + key + ", val=" + val + ", oldVal=" + oldVal + ']',
                    evt);
                assertEquals(key, evt.getKey());
                assertEquals(val, evt.getValue());
                assertEquals(oldVal, evt.getOldValue());

                Long curPartCntr = partCntrs.get(aff.partition(key));
                Long cntr = evtCntrs.get(key);
                CacheQueryEntryEvent qryEntryEvt = (CacheQueryEntryEvent)evt.unwrap(CacheQueryEntryEvent.class);

                assertNotNull(cntr);
                assertNotNull(curPartCntr);
                assertNotNull(qryEntryEvt);
                assertTrue(cntr <= curPartCntr);

                assertEquals((long)cntr, qryEntryEvt.getPartitionUpdateCounter());
            }
        }
    }

    /**
     * @param rnd {@link Random}.
     * @return {@link TransactionIsolation}.
     */
    private TransactionIsolation txRandomIsolation(Random rnd) {
        int val = rnd.nextInt(3);

        if (val == 0)
            return READ_COMMITTED;
        else if (val == 1)
            return REPEATABLE_READ;
        else
            return SERIALIZABLE;
    }

    /**
     * @param rnd {@link Random}.
     * @return {@link TransactionConcurrency}.
     */
    private TransactionConcurrency txRandomConcurrency(Random rnd) {
        return rnd.nextBoolean() ? TransactionConcurrency.OPTIMISTIC : TransactionConcurrency.PESSIMISTIC;
    }

    /**
     * @param cache Cache.
     * @param key Key
     * @param cntrs Partition counters.
     */
    private void updatePartitionCounter(IgniteCache<Object, Object> cache, Object key, Map<Integer, Long> cntrs,
        Map<Object, Long> evtCntrs) {
        Affinity<Object> aff = cache.unwrap(Ignite.class).affinity(cache.getName());

        int part = aff.partition(key);

        Long partCntr = cntrs.get(part);

        if (partCntr == null)
            partCntr = 0L;

        cntrs.put(part, ++partCntr);
        evtCntrs.put(key, partCntr);
    }

    /**
     * @param rnd Random generator.
     * @return Cache value.
     */
    private static Object value(Random rnd) {
        return new QueryTestValue(rnd.nextInt(VALS));
    }

    /**
     * @param evtsQueues Event queue.
     * @param partCntrs Partition counters.
     * @param aff Affinity function.
     * @param key Key.
     * @param val Value.
     * @param oldVal Old value.
     * @throws Exception If failed.
     */
    private void waitAndCheckEvent(List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues,
        Map<Integer, Long> partCntrs,
        Map<Object, Long> evtCntrs,
        Affinity<Object> aff,
        Object key,
        Object val,
        Object oldVal)
        throws Exception {
        if (val == null && oldVal == null) {
            checkNoEvent(evtsQueues);

            return;
        }

        for (BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue : evtsQueues) {
            CacheEntryEvent<?, ?> evt = evtsQueue.poll(5, SECONDS);

            assertNotNull("Failed to wait for event [key=" + key + ", val=" + val + ", oldVal=" + oldVal + ']', evt);
            assertEquals(key, evt.getKey());
            assertEquals(val, evt.getValue());
            assertEquals(oldVal, evt.getOldValue());

            Long curPartCntr = partCntrs.get(aff.partition(key));

            Long cntr = evtCntrs.get(key);
            CacheQueryEntryEvent qryEntryEvt = evt.unwrap(CacheQueryEntryEvent.class);

            assertNotNull(cntr);
            assertNotNull(curPartCntr);
            assertNotNull(qryEntryEvt);
            assertTrue(cntr <= curPartCntr);

            assertEquals((long)cntr, qryEntryEvt.getPartitionUpdateCounter());
        }
    }

    /**
     * @param evtsQueues Event queue.
     * @throws Exception If failed.
     */
    private void checkNoEvent(List<BlockingQueue<CacheEntryEvent<?, ?>>> evtsQueues) throws Exception {
        for (BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue : evtsQueues) {
            CacheEntryEvent<?, ?> evt = evtsQueue.poll(50, MILLISECONDS);

            assertNull(evt);
        }
    }

    /**
     *
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param store If {@code true} configures dummy cache store.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode,
        boolean store) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setName("cache-" + UUID.randomUUID()); // TODO GG-11220 (remove setName when fixed).
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (store) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
        }

        return ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    // No-op.
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    // No-op.
                }
            };
        }
    }

    /**
     *
     */
    public static class QueryTestKey implements Serializable, Comparable {
        /** */
        private final Integer key;

        /**
         * @param key Key.
         */
        public QueryTestKey(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestKey that = (QueryTestKey)o;

            return key.equals(that.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestKey.class, this);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(Object o) {
            return key - ((QueryTestKey)o).key;
        }
    }

    /**
     *
     */
    public static class QueryTestValue implements Serializable {
        /** */
        @GridToStringInclude
        protected final Integer val1;

        /** */
        @GridToStringInclude
        protected final String val2;

        /**
         * @param val Value.
         */
        public QueryTestValue(Integer val) {
            this.val1 = val;
            this.val2 = String.valueOf(val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestValue that = (QueryTestValue) o;

            return val1.equals(that.val1) && val2.equals(that.val2);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = val1.hashCode();

            res = 31 * res + val2.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestValue.class, this);
        }
    }

    /**
     *
     */
    protected static class EntrySetValueProcessor implements EntryProcessor<Object, Object, Object> {
        /** */
        private Object val;

        /** */
        private boolean retOld;

        /** */
        private boolean skipModify;

        /**
         * @param skipModify If {@code true} then entry will not be modified.
         */
        public EntrySetValueProcessor(boolean skipModify) {
            this.skipModify = skipModify;
        }

        /**
         * @param val Value to set.
         * @param retOld Return old value flag.
         */
        public EntrySetValueProcessor(Object val, boolean retOld) {
            this.val = val;
            this.retOld = retOld;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
            if (skipModify)
                return null;

            Object old = retOld ? e.getValue() : null;

            if (val != null)
                e.setValue(val);
            else
                e.remove();

            return old;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntrySetValueProcessor.class, this);
        }
    }

    /**
     *
     */
    protected enum ContinuousDeploy {
        CLIENT, SERVER, ALL
    }
}
