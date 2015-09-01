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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Test for TRANSFORM events recording.
 */
@SuppressWarnings("ConstantConditions")
public class GridCacheTransformEventSelfTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int GRID_CNT = 3;

    /** Backups count for partitioned cache. */
    private static final int BACKUP_CNT = 1;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Closure name. */
    private static final String CLO_NAME = Transformer.class.getName();

    /** Key 1. */
    private Integer key1;

    /** Key 2. */
    private Integer key2;

    /** Two keys in form of a set. */
    private Set<Integer> keys;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Nodes. */
    private Ignite[] ignites;

    /** Node IDs. */
    private UUID[] ids;

    /** Caches. */
    private IgniteCache<Integer, Integer>[] caches;

    /** Recorded events.*/
    private ConcurrentHashSet<CacheEvent> evts;

    /** Cache mode. */
    private CacheMode cacheMode;

    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** TX concurrency. */
    private TransactionConcurrency txConcurrency;

    /** TX isolation. */
    private TransactionIsolation txIsolation;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        TransactionConfiguration tCfg = cfg.getTransactionConfiguration();

        tCfg.setDefaultTxConcurrency(txConcurrency);
        tCfg.setDefaultTxIsolation(txIsolation);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(BACKUP_CNT);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(ccfg);
        cfg.setLocalHost("127.0.0.1");
        cfg.setIncludeEventTypes(EVT_CACHE_OBJECT_READ);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ignites = null;
        ids = null;
        caches = null;

        evts = null;

        key1 = null;
        key2 = null;
        keys = null;
    }

    /**
     * Initialization routine.
     *
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param txConcurrency TX concurrency.
     * @param txIsolation TX isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void initialize(CacheMode cacheMode, CacheAtomicityMode atomicityMode,
        TransactionConcurrency txConcurrency, TransactionIsolation txIsolation) throws Exception {
        this.cacheMode = cacheMode;
        this.atomicityMode = atomicityMode;
        this.txConcurrency = txConcurrency;
        this.txIsolation = txIsolation;

        evts = new ConcurrentHashSet<>();

        startGrids(GRID_CNT);

        awaitPartitionMapExchange();

        ignites = new Ignite[GRID_CNT];
        ids = new UUID[GRID_CNT];
        caches = new IgniteCache[GRID_CNT];

        for (int i = 0; i < GRID_CNT; i++) {
            ignites[i] = grid(i);

            ids[i] = ignites[i].cluster().localNode().id();

            caches[i] = ignites[i].cache(CACHE_NAME);

            ignites[i].events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    CacheEvent evt0 = (CacheEvent)evt;

                    if (evt0.closureClassName() != null) {
                        System.out.println("ADDED: [nodeId=" + evt0.node() + ", evt=" + evt0 + ']');

                        evts.add(evt0);
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_READ);
        }

        int key = 0;

        while (true) {
            if (cacheMode != PARTITIONED || (primary(0, key) && backup(1, key))) {
                key1 = key++;

                break;
            }
            else
                key++;
        }

        while (true) {
            if (cacheMode != PARTITIONED || (primary(0, key) && backup(1, key))) {
                key2 = key;

                break;
            }
            else
                key++;
        }

        keys = new HashSet<>();

        keys.add(key1);
        keys.add(key2);

        caches[0].put(key1, 1);
        caches[0].put(key2, 2);

        for (int i = 0; i < GRID_CNT; i++) {
            ignites[i].events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    CacheEvent evt0 = (CacheEvent)evt;

                    if (evt0.closureClassName() != null)
                        evts.add(evt0);

                    return true;
                }
            }, EVT_CACHE_OBJECT_READ);
        }
    }

    /**
     * @param gridIdx Grid index.
     * @param key Key.
     * @return {@code True} if grid is primary for given key.
     */
    private boolean primary(int gridIdx, Object key) {
        Affinity<Object> aff = grid(0).affinity(CACHE_NAME);

        return aff.isPrimary(grid(gridIdx).cluster().localNode(), key);
    }

    /**
     * @param gridIdx Grid index.
     * @param key Key.
     * @return {@code True} if grid is primary for given key.
     */
    private boolean backup(int gridIdx, Object key) {
        Affinity<Object> aff = grid(0).affinity(CACHE_NAME);

        return aff.isBackup(grid(gridIdx).cluster().localNode(), key);
    }

    /**
     * Test TRANSACTIONAL LOCAL cache with OPTIMISTIC/REPEATABLE_READ transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxLocalOptimisticRepeatableRead() throws Exception {
        checkTx(LOCAL, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * Test TRANSACTIONAL LOCAL cache with OPTIMISTIC/READ_COMMITTED transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxLocalOptimisticReadCommitted() throws Exception {
        checkTx(LOCAL, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * Test TRANSACTIONAL LOCAL cache with OPTIMISTIC/SERIALIZABLE transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxLocalOptimisticSerializable() throws Exception {
        checkTx(LOCAL, OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * Test TRANSACTIONAL LOCAL cache with PESSIMISTIC/REPEATABLE_READ transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxLocalPessimisticRepeatableRead() throws Exception {
        checkTx(LOCAL, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * Test TRANSACTIONAL LOCAL cache with PESSIMISTIC/READ_COMMITTED transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxLocalPessimisticReadCommitted() throws Exception {
        checkTx(LOCAL, PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * Test TRANSACTIONAL LOCAL cache with PESSIMISTIC/SERIALIZABLE transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxLocalPessimisticSerializable() throws Exception {
        checkTx(LOCAL, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * Test TRANSACTIONAL PARTITIONED cache with OPTIMISTIC/REPEATABLE_READ transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxPartitionedOptimisticRepeatableRead() throws Exception {
        checkTx(PARTITIONED, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * Test TRANSACTIONAL PARTITIONED cache with OPTIMISTIC/READ_COMMITTED transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxPartitionedOptimisticReadCommitted() throws Exception {
        checkTx(PARTITIONED, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * Test TRANSACTIONAL PARTITIONED cache with OPTIMISTIC/SERIALIZABLE transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxPartitionedOptimisticSerializable() throws Exception {
        checkTx(PARTITIONED, OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * Test TRANSACTIONAL PARTITIONED cache with PESSIMISTIC/REPEATABLE_READ transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxPartitionedPessimisticRepeatableRead() throws Exception {
        checkTx(PARTITIONED, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * Test TRANSACTIONAL PARTITIONED cache with PESSIMISTIC/READ_COMMITTED transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxPartitionedPessimisticReadCommitted() throws Exception {
        checkTx(PARTITIONED, PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * Test TRANSACTIONAL PARTITIONED cache with PESSIMISTIC/SERIALIZABLE transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxPartitionedPessimisticSerializable() throws Exception {
        checkTx(PARTITIONED, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * Test TRANSACTIONAL REPLICATED cache with OPTIMISTIC/REPEATABLE_READ transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxReplicatedOptimisticRepeatableRead() throws Exception {
        checkTx(REPLICATED, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * Test TRANSACTIONAL REPLICATED cache with OPTIMISTIC/READ_COMMITTED transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxReplicatedOptimisticReadCommitted() throws Exception {
        checkTx(REPLICATED, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * Test TRANSACTIONAL REPLICATED cache with OPTIMISTIC/SERIALIZABLE transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxReplicatedOptimisticSerializable() throws Exception {
        checkTx(REPLICATED, OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * Test TRANSACTIONAL REPLICATED cache with PESSIMISTIC/REPEATABLE_READ transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxReplicatedPessimisticRepeatableRead() throws Exception {
        checkTx(REPLICATED, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * Test TRANSACTIONAL REPLICATED cache with PESSIMISTIC/READ_COMMITTED transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxReplicatedPessimisticReadCommitted() throws Exception {
        checkTx(REPLICATED, PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * Test TRANSACTIONAL REPLICATED cache with PESSIMISTIC/SERIALIZABLE transaction.
     *
     * @throws Exception If failed.
     */
    public void testTxReplicatedPessimisticSerializable() throws Exception {
        checkTx(REPLICATED, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * Test ATOMIC LOCAL cache.
     *
     * @throws Exception If failed.
     */
    public void testAtomicLocal() throws Exception {
        checkAtomic(LOCAL);
    }

    /**
     * Test ATOMIC PARTITIONED cache.
     *
     * @throws Exception If failed.
     */
    public void testAtomicPartitioned() throws Exception {
        checkAtomic(PARTITIONED);
    }

    /**
     * Test ATOMIC REPLICATED cache.
     *
     * @throws Exception If failed.
     */
    public void testAtomicReplicated() throws Exception {
        checkAtomic(REPLICATED);
    }

    /**
     * Check ATOMIC cache.
     *
     * @param cacheMode Cache mode.
     * @throws Exception If failed.
     */
    private void checkAtomic(CacheMode cacheMode) throws Exception {
        initialize(cacheMode, ATOMIC, null, null);

        caches[0].invoke(key1, new Transformer());

        checkEventNodeIdsStrict(primaryIdsForKeys(key1));

        assert evts.isEmpty();

        caches[0].invokeAll(keys, new Transformer());

        checkEventNodeIdsStrict(primaryIdsForKeys(key1, key2));
    }

    /**
     * Check TRANSACTIONAL cache.
     *
     * @param cacheMode Cache mode.
     * @param txConcurrency TX concurrency.
     * @param txIsolation TX isolation.
     *
     * @throws Exception If failed.
     */
    private void checkTx(CacheMode cacheMode, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {
        initialize(cacheMode, TRANSACTIONAL, txConcurrency, txIsolation);

        System.out.println("BEFORE: " + evts.size());

        caches[0].invoke(key1, new Transformer());

        System.out.println("AFTER: " + evts.size());

        checkEventNodeIdsStrict(idsForKeys(key1));

        assert evts.isEmpty();

        caches[0].invokeAll(keys, new Transformer());

        checkEventNodeIdsStrict(idsForKeys(key1, key2));
    }

    /**
     * Get node IDs where the given keys must reside.
     *
     * @param keys Keys.
     * @return Node IDs.
     */
    private UUID[] idsForKeys(int... keys) {
        return idsForKeys(false, keys);
    }

    /**
     * Get primary node IDs where the given keys must reside.
     *
     * @param keys Keys.
     * @return Node IDs.
     */
    private UUID[] primaryIdsForKeys(int... keys) {
        return idsForKeys(true, keys);
    }

    /**
     * Get node IDs where the given keys must reside.
     *
     * @param primaryOnly Primary only flag.
     * @param keys Keys.
     * @return Node IDs.
     */
    @SuppressWarnings("UnusedDeclaration")
    private UUID[] idsForKeys(boolean primaryOnly, int... keys) {
        List<UUID> res = new ArrayList<>();

        if (cacheMode == LOCAL) {
            for (int key : keys)
                res.add(ids[0]); // Perform PUTs from the node with index 0.
        }
        else if (cacheMode == PARTITIONED) {
            for (int key : keys) {
                for (int i = 0; i < GRID_CNT; i++) {
                    if (primary(i, key) || (!primaryOnly && backup(i, key)))
                        res.add(ids[i]);
                }
            }
        }
        else if (cacheMode == REPLICATED) {
            for (int key : keys) {
                if (primaryOnly)
                    res.add(grid(0).affinity(CACHE_NAME).mapKeyToNode(key).id());
                else
                    res.addAll(Arrays.asList(ids));
            }
        }

        return res.toArray(new UUID[res.size()]);
    }

    /**
     * Ensure that events were recorded on the given nodes.
     *
     * @param ids Event IDs.
     */
    private void checkEventNodeIdsStrict(UUID... ids) {
        if (ids == null)
            assertTrue(evts.isEmpty());
        else {
            assertEquals(ids.length, evts.size());

            for (UUID id : ids) {
                CacheEvent foundEvt = null;

                for (CacheEvent evt : evts) {
                    if (F.eq(id, evt.node().id())) {
                        assertEquals(CLO_NAME, evt.closureClassName());

                        foundEvt = evt;

                        break;
                    }
                }

                if (foundEvt == null) {
                    int gridIdx = -1;

                    for (int i = 0; i < GRID_CNT; i++) {
                        if (F.eq(this.ids[i], id)) {
                            gridIdx = i;

                            break;
                        }
                    }

                    fail("Expected transform event was not triggered on the node [nodeId=" + id +
                        ", key1Primary=" + primary(gridIdx, key1) + ", key1Backup=" + backup(gridIdx, key1) +
                        ", key2Primary=" + primary(gridIdx, key2) + ", key2Backup=" + backup(gridIdx, key2) + ']');
                }
                else
                    evts.remove(foundEvt);
            }
        }
    }

    /**
     * Transform closure.
     */
    private static class Transformer implements EntryProcessor<Integer, Integer, Void>, Serializable {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
            e.setValue(e.getValue() + 1);

            return null;
        }
    }
}