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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.inmemory.GridTestSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.jsr166.ConcurrentHashMap8;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.TestMemoryMode;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheSerializableTransactionsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final boolean FAST = false;

    /** */
    private static Map<Integer, Integer> storeMap = new ConcurrentHashMap8<>();

    /** */
    private static final int SRVS = 4;

    /** */
    private static final int CLIENTS = 3;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        client = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxStreamerLoad() throws Exception {
        txStreamerLoad(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxStreamerLoadAllowOverwrite() throws Exception {
        txStreamerLoad(true);
    }

    /**
     * @param allowOverwrite Streamer flag.
     * @throws Exception If failed.
     */
    private void txStreamerLoad(boolean allowOverwrite) throws Exception {
        Ignite ignite0 = ignite(0);

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            if (ccfg.getCacheStoreFactory() == null)
                continue;

            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                awaitPartitionMapExchange();

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys)
                    txStreamerLoad(ignite0, key, cache.getName(), allowOverwrite);

                txStreamerLoad(ignite(SRVS), 10_000, cache.getName(), allowOverwrite);
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @param ignite Node.
     * @param key Key.
     * @param cacheName Cache name.
     * @param allowOverwrite Streamer flag.
     * @throws Exception If failed.
     */
    private void txStreamerLoad(Ignite ignite,
        Integer key,
        String cacheName,
        boolean allowOverwrite) throws Exception {
        IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

        log.info("Test key: " + key);

        Integer loadVal = -1;

        IgniteTransactions txs = ignite.transactions();

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cache.getName())) {
            streamer.allowOverwrite(allowOverwrite);

            streamer.addData(key, loadVal);
        }

        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
            Integer val = cache.get(key);

            assertEquals(loadVal, val);

            tx.commit();
        }

        checkValue(key, loadVal, cache.getName());

        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
            Integer val = cache.get(key);

            assertEquals(loadVal, val);

            cache.put(key, 0);

            tx.commit();
        }

        checkValue(key, 0, cache.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxLoadFromStore() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            if (ccfg.getCacheStoreFactory() == null)
                continue;

            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer storeVal = -1;

                    storeMap.put(key, storeVal);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertEquals(storeVal, val);

                        tx.commit();
                    }

                    checkValue(key, storeVal, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCommitReadOnly1() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        tx.rollback();
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 1);

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        tx.commit();
                    }
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCommitReadOnly2() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (final Integer key : keys) {
                    log.info("Test key: " + key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        txAsync(cache, OPTIMISTIC, SERIALIZABLE,
                            new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                    cache.get(key);

                                    return null;
                                }
                            }
                        );

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache.get(key);

                        assertNull(val);

                        txAsync(cache, PESSIMISTIC, REPEATABLE_READ,
                            new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                    cache.get(key);

                                    return null;
                                }
                            }
                        );

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCommit() throws Exception {
        Ignite ignite0 = ignite(0);
        Ignite ignite1 = ignite(1);

        final IgniteTransactions txs0 = ignite0.transactions();
        final IgniteTransactions txs1 = ignite1.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache0 = ignite0.createCache(ccfg);
                IgniteCache<Integer, Integer> cache1 = ignite1.cache(ccfg.getName());

                List<Integer> keys = testKeys(cache0);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    for (int i = 0; i < 100; i++) {
                        try (Transaction tx = txs0.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache0.get(key);

                            assertEquals(expVal, val);

                            cache0.put(key, i);

                            tx.commit();

                            expVal = i;
                        }

                        try (Transaction tx = txs1.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache1.get(key);

                            assertEquals(expVal, val);

                            cache1.put(key, val);

                            tx.commit();
                        }

                        try (Transaction tx = txs0.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache0.get(key);

                            assertEquals(expVal, val);

                            cache0.put(key, val);

                            tx.commit();
                        }
                    }

                    checkValue(key, expVal, cache0.getName());

                    cache0.remove(key);

                    try (Transaction tx = txs0.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Integer val = cache0.get(key);

                        assertNull(val);

                        cache0.put(key, expVal + 1);

                        tx.commit();
                    }

                    checkValue(key, expVal + 1, cache0.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxRollback() throws Exception {
        Ignite ignite0 = ignite(0);
        Ignite ignite1 = ignite(1);

        final IgniteTransactions txs0 = ignite0.transactions();
        final IgniteTransactions txs1 = ignite1.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache0 = ignite0.createCache(ccfg);
                IgniteCache<Integer, Integer> cache1 = ignite1.cache(ccfg.getName());

                List<Integer> keys = testKeys(cache0);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    for (int i = 0; i < 100; i++) {
                        try (Transaction tx = txs0.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache0.get(key);

                            assertEquals(expVal, val);

                            cache0.put(key, i);

                            tx.rollback();
                        }

                        try (Transaction tx = txs0.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache0.get(key);

                            assertEquals(expVal, val);

                            cache0.put(key, i);

                            tx.commit();

                            expVal = i;
                        }

                        try (Transaction tx = txs1.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache1.get(key);

                            assertEquals(expVal, val);

                            cache1.put(key, val);

                            tx.commit();
                        }
                    }

                    checkValue(key, expVal, cache0.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCommitReadOnlyGetAll() throws Exception {
        testTxCommitReadOnlyGetAll(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCommitReadOnlyGetEntries() throws Exception {
        testTxCommitReadOnlyGetAll(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testTxCommitReadOnlyGetAll(boolean needVer) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                Set<Integer> keys = new HashSet<>();

                for (int i = 0; i < 100; i++)
                    keys.add(i);

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    if (needVer) {
                        Collection<CacheEntry<Integer, Integer>> c = cache.getEntries(keys);

                        assertTrue(c.isEmpty());
                    }
                    else {
                        Map<Integer, Integer> map = cache.getAll(keys);

                        assertTrue(map.isEmpty());
                    }

                    tx.commit();
                }

                for (Integer key : keys)
                    checkValue(key, null, cache.getName());

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    if (needVer) {
                        Collection<CacheEntry<Integer, Integer>> c = cache.getEntries(keys);

                        assertTrue(c.isEmpty());
                    }
                    else {
                        Map<Integer, Integer> map = cache.getAll(keys);

                        assertTrue(map.isEmpty());
                    }

                    tx.rollback();
                }

                for (Integer key : keys)
                    checkValue(key, null, cache.getName());
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCommitReadWriteTwoNodes() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                Integer key0 = primaryKey(ignite(0).cache(null));
                Integer key1 = primaryKey(ignite(1).cache(null));

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(key0, key0);

                    cache.get(key1);

                    tx.commit();
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictRead1() throws Exception {
        txConflictRead(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictRead2() throws Exception {
        txConflictRead(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadEntry1() throws Exception {
        txConflictRead(true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadEntry2() throws Exception {
        txConflictRead(false, true);
    }

    /**
     * @param noVal If {@code true} there is no cache value when read in tx.
     * @param needVer If {@code true} then gets entry, otherwise just value.
     * @throws Exception If failed.
     */
    private void txConflictRead(boolean noVal, boolean needVer) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    if (!noVal) {
                        expVal = -1;

                        cache.put(key, expVal);
                    }

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        if (needVer) {
                            CacheEntry<Integer, Integer> val = cache.getEntry(key);

                            assertEquals(expVal, val == null ? null : val.getValue());
                        }
                        else {
                            Integer val = cache.get(key);

                            assertEquals(expVal, val);
                        }

                        updateKey(cache, key, 1);

                        tx.commit();
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        if (needVer) {
                            CacheEntry<Integer, Integer> val = cache.getEntry(key);

                            assertEquals((Integer)1, val.getValue());
                        }
                        else {
                            Object val = cache.get(key);

                            assertEquals(1, val);
                        }

                        tx.commit();
                    }

                    checkValue(key, 1, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadWrite1() throws Exception {
        txConflictReadWrite(true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadWrite2() throws Exception {
        txConflictReadWrite(false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadRemove1() throws Exception {
        txConflictReadWrite(true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadRemove2() throws Exception {
        txConflictReadWrite(false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadEntryWrite1() throws Exception {
        txConflictReadWrite(true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadEntryWrite2() throws Exception {
        txConflictReadWrite(false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadEntryRemove1() throws Exception {
        txConflictReadWrite(true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadEntryRemove2() throws Exception {
        txConflictReadWrite(false, true, true);
    }

    /**
     * @param noVal If {@code true} there is no cache value when read in tx.
     * @param rmv If {@code true} tests remove, otherwise put.
     * @throws Exception If failed.
     */
    private void txConflictReadWrite(boolean noVal, boolean rmv, boolean needVer) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    if (!noVal) {
                        expVal = -1;

                        cache.put(key, expVal);
                    }

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            if (needVer) {
                                CacheEntry<Integer, Integer> val = cache.getEntry(key);

                                assertEquals(expVal, val == null ? null : val.getValue());
                            }
                            else {
                                Integer val = cache.get(key);

                                assertEquals(expVal, val);
                            }

                            updateKey(cache, key, 1);

                            if (rmv)
                                cache.remove(key);
                            else
                                cache.put(key, 2);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        if (needVer) {
                            CacheEntry<Integer, Integer> val = cache.getEntry(key);

                            assertEquals(1, (Object)val.getValue());
                        }
                        else {
                            Integer val = cache.get(key);

                            assertEquals(1, (Object)val);
                        }

                        if (rmv)
                            cache.remove(key);
                        else
                            cache.put(key, 2);

                        tx.commit();
                    }

                    checkValue(key, rmv ? null : 2, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReadWrite3() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

            List<Integer> readKeys = new ArrayList<>();
            List<Integer> writeKeys = new ArrayList<>();

            readKeys.add(primaryKey(cache));
            writeKeys.add(primaryKeys(cache, 1, 1000_0000).get(0));

            if (ccfg.getBackups() > 0) {
                readKeys.add(backupKey(cache));
                writeKeys.add(backupKeys(cache, 1, 1000_0000).get(0));
            }

            if (ccfg.getCacheMode() == PARTITIONED) {
                readKeys.add(nearKey(cache));
                writeKeys.add(nearKeys(cache, 1, 1000_0000).get(0));
            }

            try {
                for (Integer readKey : readKeys) {
                    for (Integer writeKey : writeKeys) {
                        try {
                            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                cache.get(readKey);

                                cache.put(writeKey, writeKey);

                                updateKey(cache, readKey, 0);

                                tx.commit();
                            }

                            fail();
                        }
                        catch (TransactionOptimisticException ignored) {
                            // Expected exception.
                        }

                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            cache.get(readKey);

                            cache.put(writeKey, writeKey);

                            tx.commit();
                        }
                    }
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictGetAndPut1() throws Exception {
        txConflictGetAndPut(true, false);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictGetAndPut2() throws Exception {
        txConflictGetAndPut(false, false);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictGetAndRemove1() throws Exception {
        txConflictGetAndPut(true, true);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictGetAndRemove2() throws Exception {
        txConflictGetAndPut(false, true);
    }

    /**
     * @param noVal If {@code true} there is no cache value when read in tx.
     * @param rmv If {@code true} tests remove, otherwise put.
     * @throws Exception If failed.
     */
    private void txConflictGetAndPut(boolean noVal, boolean rmv) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    if (!noVal) {
                        expVal = -1;

                        cache.put(key, expVal);
                    }

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = rmv ? cache.getAndRemove(key) : cache.getAndPut(key, 2);

                            assertEquals(expVal, val);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object val = rmv ? cache.getAndRemove(key) : cache.getAndPut(key, 2);

                        assertEquals(1, val);

                        tx.commit();
                    }

                    checkValue(key, rmv ? null : 2, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictInvoke1() throws Exception {
        txConflictInvoke(true, false);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictInvoke2() throws Exception {
        txConflictInvoke(false, false);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictInvoke3() throws Exception {
        txConflictInvoke(true, true);
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictInvoke4() throws Exception {
        txConflictInvoke(false, true);
    }

    /**
     * @param noVal If {@code true} there is no cache value when read in tx.
     * @param rmv If {@code true} invoke does remove value, otherwise put.
     * @throws Exception If failed.
     */
    private void txConflictInvoke(boolean noVal, boolean rmv) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    Integer expVal = null;

                    if (!noVal) {
                        expVal = -1;

                        cache.put(key, expVal);
                    }

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Integer val = cache.invoke(key, new SetValueProcessor(rmv ? null : 2));

                            assertEquals(expVal, val);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object val = cache.invoke(key, new SetValueProcessor(rmv ? null : 2));

                        assertEquals(1, val);

                        tx.commit();
                    }

                    checkValue(key, rmv ? null : 2, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed
     */
    public void testTxConflictInvokeAll() throws Exception {
        Ignite ignite0 = ignite(0);

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache0 = ignite0.createCache(ccfg);

                final Integer key1 = primaryKey(ignite(0).cache(cache0.getName()));
                final Integer key2 = primaryKey(ignite(1).cache(cache0.getName()));

                Map<Integer, Integer> vals = new HashMap<>();

                int newVal = 0;

                for (Ignite ignite : G.allGrids()) {
                    log.info("Test node: " + ignite.name());

                    IgniteTransactions txs = ignite.transactions();

                    IgniteCache<Integer, Integer> cache = ignite.cache(cache0.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Map<Integer, EntryProcessorResult<Integer>> res =
                            cache.invokeAll(F.asSet(key1, key2), new SetValueProcessor(newVal));

                        if (!vals.isEmpty()) {
                            EntryProcessorResult<Integer> res1 = res.get(key1);

                            assertNotNull(res1);
                            assertEquals(vals.get(key1), res1.get());

                            EntryProcessorResult<Integer> res2 = res.get(key2);

                            assertNotNull(res2);
                            assertEquals(vals.get(key2), res2.get());
                        }
                        else
                            assertTrue(res.isEmpty());

                        tx.commit();
                    }

                    checkValue(key1, newVal, cache.getName());
                    checkValue(key2, newVal, cache.getName());

                    vals.put(key1, newVal);
                    vals.put(key2, newVal);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Map<Integer, EntryProcessorResult<Integer>> res =
                                cache.invokeAll(F.asSet(key1, key2), new SetValueProcessor(newVal + 1));

                            EntryProcessorResult<Integer> res1 = res.get(key1);

                            assertNotNull(res1);
                            assertEquals(vals.get(key1), res1.get());

                            EntryProcessorResult<Integer> res2 = res.get(key2);

                            assertNotNull(res2);
                            assertEquals(vals.get(key2), res2.get());

                            updateKey(cache0, key1, -1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key1, -1, cache.getName());
                    checkValue(key2, newVal, cache.getName());

                    vals.put(key1, -1);
                    vals.put(key2, newVal);

                    newVal++;
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictPutIfAbsent() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean put = cache.putIfAbsent(key, 2);

                            assertTrue(put);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean put = cache.putIfAbsent(key, 2);

                        assertFalse(put);

                        tx.commit();
                    }

                    checkValue(key, 1, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean put = cache.putIfAbsent(key, 2);

                        assertTrue(put);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean put = cache.putIfAbsent(key, 2);

                            assertFalse(put);

                            updateKey(cache, key, 3);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 3, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictGetAndPutIfAbsent() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Object old = cache.getAndPutIfAbsent(key, 2);

                            assertNull(old);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object old = cache.getAndPutIfAbsent(key, 2);

                        assertEquals(1, old);

                        tx.commit();
                    }

                    checkValue(key, 1, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object old = cache.getAndPutIfAbsent(key, 2);

                        assertNull(old);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Object old = cache.getAndPutIfAbsent(key, 4);

                            assertEquals(2, old);

                            updateKey(cache, key, 3);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 3, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictReplace() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (final Integer key : keys) {
                    log.info("Test key: " + key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean replace = cache.replace(key, 2);

                            assertFalse(replace);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 2);

                        assertTrue(replace);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 2);

                        assertFalse(replace);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean replace = cache.replace(key, 2);

                            assertFalse(replace);

                            updateKey(cache, key, 3);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 3, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean replace = cache.replace(key, 2);

                            assertTrue(replace);

                            txAsync(cache, OPTIMISTIC, SERIALIZABLE,
                                new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                    @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                        cache.remove(key);

                                        return null;
                                    }
                                }
                            );

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 1);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 2);

                        assertTrue(replace);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictGetAndReplace() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (final Integer key : keys) {
                    log.info("Test key: " + key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Object old = cache.getAndReplace(key, 2);

                            assertNull(old);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object old = cache.getAndReplace(key, 2);

                        assertEquals(1, old);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object old = cache.getAndReplace(key, 2);

                        assertNull(old);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Object old = cache.getAndReplace(key, 2);

                            assertNull(old);

                            updateKey(cache, key, 3);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 3, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Object old = cache.getAndReplace(key, 2);

                            assertEquals(3, old);

                            txAsync(cache, OPTIMISTIC, SERIALIZABLE,
                                new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                    @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                        cache.remove(key);

                                        return null;
                                    }
                                }
                            );

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 1);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object old = cache.getAndReplace(key, 2);

                        assertEquals(1, old);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictRemoveWithOldValue() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (final Integer key : keys) {
                    log.info("Test key: " + key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean rmv = cache.remove(key, 2);

                            assertFalse(rmv);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean rmv = cache.remove(key, 1);

                        assertTrue(rmv);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean rmv = cache.remove(key, 2);

                        assertFalse(rmv);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 2);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean rmv = cache.remove(key, 2);

                            assertTrue(rmv);

                            updateKey(cache, key, 3);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 3, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean rmv = cache.remove(key, 3);

                            assertTrue(rmv);

                            txAsync(cache, OPTIMISTIC, SERIALIZABLE,
                                new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                    @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                        cache.remove(key);

                                        return null;
                                    }
                                }
                            );

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 1);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean rmv = cache.remove(key, 2);

                        assertFalse(rmv);

                        tx.commit();
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean rmv = cache.remove(key, 1);

                        assertTrue(rmv);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictCasReplace() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (final Integer key : keys) {
                    log.info("Test key: " + key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean replace = cache.replace(key, 1, 2);

                            assertFalse(replace);

                            updateKey(cache, key, 1);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 1, 2);

                        assertTrue(replace);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 1, 2);

                        assertFalse(replace);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 2);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean replace = cache.replace(key, 2, 1);

                            assertTrue(replace);

                            updateKey(cache, key, 3);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, 3, cache.getName());

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            boolean replace = cache.replace(key, 3, 4);

                            assertTrue(replace);

                            txAsync(cache, OPTIMISTIC, SERIALIZABLE,
                                new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                    @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                        cache.remove(key);

                                        return null;
                                    }
                                }
                            );

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 1);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 2, 3);

                        assertFalse(replace);

                        tx.commit();
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean replace = cache.replace(key, 1, 3);

                        assertTrue(replace);

                        tx.commit();
                    }

                    checkValue(key, 3, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictRemoveReturnBoolean1() throws Exception {
        txConflictRemoveReturnBoolean(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxConflictRemoveReturnBoolean2() throws Exception {
        txConflictRemoveReturnBoolean(true);
    }

    /**
     * @param noVal If {@code true} there is no cache value when do update in tx.
     * @throws Exception If failed.
     */
    private void txConflictRemoveReturnBoolean(boolean noVal) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (final Integer key : keys) {
                    log.info("Test key: " + key);

                    if (!noVal)
                        cache.put(key, -1);

                    if (noVal) {
                        try {
                            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                boolean res = cache.remove(key);

                                assertFalse(res);

                                updateKey(cache, key, -1);

                                tx.commit();
                            }

                            fail();
                        }
                        catch (TransactionOptimisticException e) {
                            log.info("Expected exception: " + e);
                        }

                        checkValue(key, -1, cache.getName());
                    }
                    else {
                        try {
                            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                boolean res = cache.remove(key);

                                assertTrue(res);

                                txAsync(cache, PESSIMISTIC, REPEATABLE_READ,
                                    new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                        @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                            cache.remove(key);

                                            return null;
                                        }
                                    }
                                );

                                tx.commit();
                            }

                            fail();
                        }
                        catch (TransactionOptimisticException e) {
                            log.info("Expected exception: " + e);
                        }

                        checkValue(key, null, cache.getName());

                        cache.put(key, -1);
                    }

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean res = cache.remove(key);

                        assertTrue(res);

                        updateKey(cache, key, 2);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    // Check no conflict for removeAll with single key.
                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache.removeAll(Collections.singleton(key));

                        txAsync(cache, PESSIMISTIC, REPEATABLE_READ,
                            new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                                @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                    cache.remove(key);

                                    return null;
                                }
                            }
                        );

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    cache.put(key, 2);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean res = cache.remove(key);

                        assertTrue(res);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean res = cache.remove(key);

                        assertFalse(res);

                        tx.commit();
                    }

                    checkValue(key, null, cache.getName());

                    try {
                        cache.put(key, 1);

                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            Object val = cache.get(key);

                            assertEquals(1, val);

                            boolean res = cache.remove(key);

                            assertTrue(res);

                            updateKey(cache, key, 2);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictPut1() throws Exception {
        txNoConflictUpdate(true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictPut2() throws Exception {
        txNoConflictUpdate(false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictPut3() throws Exception {
        txNoConflictUpdate(false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictRemove1() throws Exception {
        txNoConflictUpdate(true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictRemove2() throws Exception {
        txNoConflictUpdate(false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictRemove3() throws Exception {
        txNoConflictUpdate(false, true, true);
    }

    /**
     * @throws Exception If failed.
     * @param noVal If {@code true} there is no cache value when do update in tx.
     * @param rmv If {@code true} tests remove, otherwise put.
     * @param getAfterUpdate If {@code true} tries to get value in tx after update.
     */
    private void txNoConflictUpdate(boolean noVal, boolean rmv, boolean getAfterUpdate) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    if (!noVal)
                        cache.put(key, -1);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        if (rmv)
                            cache.remove(key);
                        else
                            cache.put(key, 2);

                        if (getAfterUpdate) {
                            Object val = cache.get(key);

                            if (rmv)
                                assertNull(val);
                            else
                                assertEquals(2, val);
                        }

                        if (!rmv)
                            updateKey(cache, key, 1);

                        tx.commit();
                    }

                    checkValue(key, rmv ? null : 2, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache.put(key, 3);

                        tx.commit();
                    }

                    checkValue(key, 3, cache.getName());
                }

                Map<Integer, Integer> map = new HashMap<>();

                for (int i = 0; i < 100; i++)
                    map.put(i, i);

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    if (rmv)
                        cache.removeAll(map.keySet());
                    else
                        cache.putAll(map);

                    if (getAfterUpdate) {
                        Map<Integer, Integer> res = cache.getAll(map.keySet());

                        if (rmv) {
                            for (Integer key : map.keySet())
                                assertNull(res.get(key));
                        }
                        else {
                            for (Integer key : map.keySet())
                                assertEquals(map.get(key), res.get(key));
                        }
                    }

                    txAsync(cache, PESSIMISTIC, REPEATABLE_READ,
                        new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
                            @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                                Map<Integer, Integer> map = new HashMap<>();

                                for (int i = 0; i < 100; i++)
                                    map.put(i, -1);

                                cache.putAll(map);

                                return null;
                            }
                        }
                    );

                    tx.commit();
                }

                for (int i = 0; i < 100; i++)
                    checkValue(i, rmv ? null : i, cache.getName());
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictContainsKey1() throws Exception {
        txNoConflictContainsKey(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoConflictContainsKey2() throws Exception {
        txNoConflictContainsKey(true);
    }

    /**
     * @param noVal If {@code true} there is no cache value when do update in tx.
     * @throws Exception If failed.
     */
    private void txNoConflictContainsKey(boolean noVal) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    if (!noVal)
                        cache.put(key, -1);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean res = cache.containsKey(key);

                        assertEquals(!noVal, res);

                        updateKey(cache, key, 1);

                        tx.commit();
                    }

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean res = cache.containsKey(key);

                        assertTrue(res);

                        updateKey(cache, key, 2);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean res = cache.containsKey(key);

                        assertTrue(res);

                        tx.commit();
                    }

                    cache.remove(key);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        boolean res = cache.containsKey(key);

                        assertFalse(res);

                        updateKey(cache, key, 3);

                        tx.commit();
                    }

                    checkValue(key, 3, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxRollbackIfLocked1() throws Exception {
        Ignite ignite0 = ignite(0);

        IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                List<Integer> keys = testKeys(cache);

                for (Integer key : keys) {
                    log.info("Test key: " + key);

                    CountDownLatch latch = new CountDownLatch(1);

                    IgniteInternalFuture<?> fut = lockKey(latch, cache, key);

                    try {
                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            cache.put(key, 2);

                            log.info("Commit");

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    latch.countDown();

                    fut.get();

                    checkValue(key, 1, cache.getName());

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache.put(key, 2);

                        tx.commit();
                    }

                    checkValue(key, 2, cache.getName());
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxRollbackIfLocked2() throws Exception {
        rollbackIfLockedPartialLock(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxRollbackIfLocked3() throws Exception {
        rollbackIfLockedPartialLock(true);
    }

    /**
     * @param locKey If {@code true} gets lock for local key.
     * @throws Exception If failed.
     */
    private void rollbackIfLockedPartialLock(boolean locKey) throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                final Integer key1 = primaryKey(ignite(1).cache(cache.getName()));
                final Integer key2 = locKey ? primaryKey(cache) : primaryKey(ignite(2).cache(cache.getName()));

                CountDownLatch latch = new CountDownLatch(1);

                IgniteInternalFuture<?> fut = lockKey(latch, cache, key1);

                try {
                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache.put(key1, 2);
                        cache.put(key2, 2);

                        tx.commit();
                    }

                    fail();
                }
                catch (TransactionOptimisticException e) {
                    log.info("Expected exception: " + e);
                }

                latch.countDown();

                fut.get();

                checkValue(key1, 1, cache.getName());
                checkValue(key2, null, cache.getName());

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(key1, 2);
                    cache.put(key2, 2);

                    tx.commit();
                }

                checkValue(key1, 2, cache.getName());
                checkValue(key2, 2, cache.getName());
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoReadLockConflict() throws Exception {
        checkNoReadLockConflict(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoReadLockConflictGetEntry() throws Exception {
        checkNoReadLockConflict(true);
    }

    /**
     * @param entry If {@code true} then uses 'getEntry' to read value, otherwise uses 'get'.
     * @throws Exception If failed.
     */
    private void checkNoReadLockConflict(final boolean entry) throws Exception {
        Ignite ignite0 = ignite(0);

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            final AtomicInteger putKey = new AtomicInteger(1_000_000);

            ignite0.createCache(ccfg);

            CacheConfiguration<Integer, Integer> readCacheCcfg = new CacheConfiguration<>(ccfg);

            readCacheCcfg.setName(ccfg.getName() + "-read");

            ignite0.createCache(readCacheCcfg);

            try {
                checkNoReadLockConflict(ignite(0), ccfg.getName(), ccfg.getName(), entry, putKey);

                checkNoReadLockConflict(ignite(1), ccfg.getName(), ccfg.getName(), entry, putKey);

                checkNoReadLockConflict(ignite(SRVS), ccfg.getName(), ccfg.getName(), entry, putKey);

                checkNoReadLockConflict(ignite(0), readCacheCcfg.getName(), ccfg.getName(), entry, putKey);

                checkNoReadLockConflict(ignite(1), readCacheCcfg.getName(), ccfg.getName(), entry, putKey);

                checkNoReadLockConflict(ignite(SRVS), readCacheCcfg.getName(), ccfg.getName(), entry, putKey);
            }
            finally {
                destroyCache(ccfg.getName());

                destroyCache(readCacheCcfg.getName());
            }
        }
    }

    /**
     * @param ignite Node.
     * @param readCacheName Cache name for get.
     * @param writeCacheName Cache name for put.
     * @param entry If {@code true} then uses 'getEntry' to read value, otherwise uses 'get'.
     * @param putKey Write key counter.
     * @throws Exception If failed.
     */
    private void checkNoReadLockConflict(final Ignite ignite,
        String readCacheName,
        String writeCacheName,
        final boolean entry,
        final AtomicInteger putKey) throws Exception
    {
        final int THREADS = 64;

        final IgniteCache<Integer, Integer> readCache = ignite.cache(readCacheName);
        final IgniteCache<Integer, Integer> writeCache = ignite.cache(writeCacheName);

        List<Integer> readKeys = testKeys(readCache);

        for (final Integer readKey : readKeys) {
            final CyclicBarrier barrier = new CyclicBarrier(THREADS);

            readCache.put(readKey, Integer.MIN_VALUE);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                        if (entry)
                            readCache.get(readKey);
                        else
                            readCache.getEntry(readKey);

                        barrier.await();

                        writeCache.put(putKey.incrementAndGet(), 0);

                        tx.commit();
                    }

                    return null;
                }
            }, THREADS, "test-thread");

            assertEquals((Integer)Integer.MIN_VALUE, readCache.get(readKey));

            readCache.put(readKey, readKey);

            assertEquals(readKey, readCache.get(readKey));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoReadLockConflictMultiNode() throws Exception {
        Ignite ignite0 = ignite(0);

        for (final CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            final AtomicInteger putKey = new AtomicInteger(1_000_000);

            ignite0.createCache(ccfg);

            try {
                final int THREADS = 64;

                IgniteCache<Integer, Integer> cache0 = ignite0.cache(ccfg.getName());

                List<Integer> readKeys = testKeys(cache0);

                for (final Integer readKey : readKeys) {
                    final CyclicBarrier barrier = new CyclicBarrier(THREADS);

                    cache0.put(readKey, Integer.MIN_VALUE);

                    final AtomicInteger idx = new AtomicInteger();

                    GridTestUtils.runMultiThreaded(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            Ignite ignite = ignite(idx.incrementAndGet() % (CLIENTS + SRVS));

                            IgniteCache<Integer, Integer> cache = ignite.cache(ccfg.getName());

                            try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                                cache.get(readKey);

                                barrier.await();

                                cache.put(putKey.incrementAndGet(), 0);

                                tx.commit();
                            }

                            return null;
                        }
                    }, THREADS, "test-thread");

                    assertEquals((Integer)Integer.MIN_VALUE, cache0.get(readKey));

                    cache0.put(readKey, readKey);

                    assertEquals(readKey, cache0.get(readKey));
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("UnnecessaryLocalVariable")
    public void testReadLockPessimisticTxConflict() throws Exception {
        Ignite ignite0 = ignite(0);

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            ignite0.createCache(ccfg);

            try {
                Ignite ignite = ignite0;

                IgniteCache<Integer, Integer> cache = ignite.cache(ccfg.getName());

                Integer writeKey = Integer.MAX_VALUE;

                List<Integer> readKeys = testKeys(cache);

                for (Integer readKey : readKeys) {
                    CountDownLatch latch = new CountDownLatch(1);

                    IgniteInternalFuture<?> fut = lockKey(latch, cache, readKey);

                    try {
                        // No conflict for write, conflict with pessimistic tx for read.
                        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                            cache.put(writeKey, writeKey);

                            cache.get(readKey);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }
                    finally {
                        latch.countDown();
                    }

                    fut.get();
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("UnnecessaryLocalVariable")
    public void testReadWriteTxConflict() throws Exception {
        Ignite ignite0 = ignite(0);

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            ignite0.createCache(ccfg);

            try {
                Ignite ignite = ignite0;

                IgniteCache<Integer, Integer> cache = ignite.cache(ccfg.getName());

                Integer writeKey = Integer.MAX_VALUE;

                List<Integer> readKeys = testKeys(cache);

                for (Integer readKey : readKeys) {
                    try {
                        // No conflict for read, conflict for write.
                        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                            cache.getAndPut(writeKey, writeKey);

                            cache.get(readKey);

                            updateKey(cache, writeKey, writeKey + readKey);

                            tx.commit();
                        }

                        fail();
                    }
                    catch (TransactionOptimisticException e) {
                        log.info("Expected exception: " + e);
                    }

                    assertEquals((Integer)(writeKey + readKey), cache.get(writeKey));
                    assertNull(cache.get(readKey));

                    cache.put(readKey, readKey);

                    assertEquals(readKey, cache.get(readKey));
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadWriteTransactionsNoDeadlock() throws Exception {
        checkReadWriteTransactionsNoDeadlock(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadWriteTransactionsNoDeadlockMultinode() throws Exception {
        checkReadWriteTransactionsNoDeadlock(true);
    }

    /**
     * @param multiNode Multi-node test flag.
     * @throws Exception If failed.
     */
    private void checkReadWriteTransactionsNoDeadlock(final boolean multiNode) throws Exception {
        final Ignite ignite0 = ignite(0);

        for (final CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            ignite0.createCache(ccfg);

            try {
                final long stopTime = U.currentTimeMillis() + 10_000;

                final AtomicInteger idx = new AtomicInteger();

                GridTestUtils.runMultiThreaded(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Ignite ignite = multiNode ? ignite(idx.incrementAndGet() % (SRVS + CLIENTS)) : ignite0;

                        IgniteCache<Integer, Integer> cache = ignite.cache(ccfg.getName());

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (U.currentTimeMillis() < stopTime) {
                            try {
                                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                                    for (int i = 0; i < 10; i++) {
                                        Integer key = rnd.nextInt(30);

                                        if (rnd.nextBoolean())
                                            cache.get(key);
                                        else
                                            cache.put(key, key);
                                    }

                                    tx.commit();
                                }
                            }
                            catch (TransactionOptimisticException ignore) {
                                // No-op.
                            }
                        }

                        return null;
                    }
                }, 32, "test-thread");
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadWriteAccountTx() throws Exception {
        final CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED,
            FULL_SYNC,
            1,
            false,
            false);

        ignite(0).createCache(ccfg);

        try {
            final int ACCOUNTS = 50;
            final int VAL_PER_ACCOUNT = 1000;

            IgniteCache<Integer, Account> cache0 = ignite(0).cache(ccfg.getName());

            final Set<Integer> keys = new HashSet<>();

            for (int i = 0; i < ACCOUNTS; i++) {
                cache0.put(i, new Account(VAL_PER_ACCOUNT));

                keys.add(i);
            }

            final List<Ignite> clients = clients();

            final AtomicBoolean stop = new AtomicBoolean();

            final AtomicInteger idx = new AtomicInteger();

            IgniteInternalFuture<?> readFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try {
                        int threadIdx = idx.getAndIncrement();

                        int nodeIdx = threadIdx % (SRVS + CLIENTS);

                        Ignite node = ignite(nodeIdx);

                        IgniteCache<Integer, Account> cache = node.cache(ccfg.getName());

                        IgniteTransactions txs = node.transactions();

                        Integer putKey = ACCOUNTS + threadIdx;

                        while (!stop.get()) {
                            int sum;

                            while (true) {
                                sum = 0;

                                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                    Map<Integer, Account> data = cache.getAll(keys);

                                    for (int i = 0; i < ACCOUNTS; i++) {
                                        Account account = data.get(i);

                                        assertNotNull(account);

                                        sum += account.value();
                                    }

                                    cache.put(putKey, new Account(sum));

                                    tx.commit();
                                }
                                catch (TransactionOptimisticException ignored) {
                                    continue;
                                }

                                break;
                            }

                            assertEquals(ACCOUNTS * VAL_PER_ACCOUNT, sum);
                        }

                        return null;
                    }
                    catch (Throwable e) {
                        stop.set(true);

                        log.error("Unexpected error: " + e);

                        throw e;
                    }
                }
            }, (SRVS + CLIENTS) * 2, "update-thread");

            IgniteInternalFuture<?> updateFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try {
                        int nodeIdx = idx.getAndIncrement() % clients.size();

                        Ignite node = clients.get(nodeIdx);

                        IgniteCache<Integer, Account> cache = node.cache(ccfg.getName());

                        IgniteTransactions txs = node.transactions();

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (!stop.get()) {
                            int id1 = rnd.nextInt(ACCOUNTS);

                            int id2 = rnd.nextInt(ACCOUNTS);

                            while (id2 == id1)
                                id2 = rnd.nextInt(ACCOUNTS);

                            while (true) {
                                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                    Account a1 = cache.get(id1);
                                    Account a2 = cache.get(id2);

                                    assertNotNull(a1);
                                    assertNotNull(a2);

                                    if (a1.value() > 0) {
                                        a1 = new Account(a1.value() - 1);
                                        a2 = new Account(a2.value() + 1);
                                    }

                                    cache.put(id1, a1);
                                    cache.put(id2, a2);

                                    tx.commit();
                                }
                                catch (TransactionOptimisticException ignored) {
                                    continue;
                                }

                                break;
                            }
                        }

                        return null;
                    }
                    catch (Throwable e) {
                        stop.set(true);

                        log.error("Unexpected error: " + e);

                        throw e;
                    }
                }
            }, 2, "update-thread");

            try {
                U.sleep(15_000);
            }
            finally {
                stop.set(true);
            }

            readFut.get();
            updateFut.get();
            int sum = 0;

            for (int i = 0; i < ACCOUNTS; i++) {
                Account a = cache0.get(i);

                assertNotNull(a);
                assertTrue(a.value() >= 0);

                log.info("Account: " + a.value());

                sum += a.value();
            }

            assertEquals(ACCOUNTS * VAL_PER_ACCOUNT, sum);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearCacheReaderUpdate() throws Exception {
        Ignite ignite0 = ignite(0);

        IgniteCache<Integer, Integer> cache0 =
            ignite0.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));

        final String cacheName = cache0.getName();

        try {
            Ignite client1 = ignite(SRVS);
            Ignite client2 = ignite(SRVS + 1);

            IgniteCache<Integer, Integer> cache1 = client1.createNearCache(cacheName,
                new NearCacheConfiguration<Integer, Integer>());
            IgniteCache<Integer, Integer> cache2 = client2.createNearCache(cacheName,
                new NearCacheConfiguration<Integer, Integer>());

            Integer key = primaryKey(ignite(0).cache(cacheName));

            try (Transaction tx = client1.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                assertNull(cache1.get(key));
                cache1.put(key, 1);

                tx.commit();
            }

            try (Transaction tx = client2.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                assertEquals(1, (Object) cache2.get(key));
                cache2.put(key, 2);

                tx.commit();
            }

            try (Transaction tx = client1.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                assertEquals(2, (Object)cache1.get(key));
                cache1.put(key, 3);

                tx.commit();
            }
        }
        finally {
            destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackNearCache1() throws Exception {
        rollbackNearCacheWrite(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackNearCache2() throws Exception {
        rollbackNearCacheWrite(false);
    }

    /**
     * @param near If {@code true} locks entry using the same near cache.
     * @throws Exception If failed.
     */
    private void rollbackNearCacheWrite(boolean near) throws Exception {
        Ignite ignite0 = ignite(0);

        IgniteCache<Integer, Integer> cache0 =
            ignite0.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));

        final String cacheName = cache0.getName();

        try {
            Ignite ignite = ignite(SRVS);

            IgniteCache<Integer, Integer> cache = ignite.createNearCache(cacheName,
                new NearCacheConfiguration<Integer, Integer>());

            IgniteTransactions txs = ignite.transactions();

            Integer key1 = primaryKey(ignite(0).cache(cacheName));
            Integer key2 = primaryKey(ignite(1).cache(cacheName));
            Integer key3 = primaryKey(ignite(2).cache(cacheName));

            CountDownLatch latch = new CountDownLatch(1);

            IgniteInternalFuture<?> fut = null;

            try {
                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(key1, key1);
                    cache.put(key2, key2);
                    cache.put(key3, key3);

                    fut = lockKey(latch, near ? cache : cache0, key2);

                    tx.commit();
                }

                fail();
            }
            catch (TransactionOptimisticException e) {
                log.info("Expected exception: " + e);
            }

            latch.countDown();

            assert fut != null;

            fut.get();

            checkValue(key1, null, cacheName);
            checkValue(key2, 1, cacheName);
            checkValue(key3, null, cacheName);

            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                cache.put(key1, key1);
                cache.put(key2, key2);
                cache.put(key3, key3);

                tx.commit();
            }

            checkValue(key1, key1, cacheName);
            checkValue(key2, key2, cacheName);
            checkValue(key3, key3, cacheName);
        }
        finally {
            destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackNearCache3() throws Exception {
        rollbackNearCacheRead(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackNearCache4() throws Exception {
        rollbackNearCacheRead(false);
    }

    /**
     * @param near If {@code true} updates entry using the same near cache.
     * @throws Exception If failed.
     */
    private void rollbackNearCacheRead(boolean near) throws Exception {
        Ignite ignite0 = ignite(0);

        IgniteCache<Integer, Integer> cache0 =
            ignite0.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));

        final String cacheName = cache0.getName();

        try {
            Ignite ignite = ignite(SRVS);

            IgniteCache<Integer, Integer> cache = ignite.createNearCache(cacheName,
                new NearCacheConfiguration<Integer, Integer>());

            IgniteTransactions txs = ignite.transactions();

            Integer key1 = primaryKey(ignite(0).cache(cacheName));
            Integer key2 = primaryKey(ignite(1).cache(cacheName));
            Integer key3 = primaryKey(ignite(2).cache(cacheName));

            cache0.put(key1, -1);
            cache0.put(key2, -1);
            cache0.put(key3, -1);

            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                cache.get(key1);
                cache.get(key2);
                cache.get(key3);

                updateKey(near ? cache : cache0, key2, -2);

                tx.commit();
            }

            checkValue(key1, -1, cacheName);
            checkValue(key2, -2, cacheName);
            checkValue(key3, -1, cacheName);

            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                cache.put(key1, key1);
                cache.put(key2, key2);
                cache.put(key3, key3);

                tx.commit();
            }

            checkValue(key1, key1, cacheName);
            checkValue(key2, key2, cacheName);
            checkValue(key3, key3, cacheName);
        }
        finally {
            destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTx() throws Exception {
        Ignite ignite0 = ignite(0);

        final String CACHE1 = "cache1";
        final String CACHE2 = "cache2";

        try {
            CacheConfiguration<Integer, Integer> ccfg1 =
                cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false);

            ccfg1.setName(CACHE1);

            ignite0.createCache(ccfg1);

            CacheConfiguration<Integer, Integer> ccfg2=
                cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false);

            ccfg2.setName(CACHE2);

            ignite0.createCache(ccfg2);

            Integer newVal = 0;

            List<Integer> keys = testKeys(ignite0.<Integer, Integer>cache(CACHE1));

            for (Ignite ignite : G.allGrids()) {
                log.info("Test node: " + ignite.name());

                IgniteCache<Integer, Integer> cache1 = ignite.cache(CACHE1);
                IgniteCache<Integer, Integer> cache2 = ignite.cache(CACHE2);

                IgniteTransactions txs = ignite.transactions();

                for (Integer key : keys) {
                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache1.put(key, newVal);
                        cache2.put(key, newVal);

                        tx.commit();
                    }

                    checkValue(key, newVal, CACHE1);
                    checkValue(key, newVal, CACHE2);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object val1 = cache1.get(key);
                        Object val2 = cache2.get(key);

                        assertEquals(newVal, val1);
                        assertEquals(newVal, val2);

                        tx.commit();
                    }

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache1.put(key, newVal + 1);
                        cache2.put(key, newVal + 1);

                        tx.rollback();
                    }

                    checkValue(key, newVal, CACHE1);
                    checkValue(key, newVal, CACHE2);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object val1 = cache1.get(key);
                        Object val2 = cache2.get(key);

                        assertEquals(newVal, val1);
                        assertEquals(newVal, val2);

                        cache1.put(key, newVal + 1);
                        cache2.put(key, newVal + 1);

                        tx.commit();
                    }

                    newVal++;

                    checkValue(key, newVal, CACHE1);
                    checkValue(key, newVal, CACHE2);

                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache1.put(key, newVal);
                        cache2.put(-key, newVal);

                        tx.commit();
                    }

                    checkValue(key, newVal, CACHE1);
                    checkValue(-key, null, CACHE1);

                    checkValue(key, newVal, CACHE2);
                    checkValue(-key, newVal, CACHE2);
                }

                newVal++;

                Integer key1 = primaryKey(ignite(0).cache(CACHE1));
                Integer key2 = primaryKey(ignite(1).cache(CACHE1));

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache1.put(key1, newVal);
                    cache1.put(key2, newVal);

                    cache2.put(key1, newVal);
                    cache2.put(key2, newVal);

                    tx.commit();
                }

                checkValue(key1, newVal, CACHE1);
                checkValue(key2, newVal, CACHE1);
                checkValue(key1, newVal, CACHE2);
                checkValue(key2, newVal, CACHE2);

                CountDownLatch latch = new CountDownLatch(1);

                IgniteInternalFuture<?> fut = lockKey(latch, cache1, key1);

                try {
                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache1.put(key1, newVal + 1);
                        cache2.put(key1, newVal + 1);

                        tx.commit();
                    }

                    fail();
                }
                catch (TransactionOptimisticException e) {
                    log.info("Expected exception: " + e);
                }

                latch.countDown();

                fut.get();

                checkValue(key1, 1, CACHE1);
                checkValue(key1, newVal, CACHE2);

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache1.put(key1, newVal + 1);
                    cache2.put(key1, newVal + 1);

                    tx.commit();
                }

                newVal++;

                cache1.put(key2, newVal);
                cache2.put(key2, newVal);

                checkValue(key1, newVal, CACHE1);
                checkValue(key1, newVal, CACHE2);

                latch = new CountDownLatch(1);

                fut = lockKey(latch, cache1, key1);

                try {
                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache1.put(key1, newVal + 1);
                        cache2.put(key2, newVal + 1);

                        tx.commit();
                    }

                    fail();
                }
                catch (TransactionOptimisticException e) {
                    log.info("Expected exception: " + e);
                }

                latch.countDown();

                fut.get();

                checkValue(key1, 1, CACHE1);
                checkValue(key2, newVal, CACHE2);

                try {
                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                        Object val1 = cache1.get(key1);
                        Object val2 = cache2.get(key2);

                        assertEquals(1, val1);
                        assertEquals(newVal, val2);

                        updateKey(cache2, key2, 1);

                        cache1.put(key1, newVal + 1);
                        cache2.put(key2, newVal + 1);

                        tx.commit();
                    }

                    fail();
                }
                catch (TransactionOptimisticException e) {
                    log.info("Expected exception: " + e);
                }

                checkValue(key1, 1, CACHE1);
                checkValue(key2, 1, CACHE2);

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    Object val1 = cache1.get(key1);
                    Object val2 = cache2.get(key2);

                    assertEquals(1, val1);
                    assertEquals(1, val2);

                    cache1.put(key1, newVal + 1);
                    cache2.put(key2, newVal + 1);

                    tx.commit();
                }

                newVal++;

                checkValue(key1, newVal, CACHE1);
                checkValue(key2, newVal, CACHE2);

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    Object val1 = cache1.get(key1);
                    Object val2 = cache2.get(key2);

                    assertEquals(newVal, val1);
                    assertEquals(newVal, val2);

                    updateKey(cache2, key2, newVal);

                    tx.commit();
                }

                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    Object val1 = cache1.get(key1);
                    Object val2 = cache2.get(key2);

                    assertEquals(newVal, val1);
                    assertEquals(newVal, val2);

                    tx.commit();
                }
            }
        }
        finally {
            destroyCache(CACHE1);
            destroyCache(CACHE2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomOperations() throws Exception {
        Ignite ignite0 = ignite(0);

        long stopTime = U.currentTimeMillis() + getTestTimeout() - 30_000;

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            logCacheInfo(ccfg);

            try {
                IgniteCache<Integer, Integer> cache0 = ignite0.createCache(ccfg);

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (Ignite ignite : G.allGrids()) {
                    log.info("Test node: " + ignite.name());

                    IgniteCache<Integer, Integer> cache = ignite.cache(ccfg.getName());

                    IgniteTransactions txs = ignite.transactions();

                    final int KEYS = 100;

                    for (int i = 0; i < 1000; i++) {
                        Integer key1 = rnd.nextInt(KEYS);

                        Integer key2;

                        if (rnd.nextBoolean()) {
                            key2 = rnd.nextInt(KEYS);

                            while (key2.equals(key1))
                                key2 = rnd.nextInt(KEYS);
                        }
                        else
                            key2 = key1 + 1;

                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            randomOperation(rnd, cache, key1);
                            randomOperation(rnd, cache, key2);

                            tx.commit();
                        }

                        if (i % 100 == 0 && U.currentTimeMillis() > stopTime)
                            break;
                    }

                    for (int key = 0; key < KEYS; key++) {
                        Integer val = cache0.get(key);

                        for (int node = 1; node < SRVS + CLIENTS; node++)
                            assertEquals(val, ignite(node).cache(cache.getName()).get(key));
                    }

                    if (U.currentTimeMillis() > stopTime)
                        break;
                }
            }
            finally {
                destroyCache(ccfg.getName());
            }
        }
    }

    /**
     * @param rnd Random.
     * @param cache Cache.
     * @param key Key.
     */
    private void randomOperation(ThreadLocalRandom rnd, IgniteCache<Integer, Integer> cache, Integer key) {
        switch (rnd.nextInt(4)) {
            case 0:
                cache.put(key, rnd.nextInt());

                break;

            case 1:
                cache.remove(key);

                break;

            case 2:
                cache.invoke(key, new SetValueProcessor(rnd.nextBoolean() ? 1 : null));

                break;

            case 3:
                cache.get(key);

                break;

            default:
                assert false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementTxRestart() throws Exception {
        incrementTx(false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementTx1() throws Exception {
        incrementTx(false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementTx2() throws Exception {
        incrementTx(false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementTxNearCache1() throws Exception {
        incrementTx(true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementTxNearCache2() throws Exception {
        incrementTx(true, true, false);
    }

    /**
     * @param nearCache If {@code true} near cache is enabled.
     * @param store If {@code true} cache store is enabled.
     * @param restart If {@code true} restarts one node.
     * @throws Exception If failed.
     */
    private void incrementTx(boolean nearCache, boolean store, final boolean restart) throws Exception {
        final Ignite srv = ignite(1);

        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, store, false);

        final List<Ignite> clients = clients();

        final String cacheName = srv.createCache(ccfg).getName();

        final AtomicBoolean stop = new AtomicBoolean();

        try {
            final List<IgniteCache<Integer, Integer>> caches = new ArrayList<>();

            for (Ignite client : clients) {
                if (nearCache)
                    caches.add(client.createNearCache(cacheName, new NearCacheConfiguration<Integer, Integer>()));
                else
                    caches.add(client.<Integer, Integer>cache(cacheName));
            }

            IgniteInternalFuture<?> restartFut = restart ? restartFuture(stop, null) : null;

            final long stopTime = U.currentTimeMillis() + getTestTimeout() - 30_000;

            for (int i = 0; i < 30; i++) {
                final AtomicInteger cntr = new AtomicInteger();

                final Integer key = i;

                final AtomicInteger threadIdx = new AtomicInteger();

                final int THREADS = 10;

                final CyclicBarrier barrier = new CyclicBarrier(THREADS);

                GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int idx = threadIdx.getAndIncrement() % caches.size();

                        IgniteCache<Integer, Integer> cache = caches.get(idx);

                        Ignite ignite = cache.unwrap(Ignite.class);

                        IgniteTransactions txs = ignite.transactions();

                        log.info("Started update thread: " + ignite.name());

                        barrier.await();

                        for (int i = 0; i < 1000; i++) {
                            if (i % 100 == 0 && U.currentTimeMillis() > stopTime)
                                break;

                            try {
                                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                    Integer val = cache.get(key);

                                    cache.put(key, val == null ? 1 : val + 1);

                                    tx.commit();
                                }

                                cntr.incrementAndGet();
                            }
                            catch (TransactionOptimisticException ignore) {
                                // Retry.
                            }
                            catch (IgniteException | CacheException e) {
                                assertTrue("Unexpected exception [err=" + e + ", cause=" + e.getCause() + ']',
                                    restart && X.hasCause(e, ClusterTopologyCheckedException.class));
                            }
                        }

                        return null;
                    }
                }, THREADS, "update-thread").get();

                log.info("Iteration [iter=" + i + ", val=" + cntr.get() + ']');

                assertTrue(cntr.get() > 0);

                checkValue(key, cntr.get(), cacheName, restart);

                if (U.currentTimeMillis() > stopTime)
                    break;
            }

            stop.set(true);

            if (restartFut != null)
                restartFut.get();
        }
        finally {
            stop.set(true);

            destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementTxMultipleNodeRestart() throws Exception {
        incrementTxMultiple(false, false, true);
    }

    /**
     * @param nearCache If {@code true} near cache is enabled.
     * @param store If {@code true} cache store is enabled.
     * @param restart If {@code true} restarts one node.
     * @throws Exception If failed.
     */
    private void incrementTxMultiple(boolean nearCache, boolean store, final boolean restart) throws Exception {
        final Ignite srv = ignite(1);

        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, store, false);

        final List<Ignite> clients = clients();

        final String cacheName = srv.createCache(ccfg).getName();

        final AtomicBoolean stop = new AtomicBoolean();

        try {
            final List<IgniteCache<Integer, Integer>> caches = new ArrayList<>();

            for (Ignite client : clients) {
                if (nearCache)
                    caches.add(client.createNearCache(cacheName, new NearCacheConfiguration<Integer, Integer>()));
                else
                    caches.add(client.<Integer, Integer>cache(cacheName));
            }

            IgniteInternalFuture<?> restartFut = restart ? restartFuture(stop, null) : null;

            for (int i = 0; i < 20; i += 2) {
                final AtomicInteger cntr = new AtomicInteger();

                final Integer key1 = i;
                final Integer key2 = i + 1;

                final AtomicInteger threadIdx = new AtomicInteger();

                final int THREADS = 10;

                final CyclicBarrier barrier = new CyclicBarrier(THREADS);

                final ConcurrentSkipListSet<Integer> vals1 = new ConcurrentSkipListSet<>();
                final ConcurrentSkipListSet<Integer> vals2 = new ConcurrentSkipListSet<>();

                GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int idx = threadIdx.getAndIncrement() % caches.size();

                        IgniteCache<Integer, Integer> cache = caches.get(idx);

                        Ignite ignite = cache.unwrap(Ignite.class);

                        IgniteTransactions txs = ignite.transactions();

                        log.info("Started update thread: " + ignite.name());

                        barrier.await();

                        for (int i = 0; i < 1000; i++) {
                            try {
                                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                    Integer val1 = cache.get(key1);
                                    Integer val2 = cache.get(key2);

                                    Integer newVal1 = val1 == null ? 1 : val1 + 1;
                                    Integer newVal2 = val2 == null ? 1 : val2 + 1;

                                    cache.put(key1, newVal1);
                                    cache.put(key2, newVal2);

                                    tx.commit();

                                    assertTrue(vals1.add(newVal1));
                                    assertTrue(vals2.add(newVal2));
                                }

                                cntr.incrementAndGet();
                            }
                            catch (TransactionOptimisticException ignore) {
                                // Retry.
                            }
                            catch (IgniteException | CacheException e) {
                                assertTrue("Unexpected exception [err=" + e + ", cause=" + e.getCause() + ']',
                                    restart && X.hasCause(e, ClusterTopologyCheckedException.class));
                            }
                        }

                        return null;
                    }
                }, THREADS, "update-thread").get();

                log.info("Iteration [iter=" + i + ", val=" + cntr.get() + ']');

                assertTrue(cntr.get() > 0);

                checkValue(key1, cntr.get(), cacheName, restart);
                checkValue(key2, cntr.get(), cacheName, restart);
            }

            stop.set(true);

            if (restartFut != null)
                restartFut.get();
        }
        finally {
            stop.set(true);

            destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRemoveTx() throws Exception {
        getRemoveTx(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRemoveTxNearCache1() throws Exception {
        getRemoveTx(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRemoveTxNearCache2() throws Exception {
        getRemoveTx(true, true);
    }

    /**
     * @param nearCache If {@code true} near cache is enabled.
     * @param store If {@code true} cache store is enabled.
     * @throws Exception If failed.
     */
    private void getRemoveTx(boolean nearCache, boolean store) throws Exception {
        long stopTime = U.currentTimeMillis() + getTestTimeout() - 30_000;

        final Ignite ignite0 = ignite(0);

        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, store, false);

        final List<Ignite> clients = clients();

        final String cacheName = ignite0.createCache(ccfg).getName();

        try {
            final List<IgniteCache<Integer, Integer>> caches = new ArrayList<>();

            for (Ignite client : clients) {
                if (nearCache)
                    caches.add(client.createNearCache(cacheName, new NearCacheConfiguration<Integer, Integer>()));
                else
                    caches.add(client.<Integer, Integer>cache(cacheName));
            }

            for (int i = 0; i < 100; i++) {
                if (U.currentTimeMillis() > stopTime)
                    break;

                final AtomicInteger cntr = new AtomicInteger();

                final Integer key = i;

                final AtomicInteger threadIdx = new AtomicInteger();

                final int THREADS = 10;

                final CyclicBarrier barrier = new CyclicBarrier(THREADS);

                final IgniteInternalFuture<?> updateFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int thread = threadIdx.getAndIncrement();

                        int idx = thread % caches.size();

                        IgniteCache<Integer, Integer> cache = caches.get(idx);

                        Ignite ignite = cache.unwrap(Ignite.class);

                        IgniteTransactions txs = ignite.transactions();

                        log.info("Started update thread: " + ignite.name());

                        Thread.currentThread().setName("update-thread-" + ignite.name() + "-" + thread);

                        barrier.await();

                        for (int i = 0; i < 50; i++) {
                            while (true) {
                                try {
                                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                                    boolean rmv = rnd.nextInt(3) == 0;

                                    Integer val;

                                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                        val = cache.get(key);

                                        if (rmv)
                                            cache.remove(key);
                                        else
                                            cache.put(key, val == null ? 1 : val + 1);

                                        tx.commit();

                                        if (rmv) {
                                            if (val != null) {
                                                for (int j = 0; j < val; j++)
                                                    cntr.decrementAndGet();
                                            }
                                        }
                                        else
                                            cntr.incrementAndGet();
                                    }

                                    break;
                                }
                                catch (TransactionOptimisticException ignore) {
                                    // Retry.
                                }
                            }
                        }

                        return null;
                    }
                }, THREADS, "update-thread");

                updateFut.get();

                Integer val = cntr.get();

                log.info("Iteration [iter=" + i + ", val=" + val + ']');

                checkValue(key, val == 0 ? null : val, cacheName);
            }
        }
        finally {
            destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountTx1() throws Exception {
        accountTx(false, false, false, false, TestMemoryMode.HEAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountTx2() throws Exception {
        accountTx(true, false, false, false, TestMemoryMode.HEAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountTxWithNonSerializable() throws Exception {
        accountTx(false, false, true, false, TestMemoryMode.HEAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountTxNearCache() throws Exception {
        accountTx(false, true, false, false, TestMemoryMode.HEAP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountTxOffheapTiered() throws Exception {
        accountTx(false, false, false, false, TestMemoryMode.OFFHEAP_TIERED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountTxNodeRestart() throws Exception {
        accountTx(false, false, false, true, TestMemoryMode.HEAP);
    }

    /**
     * @param getAll If {@code true} uses getAll/putAll in transaction.
     * @param nearCache If {@code true} near cache is enabled.
     * @param nonSer If {@code true} starts threads executing non-serializable transactions.
     * @param restart If {@code true} restarts one node.
     * @param memMode Test memory mode.
     * @throws Exception If failed.
     */
    private void accountTx(final boolean getAll,
        final boolean nearCache,
        final boolean nonSer,
        final boolean restart,
        TestMemoryMode memMode) throws Exception {
        final Ignite srv = ignite(1);

        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false);

        GridTestUtils.setMemoryMode(null, ccfg, memMode, 1, 64);

        final String cacheName = srv.createCache(ccfg).getName();

        try {
            final List<Ignite> clients = clients();

            final int ACCOUNTS = 100;
            final int VAL_PER_ACCOUNT = 10_000;

            IgniteCache<Integer, Account> srvCache = srv.cache(cacheName);

            for (int i = 0; i < ACCOUNTS; i++)
                srvCache.put(i, new Account(VAL_PER_ACCOUNT));

            final AtomicInteger idx = new AtomicInteger();

            final int THREADS = 20;

            final long testTime = 30_000;

            final long stopTime = System.currentTimeMillis() + testTime;

            IgniteInternalFuture<?> nonSerFut = null;

            if (nonSer) {
                nonSerFut = runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        int nodeIdx = idx.getAndIncrement() % clients.size();

                        Ignite node = clients.get(nodeIdx);

                        Thread.currentThread().setName("update-pessimistic-" + node.name());

                        log.info("Pessimistic tx thread: " + node.name());

                        final IgniteTransactions txs = node.transactions();

                        final IgniteCache<Integer, Account> cache =
                            nearCache ? node.createNearCache(cacheName, new NearCacheConfiguration<Integer, Account>()) :
                                node.<Integer, Account>cache(cacheName);

                        assertNotNull(cache);

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (U.currentTimeMillis() < stopTime) {
                            int id1 = rnd.nextInt(ACCOUNTS);

                            int id2 = rnd.nextInt(ACCOUNTS);

                            while (id2 == id1)
                                id2 = rnd.nextInt(ACCOUNTS);

                            if (id1 > id2) {
                                int tmp = id1;
                                id1 = id2;
                                id2 = tmp;
                            }

                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                Account a1 = cache.get(id1);
                                Account a2 = cache.get(id2);

                                assertNotNull(a1);
                                assertNotNull(a2);

                                if (a1.value() > 0) {
                                    a1 = new Account(a1.value() - 1);
                                    a2 = new Account(a2.value() + 1);
                                }

                                cache.put(id1, a1);
                                cache.put(id2, a2);

                                tx.commit();
                            }
                        }

                        return null;
                    }
                }, 10, "non-ser-thread");
            }

            final IgniteInternalFuture<?> fut = runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int nodeIdx = idx.getAndIncrement() % clients.size();

                    Ignite node = clients.get(nodeIdx);

                    Thread.currentThread().setName("update-" + node.name());

                    log.info("Tx thread: " + node.name());

                    final IgniteTransactions txs = node.transactions();

                    final IgniteCache<Integer, Account> cache =
                        nearCache ? node.createNearCache(cacheName, new NearCacheConfiguration<Integer, Account>()) :
                            node.<Integer, Account>cache(cacheName);

                    assertNotNull(cache);

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (U.currentTimeMillis() < stopTime) {
                        int id1 = rnd.nextInt(ACCOUNTS);

                        int id2 = rnd.nextInt(ACCOUNTS);

                        while (id2 == id1)
                            id2 = rnd.nextInt(ACCOUNTS);

                        try {
                            while (true) {
                                try {
                                    try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                        if (getAll) {
                                            Map<Integer, Account> map = cache.getAll(F.asSet(id1, id2));

                                            Account a1 = cache.get(id1);
                                            Account a2 = cache.get(id2);

                                            assertNotNull(a1);
                                            assertNotNull(a2);

                                            if (a1.value() > 0) {
                                                a1 = new Account(a1.value() - 1);
                                                a2 = new Account(a2.value() + 1);
                                            }

                                            map.put(id1, a1);
                                            map.put(id2, a2);

                                            cache.putAll(map);
                                        }
                                        else {
                                            Account a1 = cache.get(id1);
                                            Account a2 = cache.get(id2);

                                            assertNotNull(a1);
                                            assertNotNull(a2);

                                            if (a1.value() > 0) {
                                                a1 = new Account(a1.value() - 1);
                                                a2 = new Account(a2.value() + 1);
                                            }

                                            cache.put(id1, a1);
                                            cache.put(id2, a2);
                                        }

                                        tx.commit();
                                    }

                                    break;
                                }
                                catch (TransactionOptimisticException ignore) {
                                    // Retry.
                                }
                                catch (IgniteException | CacheException e) {
                                    assertTrue("Unexpected exception [err=" + e + ", cause=" + e.getCause() + ']',
                                        restart && X.hasCause(e, ClusterTopologyCheckedException.class));
                                }
                            }
                        }
                        catch (Throwable e) {
                            log.error("Unexpected error: " + e, e);

                            throw e;
                        }
                    }

                    return null;
                }
            }, THREADS, "tx-thread");

            IgniteInternalFuture<?> restartFut = restart ? restartFuture(null, fut) : null;

            fut.get(testTime + 30_000);

            if (nonSerFut != null)
                nonSerFut.get();

            if (restartFut != null)
                restartFut.get();

            int sum = 0;

            for (int i = 0; i < ACCOUNTS; i++) {
                Account a = srvCache.get(i);

                assertNotNull(a);
                assertTrue(a.value() >= 0);

                log.info("Account: " + a.value());

                sum += a.value();
            }

            assertEquals(ACCOUNTS * VAL_PER_ACCOUNT, sum);

            for (int node = 0; node < SRVS + CLIENTS; node++) {
                log.info("Verify node: " + node);

                Ignite ignite = ignite(node);

                IgniteCache<Integer, Account> cache = ignite.cache(cacheName);

                sum = 0;

                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                    Map<Integer, Account> map = new HashMap<>();

                    for (int i = 0; i < ACCOUNTS; i++) {
                        Account a = cache.get(i);

                        assertNotNull(a);

                        map.put(i, a);

                        sum += a.value();
                    }

                    Account a1 = map.get(0);
                    Account a2 = map.get(1);

                    if (a1.value() > 0) {
                        a1 = new Account(a1.value() - 1);
                        a2 = new Account(a2.value() + 1);

                        map.put(0, a1);
                        map.put(1, a2);
                    }

                    cache.putAll(map);

                    tx.commit();
                }

                assertEquals(ACCOUNTS * VAL_PER_ACCOUNT, sum);
            }
        }
        finally {
            destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoOptimisticExceptionOnChangingTopology() throws Exception {
        if (FAST)
            return;

        final AtomicBoolean finished = new AtomicBoolean();

        final List<String> cacheNames = new ArrayList<>();

        Ignite srv = ignite(1);

        try {
            {
                CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false);
                ccfg.setName("cache1");
                ccfg.setRebalanceMode(SYNC);

                srv.createCache(ccfg);

                cacheNames.add(ccfg.getName());
            }

            {
                // Store enabled.
                CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, true, false);
                ccfg.setName("cache2");
                ccfg.setRebalanceMode(SYNC);

                srv.createCache(ccfg);

                cacheNames.add(ccfg.getName());
            }

            {
                // Offheap.
                CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false);
                ccfg.setName("cache3");
                ccfg.setRebalanceMode(SYNC);

                GridTestUtils.setMemoryMode(null, ccfg, TestMemoryMode.OFFHEAP_TIERED, 1, 64);

                srv.createCache(ccfg);

                cacheNames.add(ccfg.getName());
            }

            IgniteInternalFuture<?> restartFut = restartFuture(finished, null);

            List<IgniteInternalFuture<?>> futs = new ArrayList<>();

            final int KEYS_PER_THREAD = 100;

            for (int i = 1; i < SRVS + CLIENTS; i++) {
                final Ignite node = ignite(i);

                final int minKey = i * KEYS_PER_THREAD;
                final int maxKey = minKey + KEYS_PER_THREAD;

                // Threads update non-intersecting keys, optimistic exception should not be thrown.

                futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        try {
                            log.info("Started update thread [node=" + node.name() +
                                ", minKey=" + minKey +
                                ", maxKey=" + maxKey + ']');

                            final ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            List<IgniteCache<Integer, Integer>> caches = new ArrayList<>();

                            for (String cacheName : cacheNames)
                                caches.add(node.<Integer, Integer>cache(cacheName));

                            assertEquals(3, caches.size());

                            int iter = 0;

                            while (!finished.get()) {
                                int keyCnt = rnd.nextInt(1, 10);

                                final Set<Integer> keys = new LinkedHashSet<>();

                                while (keys.size() < keyCnt)
                                    keys.add(rnd.nextInt(minKey, maxKey));

                                for (final IgniteCache<Integer, Integer> cache : caches) {
                                    doInTransaction(node, OPTIMISTIC, SERIALIZABLE, new Callable<Void>() {
                                        @Override public Void call() throws Exception {
                                            for (Integer key : keys)
                                                randomOperation(rnd, cache, key);

                                            return null;
                                        }
                                    });
                                }

                                if (iter % 100 == 0)
                                    log.info("Iteration: " + iter);

                                iter++;
                            }

                            return null;
                        }
                        catch (Throwable e) {
                            log.error("Unexpected error: " + e, e);

                            throw e;
                        }
                    }
                }, "update-thread-" + i));
            }

            U.sleep(60_000);

            finished.set(true);

            restartFut.get();

            for (IgniteInternalFuture<?> fut : futs)
                fut.get();
        }
        finally {
            finished.set(true);

            for (String cacheName : cacheNames)
                destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConflictResolution() throws Exception {
        final Ignite ignite = ignite(0);

        final String cacheName =
            ignite.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false)).getName();

        try {
            final Map<Integer, Integer> keys = new HashMap<>();

            for (int i = 0; i < 500; i++)
                keys.put(i, i);

            final int THREADS = 5;

            for (int i = 0; i < 10; i++) {
                final CyclicBarrier barrier = new CyclicBarrier(THREADS);

                final AtomicInteger commitCntr = new AtomicInteger(0);

                GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                        IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                            cache.putAll(keys);

                            barrier.await();

                            tx.commit();

                            commitCntr.incrementAndGet();
                        }
                        catch (TransactionOptimisticException e) {
                            log.info("Optimistic error: " + e);
                        }

                        return null;
                    }
                }, THREADS, "update-thread").get();

                int commits = commitCntr.get();

                log.info("Iteration [iter=" + i + ", commits=" + commits + ']');

                assertTrue(commits > 0);
            }
        }
        finally {
            destroyCache(cacheName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlock() throws Exception {
        concurrentUpdateNoDeadlock(Collections.singletonList(ignite(0)), 10, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlockGetPut() throws Exception {
        concurrentUpdateNoDeadlock(Collections.singletonList(ignite(0)), 10, true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlockWithNonSerializable() throws Exception {
        concurrentUpdateNoDeadlock(Collections.singletonList(ignite(0)), 10, true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlockNodeRestart() throws Exception {
        concurrentUpdateNoDeadlock(Collections.singletonList(ignite(1)), 10, false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlockFromClients() throws Exception {
        concurrentUpdateNoDeadlock(clients(), 20, false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUpdateNoDeadlockFromClientsNodeRestart() throws Exception {
        concurrentUpdateNoDeadlock(clients(), 20, false, true, false);
    }

    /**
     * @param updateNodes Nodes executing updates.
     * @param threads Number of threads executing updates.
     * @param get If {@code true} gets value in transaction.
     * @param restart If {@code true} restarts one node.
     * @param nonSer If {@code true} starts threads executing non-serializable transactions.
     * @throws Exception If failed.
     */
    private void concurrentUpdateNoDeadlock(final List<Ignite> updateNodes,
        int threads,
        final boolean get,
        final boolean restart,
        final boolean nonSer
    ) throws Exception {
        if (FAST)
            return;

        assert updateNodes.size() > 0;

        final Ignite srv = ignite(1);

        final String cacheName =
            srv.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false)).getName();

        try {
            final int KEYS = 100;

            final AtomicBoolean finished = new AtomicBoolean();

            IgniteInternalFuture<?> fut = restart ? restartFuture(finished, null) : null;

            try {
                for (int i = 0; i < 10; i++) {
                    log.info("Iteration: " + i);

                    final long stopTime = U.currentTimeMillis() + 10_000;

                    final AtomicInteger idx = new AtomicInteger();

                    IgniteInternalFuture<?> nonSerFut = null;

                    if (nonSer) {
                        nonSerFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                int nodeIdx = idx.getAndIncrement() % updateNodes.size();

                                Ignite node = updateNodes.get(nodeIdx);

                                log.info("Non-serializable tx thread: " + node.name());

                                final IgniteCache<Integer, Integer> cache = node.cache(cacheName);

                                assertNotNull(cache);

                                final ThreadLocalRandom rnd = ThreadLocalRandom.current();

                                while (U.currentTimeMillis() < stopTime) {
                                    final TreeMap<Integer, Integer> map = new TreeMap<>();

                                    for (int i = 0; i < KEYS / 2; i++)
                                        map.put(rnd.nextInt(KEYS), rnd.nextInt());

                                    TransactionConcurrency concurrency = rnd.nextBoolean() ? PESSIMISTIC : OPTIMISTIC;

                                    doInTransaction(node, concurrency, REPEATABLE_READ, new Callable<Void>() {
                                        @Override public Void call() throws Exception {
                                            cache.putAll(map);

                                            return null;
                                        }
                                    });
                                }

                                return null;
                            }
                        }, 5, "non-ser-thread");
                    }

                    IgniteInternalFuture<?> updateFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            int nodeIdx = idx.getAndIncrement() % updateNodes.size();

                            Ignite node = updateNodes.get(nodeIdx);

                            log.info("Tx thread: " + node.name());

                            final IgniteTransactions txs = node.transactions();

                            final IgniteCache<Integer, Integer> cache = node.cache(cacheName);

                            assertNotNull(cache);

                            final ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            while (U.currentTimeMillis() < stopTime) {
                                final Map<Integer, Integer> map = new LinkedHashMap<>();

                                for (int i = 0; i < KEYS / 2; i++)
                                    map.put(rnd.nextInt(KEYS), rnd.nextInt());

                                try {
                                    if (restart) {
                                        doInTransaction(node, OPTIMISTIC, SERIALIZABLE, new Callable<Void>() {
                                            @Override public Void call() throws Exception {
                                                if (get) {
                                                    for (Map.Entry<Integer, Integer> e : map.entrySet()) {
                                                        if (rnd.nextBoolean()) {
                                                            cache.get(e.getKey());

                                                            if (rnd.nextBoolean())
                                                                cache.put(e.getKey(), e.getValue());
                                                        }
                                                        else
                                                            cache.put(e.getKey(), e.getValue());
                                                    }
                                                }
                                                else
                                                    cache.putAll(map);

                                                return null;
                                            }
                                        });
                                    }
                                    else {
                                        try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                                            if (get) {
                                                for (Map.Entry<Integer, Integer> e : map.entrySet()) {
                                                    if (rnd.nextBoolean()) {
                                                        cache.get(e.getKey());

                                                        if (rnd.nextBoolean())
                                                            cache.put(e.getKey(), e.getValue());
                                                    }
                                                    else
                                                        cache.put(e.getKey(), e.getValue());
                                                }
                                            }
                                            else
                                                cache.putAll(map);

                                            tx.commit();
                                        }
                                    }
                                }
                                catch (TransactionOptimisticException ignore) {
                                    // No-op.
                                }
                                catch (Throwable e) {
                                    log.error("Unexpected error: " + e, e);

                                    throw e;
                                }
                            }

                            return null;
                        }
                    }, threads, "tx-thread");

                    updateFut.get(60, SECONDS);

                    if (nonSerFut != null)
                        nonSerFut.get(60, SECONDS);

                    IgniteCache<Integer, Integer> cache = srv.cache(cacheName);

                    for (int key = 0; key < KEYS; key++) {
                        Integer val = cache.get(key);

                        for (int node = 1; node < SRVS + CLIENTS; node++)
                            assertEquals(val, ignite(node).cache(cache.getName()).get(key));
                    }
                }

                finished.set(true);

                if (fut != null)
                    fut.get();
            }
            finally {
                finished.set(true);
            }
        }
        finally {
            destroyCache(cacheName);
        }
    }

    /**
     * @return Cache configurations.
     */
    private List<CacheConfiguration<Integer, Integer>> cacheConfigurations() {
        List<CacheConfiguration<Integer, Integer>> ccfgs = new ArrayList<>();

        // No store, no near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, false, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, false, false));
        ccfgs.add(cacheConfiguration(REPLICATED, FULL_SYNC, 0, false, false));

        // Store, no near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, true, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, true, false));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, true, false));
        ccfgs.add(cacheConfiguration(REPLICATED, FULL_SYNC, 0, true, false));

        // No store, near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, false, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, false, true));

        // Store, near.
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, true, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, true, true));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, true, true));

        // Swap and offheap enabled.
        for (GridTestUtils.TestMemoryMode memMode : GridTestUtils.TestMemoryMode.values()) {
            CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, false, false);

            GridTestUtils.setMemoryMode(null, ccfg, memMode, 1, 64);

            ccfgs.add(ccfg);
        }

        return ccfgs;
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void logCacheInfo(CacheConfiguration<?, ?> ccfg) {
        log.info("Test cache [mode=" + ccfg.getCacheMode() +
            ", sync=" + ccfg.getWriteSynchronizationMode() +
            ", backups=" + ccfg.getBackups() +
            ", memMode=" + ccfg.getMemoryMode() +
            ", near=" + (ccfg.getNearConfiguration() != null) +
            ", store=" + ccfg.isWriteThrough() +
            ", evictPlc=" + (ccfg.getEvictionPolicy() != null) +
            ", swap=" + ccfg.isSwapEnabled()  +
            ", maxOffheap=" + ccfg.getOffHeapMaxMemory()  +
            ']');
    }

    /**
     * @param cache Cache.
     * @return Test keys.
     * @throws Exception If failed.
     */
    private List<Integer> testKeys(IgniteCache<Integer, Integer> cache) throws Exception {
        CacheConfiguration ccfg = cache.getConfiguration(CacheConfiguration.class);

        List<Integer> keys = new ArrayList<>();

        if (!cache.unwrap(Ignite.class).configuration().isClientMode()) {
            if (ccfg.getCacheMode() == PARTITIONED)
                keys.add(nearKey(cache));

            keys.add(primaryKey(cache));

            if (ccfg.getBackups() != 0)
                keys.add(backupKey(cache));
        }
        else
            keys.add(nearKey(cache));

        return keys;
    }

    /**
     * @param cache Cache.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolcation.
     * @param c Closure to run in transaction.
     * @throws Exception If failed.
     */
    private void txAsync(final IgniteCache<Integer, Integer> cache,
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation,
        final IgniteClosure<IgniteCache<Integer, Integer>, Void> c) throws Exception {
        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                try (Transaction tx = txs.txStart(concurrency, isolation)) {
                    c.apply(cache);

                    tx.commit();
                }

                return null;
            }
        }, "async-thread");

        fut.get();
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void updateKey(
        final IgniteCache<Integer, Integer> cache,
        final Integer key,
        final Integer val) throws Exception {
        txAsync(cache, PESSIMISTIC, REPEATABLE_READ, new IgniteClosure<IgniteCache<Integer, Integer>, Void>() {
            @Override public Void apply(IgniteCache<Integer, Integer> cache) {
                cache.put(key, val);

                return null;
            }
        });
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     * @param cacheName Cache name.
     */
    private void checkValue(Object key, Object expVal, String cacheName) {
        checkValue(key, expVal, cacheName, false);
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     * @param cacheName Cache name.
     * @param skipFirst If {@code true} skips first node.
     */
    private void checkValue(Object key, Object expVal, String cacheName, boolean skipFirst) {
        for (int i = 0; i < SRVS + CLIENTS; i++) {
            if (skipFirst && i == 0)
                continue;

            IgniteCache<Object, Object> cache = ignite(i).cache(cacheName);

            assertEquals(expVal, cache.get(key));
        }
    }

    /**
     * @param releaseLatch Release lock latch.
     * @param cache Cache.
     * @param key Key.
     * @return Future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture<?> lockKey(
        final CountDownLatch releaseLatch,
        final IgniteCache<Integer, Integer> cache,
        final Integer key) throws Exception {
        final CountDownLatch lockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key, 1);

                    log.info("Locked key: " + key);

                    lockLatch.countDown();

                    assertTrue(releaseLatch.await(100000, SECONDS));

                    log.info("Commit tx: " + key);

                    tx.commit();
                }

                return null;
            }
        }, "lock-thread");

        assertTrue(lockLatch.await(10, SECONDS));

        return fut;
    }

    /**
     * @param cacheName Cache name.
     */
    private void destroyCache(String cacheName) {
        storeMap.clear();

        for (Ignite ignite : G.allGrids()) {
            try {
                ignite.destroyCache(cacheName);
            }
            catch (IgniteException ignore) {
                // No-op.
            }

            GridTestSwapSpaceSpi spi = (GridTestSwapSpaceSpi)ignite.configuration().getSwapSpaceSpi();

            spi.clearAll();
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param storeEnabled If {@code true} adds cache store.
     * @param nearCache If {@code true} near cache is enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean storeEnabled,
        boolean nearCache) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (storeEnabled) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setWriteThrough(true);
            ccfg.setReadThrough(true);
        }

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<Integer, Integer>());

        return ccfg;
    }

    /**
     * @return Client nodes.
     */
    private List<Ignite> clients() {
        List<Ignite> clients = new ArrayList<>();

        for (int i = 0; i < CLIENTS; i++) {
            Ignite ignite = ignite(SRVS + i);

            assertTrue(ignite.configuration().isClientMode());

            clients.add(ignite);
        }

        return clients;
    }

    /**
     * @param stop Stop flag.
     * @param fut Future.
     * @return Restart thread future.
     */
    private IgniteInternalFuture<?> restartFuture(final AtomicBoolean stop, final IgniteInternalFuture<?> fut) {
        return GridTestUtils.runAsync(new Callable<Object>() {
            private boolean stop() {
                if (stop != null)
                    return stop.get();

                return fut.isDone();
            }

            @Override public Object call() throws Exception {
                while (!stop()) {
                    Ignite ignite = startGrid(SRVS + CLIENTS);

                    assertFalse(ignite.configuration().isClientMode());

                    U.sleep(300);

                    stopGrid(SRVS + CLIENTS);
                }

                return null;
            }
        }, "restart-thread");
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Integer, Integer> create() {
            return new CacheStoreAdapter<Integer, Integer>() {
                @Override public Integer load(Integer key) throws CacheLoaderException {
                    return storeMap.get(key);
                }

                @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
                    storeMap.put(entry.getKey(), entry.getValue());
                }

                @Override public void delete(Object key) {
                    storeMap.remove(key);
                }
            };
        }
    }

    /**
     * Sets given value, returns old value.
     */
    public static final class SetValueProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer newVal;

        /**
         * @param newVal New value to set.
         */
        SetValueProcessor(Integer newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments) {
            Integer val = entry.getValue();

            if (newVal == null)
                entry.remove();
            else
                entry.setValue(newVal);

            return val;
        }
    }

    /**
     *
     */
    static class Account {
        /** */
        private final int val;

        /**
         * @param val Value.
         */
        public Account(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Account.class, this);
        }
    }
}
