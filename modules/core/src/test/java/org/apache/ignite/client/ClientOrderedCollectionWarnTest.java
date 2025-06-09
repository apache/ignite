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

package org.apache.ignite.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** */
public class ClientOrderedCollectionWarnTest extends GridCommonAbstractTest {
    /** */
    private static final String WARN_LSNR_MSG = "Unordered map java.util.%s is used for"; //

    /** */
    private static final LogListener LINKED_HASH_MAP_WARN_LSNR =
        LogListener.matches(String.format(WARN_LSNR_MSG, "LinkedHashMap")).times(1).build();

    /** */
    private static final LogListener HASH_MAP_WARN_LSNR =
        LogListener.matches(String.format(WARN_LSNR_MSG, "HashMap")).times(1).build();

    /** */
    private static final List<LogListener> ALL_LSNR = List.of(LINKED_HASH_MAP_WARN_LSNR, HASH_MAP_WARN_LSNR);

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        testLog.registerListener(LINKED_HASH_MAP_WARN_LSNR);
        testLog.registerListener(HASH_MAP_WARN_LSNR);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** */
    private ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration().setAddresses(Config.SERVER).setLogger(testLog);
    }

    /** */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testPutAllAtomic() throws Exception {
        testPutAll(ATOMIC, new HashMap<>(), ALL_LSNR, false);
    }

    /** */
    @Test
    public void testPutAllTransactional() throws Exception {
        testPutAll(TRANSACTIONAL, new TreeMap<>(), ALL_LSNR, false);
    }

    /** */
    @Test
    public void testPutAllTransactionalWarn() throws Exception {
        testPutAll(TRANSACTIONAL, new HashMap<>(), List.of(HASH_MAP_WARN_LSNR), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllExplicitOptimistic() throws Exception {
        testPutAllWithTx(new HashMap<>(), OPTIMISTIC, ALL_LSNR, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllExplicitPessimistic() throws Exception {
        testPutAllWithTx(new HashMap<>(), PESSIMISTIC, List.of(HASH_MAP_WARN_LSNR), true);
    }

    /** */
    @Test
    public void testPutAllConflictAtomic() throws Exception {
        testPutAllConflict(ATOMIC, new HashMap<>(), ALL_LSNR, false);
    }

    /** */
    @Test
    public void testPutAllConflictTransactional() throws Exception {
        testPutAllConflict(TRANSACTIONAL, new TreeMap<>(), ALL_LSNR, false);
    }

    /** */
    @Test
    public void testPutAllConflictTransactionalWarn() throws Exception {
        testPutAllConflict(TRANSACTIONAL, new HashMap<>(), List.of(HASH_MAP_WARN_LSNR), true);
    }

    /** */
    @Test
    public void testRemoveAllConflictAtomic() throws Exception {
        testRemoveAllConflict(ATOMIC, new HashMap<>(), ALL_LSNR, false);
    }

    /** */
    @Test
    public void testRemoveAllConflictTransactional() throws Exception {
        testRemoveAllConflict(TRANSACTIONAL, new TreeMap<>(), ALL_LSNR, false);
    }

    /** */
    @Test
    public void testRemoveAllConflictTransactionalWarn() throws Exception {
        testRemoveAllConflict(TRANSACTIONAL, new HashMap<>(), List.of(HASH_MAP_WARN_LSNR), true);
    }

    /** */
    private void testPutAll(
        CacheAtomicityMode cacheMode,
        Map<Long, Long> map,
        List<LogListener> lsnrs,
        boolean warnPresent
    ) throws Exception {
        Consumer<IgniteClient> cliOp = cli -> createCache(cli, cacheMode).putAll(fillMap(map));

        testStartUp(cliOp, warnPresent, lsnrs);
    }

    /** */
    private void testPutAllWithTx(
        Map<Long, Long> map,
        TransactionConcurrency concurrency,
        List<LogListener> lsnrs,
        boolean warnPresent
    ) throws Exception {
        Consumer<IgniteClient> cliOp = cli -> {
            ClientCache<Long, Long> c = createCache(cli, TRANSACTIONAL);

            ClientTransaction tx = cli.transactions().txStart(concurrency, SERIALIZABLE);

            c.putAll(fillMap(map));

            tx.commit();
            tx.close();
        };

        testStartUp(cliOp, warnPresent, lsnrs);
    }

    /** */
    private void testPutAllConflict(
        CacheAtomicityMode cacheMode,
        Map<Object, T3<Object, GridCacheVersion, Long>> map,
        List<LogListener> lsnrs,
        boolean warnPresent
    ) throws Exception {
        Consumer<IgniteClient> cliOp = cli -> ((TcpClientCache<Object, Object>)createCache(cli, cacheMode))
            .putAllConflict(fillConflictPutMap(map));

        testStartUp(cliOp, warnPresent, lsnrs);
    }

    /** */
    private void testRemoveAllConflict(
        CacheAtomicityMode cacheMode,
        Map<Object, GridCacheVersion> map,
        List<LogListener> lsnrs,
        boolean warnPresent
    ) throws Exception {
        Consumer<IgniteClient> cliOp = cli -> ((TcpClientCache<Object, Object>)createCache(cli, cacheMode))
            .removeAllConflict(fillConflictRmvMap(map));

        testStartUp(cliOp, warnPresent, lsnrs);
    }

    /** */
    private void testStartUp(Consumer<IgniteClient> cliOp, boolean warnPresent, List<LogListener> lsnrs) throws Exception {
        try (IgniteEx ign = startGrid()) {
            try (IgniteClient cli = Ignition.startClient(getClientConfiguration())) {
                cliOp.accept(cli);
            }

            for (LogListener lsnr : lsnrs) {
                if (warnPresent)
                    assertTrue(waitForCondition(lsnr::check, getTestTimeout()));
                else
                    assertFalse(waitForCondition(lsnr::check, 1_000));
            }
        }
    }

    /** */
    private <K, V> ClientCache<K, V> createCache(IgniteClient cli, CacheAtomicityMode cacheMode) {
        ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(cacheMode);

        return cli.createCache(cacheCfg);
    }

    /** */
    private Map<Long, Long> fillMap(Map<Long, Long> map) {
        map.put(0L, 0L);
        map.put(1L, 1L);

        return map;
    }

    /** */
    private Map<Object, T3<Object, GridCacheVersion, Long>> fillConflictPutMap(Map<Object, T3<Object, GridCacheVersion, Long>> map) {
        GridCacheVersion ver = new GridCacheVersion(1, 1, 1, 1);

        map.put(0L, new T3<>(0L, ver, 0L));
        map.put(1L, new T3<>(1L, ver, 0L));

        return map;
    }

    /** */
    private Map<Object, GridCacheVersion> fillConflictRmvMap(Map<Object, GridCacheVersion> map) {
        GridCacheVersion ver = new GridCacheVersion(1, 1, 1, 1);

        map.put(0L, ver);
        map.put(1L, ver);

        return map;
    }
}
