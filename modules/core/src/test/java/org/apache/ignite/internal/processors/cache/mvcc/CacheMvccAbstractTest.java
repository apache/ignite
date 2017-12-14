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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public abstract class CacheMvccAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    static final int DFLT_PARTITION_COUNT = RendezvousAffinityFunction.DFLT_PARTITION_COUNT;

    /** */
    static final String CRD_ATTR = "testCrd";

    /** */
    static final long DFLT_TEST_TIME = 30_000;

    /** */
    protected static final int PAGE_SIZE = DataStorageConfiguration.DFLT_PAGE_SIZE;

    /** */
    protected static final int SRVS = 4;

    /** */
    protected boolean client;

    /** */
    protected boolean testSpi;

    /** */
    protected String nodeAttr;

    /** */
    protected boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMvccEnabled(true);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        if (nodeAttr != null)
            cfg.setUserAttributes(F.asMap(nodeAttr, true));

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setWalMode(WALMode.LOG_ONLY);
        storageCfg.setPageSize(PAGE_SIZE);

        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        regionCfg.setPersistenceEnabled(persistence);

        storageCfg.setDefaultDataRegionConfiguration(regionCfg);

        cfg.setDataStorageConfiguration(storageCfg);

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DFLT_TEST_TIME + 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        MvccProcessor.coordinatorAssignClosure(null);

        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            verifyCoordinatorInternalState();
        }
        finally {
            stopAllGrids();
        }

        MvccProcessor.coordinatorAssignClosure(null);

        GridTestUtils.deleteDbFiles();

        super.afterTest();
    }

    /**
     * @param cfgC Optional closure applied to cache configuration.
     * @throws Exception If failed.
     */
    final void cacheRecreate(@Nullable IgniteInClosure<CacheConfiguration> cfgC) throws Exception {
        Ignite srv0 = startGrid(0);

        final int PARTS = 64;

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, PARTS);

        if (cfgC != null)
            cfgC.apply(ccfg);

        IgniteCache<Integer, MvccTestAccount> cache = (IgniteCache)srv0.createCache(ccfg);

        for (int k = 0; k < PARTS * 2; k++) {
            assertNull(cache.get(k));

            int vals = k % 3 + 1;

            for (int v = 0; v < vals; v++)
                cache.put(k, new MvccTestAccount(v, 1));

            assertEquals(vals - 1, cache.get(k).val);
        }

        srv0.destroyCache(cache.getName());

        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, PARTS);

        if (cfgC != null)
            cfgC.apply(ccfg);

        cache = (IgniteCache)srv0.createCache(ccfg);

        for (int k = 0; k < PARTS * 2; k++) {
            assertNull(cache.get(k));

            int vals = k % 3 + 2;

            for (int v = 0; v < vals; v++)
                cache.put(k, new MvccTestAccount(v + 100, 1));

            assertEquals(vals - 1 + 100, cache.get(k).val);
        }

        srv0.destroyCache(cache.getName());

        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, PARTS);

        IgniteCache<Long, Long> cache0 = (IgniteCache)srv0.createCache(ccfg);

        for (long k = 0; k < PARTS * 2; k++) {
            assertNull(cache0.get(k));

            int vals = (int)(k % 3 + 2);

            for (long v = 0; v < vals; v++)
                cache0.put(k, v);

            assertEquals((long)(vals - 1), (Object)cache0.get(k));
        }
    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param cfgC Optional closure applied to cache configuration.
     * @param withRmvs If {@code true} then in addition to puts tests also executes removes.
     * @param readMode Read mode.
     * @throws Exception If failed.
     */
    final void accountsTxReadAll(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC,
        final boolean withRmvs,
        final ReadMode readMode
    )
        throws Exception
    {
        final int ACCOUNTS = 20;

        final int ACCOUNT_START_VAL = 1000;

        final int writers = 4;

        final int readers = 4;

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                Map<Integer, MvccTestAccount> accounts = new HashMap<>();

                for (int i = 0; i < ACCOUNTS; i++)
                    accounts.put(i, new MvccTestAccount(ACCOUNT_START_VAL, 1));

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.putAll(accounts);

                    tx.commit();
                }
            }
        };

        final Set<Integer> rmvdIds = new HashSet<>();

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);

                        try {
                            IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                            cnt++;

                            Integer id1 = rnd.nextInt(ACCOUNTS);
                            Integer id2 = rnd.nextInt(ACCOUNTS);

                            while (id1.equals(id2))
                                id2 = rnd.nextInt(ACCOUNTS);

                            TreeSet<Integer> keys = new TreeSet<>();

                            keys.add(id1);
                            keys.add(id2);

                            Integer cntr1 = null;
                            Integer cntr2 = null;

                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                MvccTestAccount a1;
                                MvccTestAccount a2;

                                Map<Integer, MvccTestAccount> accounts = cache.cache.getAll(keys);

                                a1 = accounts.get(id1);
                                a2 = accounts.get(id2);

                                if (!withRmvs) {
                                    assertNotNull(a1);
                                    assertNotNull(a2);

                                    cntr1 = a1.updateCnt + 1;
                                    cntr2 = a2.updateCnt + 1;

                                    cache.cache.put(id1, new MvccTestAccount(a1.val + 1, cntr1));
                                    cache.cache.put(id2, new MvccTestAccount(a2.val - 1, cntr2));
                                }
                                else {
                                    if (a1 != null || a2 != null) {
                                        if (a1 != null && a2 != null) {
                                            Integer rmvd = null;

                                            if (rnd.nextInt(10) == 0) {
                                                synchronized (rmvdIds) {
                                                    if (rmvdIds.size() < ACCOUNTS / 2) {
                                                        rmvd = rnd.nextBoolean() ? id1 : id2;

                                                        assertTrue(rmvdIds.add(rmvd));
                                                    }
                                                }
                                            }

                                            if (rmvd != null) {
                                                cache.cache.remove(rmvd);

                                                cache.cache.put(rmvd.equals(id1) ? id2 : id1,
                                                    new MvccTestAccount(a1.val + a2.val, 1));
                                            }
                                            else {
                                                cache.cache.put(id1, new MvccTestAccount(a1.val + 1, 1));
                                                cache.cache.put(id2, new MvccTestAccount(a2.val - 1, 1));
                                            }
                                        }
                                        else {
                                            if (a1 == null) {
                                                cache.cache.put(id1, new MvccTestAccount(100, 1));
                                                cache.cache.put(id2, new MvccTestAccount(a2.val - 100, 1));

                                                assertTrue(rmvdIds.remove(id1));
                                            }
                                            else {
                                                cache.cache.put(id1, new MvccTestAccount(a1.val - 100, 1));
                                                cache.cache.put(id2, new MvccTestAccount(100, 1));

                                                assertTrue(rmvdIds.remove(id2));
                                            }
                                        }
                                    }
                                }

                                tx.commit();
                            }

                            if (!withRmvs) {
                                Map<Integer, MvccTestAccount> accounts = cache.cache.getAll(keys);

                                MvccTestAccount a1 = accounts.get(id1);
                                MvccTestAccount a2 = accounts.get(id2);

                                assertNotNull(a1);
                                assertNotNull(a2);

                                assertTrue(a1.updateCnt >= cntr1);
                                assertTrue(a2.updateCnt >= cntr2);
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set<Integer> keys = new LinkedHashSet<>();

                    Map<Integer, Integer> lastUpdateCntrs = new HashMap<>();

                    SqlFieldsQuery sumQry = new SqlFieldsQuery("select sum(val) from MvccTestAccount");

                    while (!stop.get()) {
                        while (keys.size() < ACCOUNTS)
                            keys.add(rnd.nextInt(ACCOUNTS));

                        TestCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);

                        Map<Integer, MvccTestAccount> accounts = null;

                        try {
                            switch (readMode) {
                                case GET_ALL: {
                                    accounts = cache.cache.getAll(keys);

                                    break;
                                }

                                case SCAN: {
                                    accounts = new HashMap<>();

                                    for (IgniteCache.Entry<Integer, MvccTestAccount> e : cache.cache) {
                                        MvccTestAccount old = accounts.put(e.getKey(), e.getValue());

                                        assertNull(old);
                                    }

                                    break;
                                }

                                case SQL_ALL: {
                                    accounts = new HashMap<>();

                                    if (rnd.nextBoolean()) {
                                        SqlQuery<Integer, MvccTestAccount> qry =
                                            new SqlQuery<>(MvccTestAccount.class, "_key >= 0");

                                        for (IgniteCache.Entry<Integer, MvccTestAccount> e : cache.cache.query(qry)) {
                                            MvccTestAccount old = accounts.put(e.getKey(), e.getValue());

                                            assertNull(old);
                                        }
                                    }
                                    else {
                                        SqlFieldsQuery qry = new SqlFieldsQuery("select _key, val from MvccTestAccount");

                                        for (List<?> row : cache.cache.query(qry)) {
                                            Integer id = (Integer)row.get(0);
                                            Integer val = (Integer)row.get(1);

                                            MvccTestAccount old = accounts.put(id, new MvccTestAccount(val, 1));

                                            assertNull(old);
                                        }
                                    }

                                    break;
                                }

                                case SQL_SUM: {
                                    List<List<?>> res = cache.cache.query(sumQry).getAll();

                                    assertEquals(1, res.size());

                                    BigDecimal sum = (BigDecimal)res.get(0).get(0);

                                    assertEquals(ACCOUNT_START_VAL * ACCOUNTS, sum.intValue());

                                    break;
                                }

                                default: {
                                    fail();

                                    return;
                                }
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }

                        if (accounts != null) {
                            if (!withRmvs)
                                assertEquals(ACCOUNTS, accounts.size());

                            int sum = 0;

                            for (int i = 0; i < ACCOUNTS; i++) {
                                MvccTestAccount account = accounts.get(i);

                                if (account != null) {
                                    sum += account.val;

                                    Integer cntr = lastUpdateCntrs.get(i);

                                    if (cntr != null)
                                        assertTrue(cntr <= account.updateCnt);

                                    lastUpdateCntrs.put(i, cntr);
                                }
                                else
                                    assertTrue(withRmvs);
                            }

                            assertEquals(ACCOUNTS * ACCOUNT_START_VAL, sum);
                        }
                    }

                    if (idx == 0) {
                        TestCache<Integer, MvccTestAccount> cache = randomCache(caches, rnd);

                        Map<Integer, MvccTestAccount> accounts;

                        try {
                            accounts = cache.cache.getAll(keys);
                        }
                        finally {
                            cache.readUnlock();
                        }

                        int sum = 0;

                        for (int i = 0; i < ACCOUNTS; i++) {
                            MvccTestAccount account = accounts.get(i);

                            assertTrue(account != null || withRmvs);

                            info("Account [id=" + i + ", val=" + (account != null ? account.val : null) + ']');

                            if (account != null)
                                sum += account.val;
                        }

                        info("Sum: " + sum);
                    }
                }
            };

        readWriteTest(
            null,
            srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            DFLT_TEST_TIME,
            cfgC,
            init,
            writer,
            reader);
    }

    /**
     * @param restartMode Restart mode.
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param time Test time.
     * @param cfgC Optional closure applied to cache configuration.
     * @param writers Number of writers.
     * @param readers Number of readers.
     * @param init Optional init closure.
     * @param writer Writers threads closure.
     * @param reader Readers threads closure.
     * @throws Exception If failed.
     */
    final void readWriteTest(
        final RestartMode restartMode,
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        final int writers,
        final int readers,
        final long time,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC,
        IgniteInClosure<IgniteCache<Object, Object>> init,
        final GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer,
        final GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader) throws Exception {
        if (restartMode == RestartMode.RESTART_CRD)
            MvccProcessor.coordinatorAssignClosure(new CoordinatorAssignClosure());

        Ignite srv0 = startGridsMultiThreaded(srvs);

        if (clients > 0) {
            client = true;

            startGridsMultiThreaded(srvs, clients);

            client = false;
        }

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            FULL_SYNC,
            cacheBackups,
            cacheParts);

        if (restartMode == RestartMode.RESTART_CRD)
            ccfg.setNodeFilter(new CoordinatorNodeFilter());

        if (cfgC != null)
            cfgC.apply(ccfg);

        IgniteCache<Object, Object> cache = srv0.createCache(ccfg);

        int crdIdx = srvs + clients;

        if (restartMode == RestartMode.RESTART_CRD) {
            nodeAttr = CRD_ATTR;

            startGrid(crdIdx);
        }

        if (init != null)
            init.apply(cache);

        final List<TestCache> caches = new ArrayList<>(srvs + clients);

        for (int i = 0; i < srvs + clients; i++) {
            Ignite node = grid(i);

            caches.add(new TestCache(node.cache(cache.getName())));
        }

        final long stopTime = U.currentTimeMillis() + time;

        final AtomicBoolean stop = new AtomicBoolean();

        try {
            final AtomicInteger writerIdx = new AtomicInteger();

            IgniteInternalFuture<?> writeFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try {
                        int idx = writerIdx.getAndIncrement();

                        writer.apply(idx, caches, stop);
                    }
                    catch (Throwable e) {
                        if (restartMode != null && X.hasCause(e, ClusterTopologyException.class)) {
                            log.info("Writer error: " + e);

                            return null;
                        }

                        error("Unexpected error: " + e, e);

                        stop.set(true);

                        fail("Unexpected error: " + e);
                    }

                    return null;
                }
            }, writers, "writer");

            final AtomicInteger readerIdx = new AtomicInteger();

            IgniteInternalFuture<?> readFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    try {
                        int idx = readerIdx.getAndIncrement();

                        reader.apply(idx, caches, stop);
                    }
                    catch (Throwable e) {
                        error("Unexpected error: " + e, e);

                        stop.set(true);

                        fail("Unexpected error: " + e);
                    }

                    return null;
                }
            }, readers, "reader");

            while (System.currentTimeMillis() < stopTime && !stop.get()) {
                Thread.sleep(1000);

                if (restartMode != null) {
                    switch (restartMode) {
                        case RESTART_CRD: {
                            log.info("Start new coordinator: " + (crdIdx + 1));

                            startGrid(crdIdx + 1);

                            log.info("Stop current coordinator: " + crdIdx);

                            stopGrid(crdIdx);

                            crdIdx++;

                            awaitPartitionMapExchange();

                            break;
                        }

                        case RESTART_RND_SRV: {
                            ThreadLocalRandom rnd = ThreadLocalRandom.current();

                            int idx = rnd.nextInt(srvs);

                            TestCache cache0 = caches.get(idx);

                            cache0.stopLock.writeLock().lock();

                            log.info("Stop node: " + idx);

                            stopGrid(idx);

                            log.info("Start new node: " + idx);

                            Ignite srv = startGrid(idx);

                            synchronized (caches) {
                                caches.set(idx, new TestCache(srv.cache(DEFAULT_CACHE_NAME)));
                            }

                            awaitPartitionMapExchange();

                            break;
                        }

                        default:
                            fail();
                    }
                }
            }

            stop.set(true);

            writeFut.get();
            readFut.get();
        }
        finally {
            stop.set(true);
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @param parts Number of partitions.
     * @return Cache configuration.
     */
    final CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        int parts) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, parts));

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    final void verifyCoordinatorInternalState() throws Exception {
        for (Ignite node : G.allGrids()) {
            final MvccProcessor crd = ((IgniteKernal)node).context().cache().context().coordinators();

            Map activeTxs = GridTestUtils.getFieldValue(crd, "activeTxs");

            assertTrue("Txs on node [node=" + node.name() + ", txs=" + activeTxs.toString() + ']',
                activeTxs.isEmpty());

            Map cntrFuts = GridTestUtils.getFieldValue(crd, "verFuts");

            assertTrue(cntrFuts.isEmpty());

            Map ackFuts = GridTestUtils.getFieldValue(crd, "ackFuts");

            assertTrue(ackFuts.isEmpty());

            // TODO IGNITE-3478
            // checkActiveQueriesCleanup(node);
        }
    }

    /**
     * @param node Node.
     * @throws Exception If failed.
     */
    protected final void checkActiveQueriesCleanup(Ignite node) throws Exception {
        final MvccProcessor crd = ((IgniteKernal)node).context().cache().context().coordinators();

        assertTrue("Active queries not cleared: " + node.name(), GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    Object activeQueries = GridTestUtils.getFieldValue(crd, "activeQueries");

                    synchronized (activeQueries) {
                        Long minQry = GridTestUtils.getFieldValue(activeQueries, "minQry");

                        if (minQry != null)
                            log.info("Min query: " + minQry);

                        Map<Object, Map> queriesMap = GridTestUtils.getFieldValue(activeQueries, "activeQueries");

                        boolean empty = true;

                        for (Map.Entry<Object, Map> e : queriesMap.entrySet()) {
                            if (!e.getValue().isEmpty()) {
                                empty = false;

                                log.info("Active queries: " + e);
                            }
                        }

                        return empty && minQry == null;
                    }
                }
            }, 8_000)
        );

        assertTrue("Previous coordinator queries not empty: " + node.name(), GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    Map queries = GridTestUtils.getFieldValue(crd, "prevCrdQueries", "activeQueries");
                    Boolean prevDone = GridTestUtils.getFieldValue(crd, "prevCrdQueries", "prevQueriesDone");

                    if (!queries.isEmpty() || !prevDone)
                        log.info("Previous coordinator state [prevDone=" + prevDone + ", queries=" + queries + ']');

                    return queries.isEmpty();
                }
            }, 8_000)
        );
    }

    /**
     * @param caches Caches.
     * @param rnd Random.
     * @return Random cache.
     */
    static <K, V> TestCache<K, V> randomCache(
        List<TestCache> caches,
        ThreadLocalRandom rnd) {
        synchronized (caches) {
            if (caches.size() == 1) {
                TestCache cache = caches.get(0);

                assertTrue(cache.readLock());

                return cache;
            }

            for (;;) {
                int idx = rnd.nextInt(caches.size());

                TestCache testCache = caches.get(idx);

                if (testCache.readLock())
                    return testCache;
            }
        }
    }

    /**
     *
     */
    static class MvccTestAccount {
        /** */
        @QuerySqlField(index = false)
        final int val;

        /** */
        final int updateCnt;

        /**
         * @param val Value.
         * @param updateCnt Updates counter.
         */
        MvccTestAccount(int val, int updateCnt) {
            assert updateCnt > 0;

            this.val = val;
            this.updateCnt = updateCnt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MvccTestAccount.class, this);
        }
    }

    /**
     *
     */
    enum ReadMode {
        /** */
        GET_ALL,

        /** */
        SCAN,

        /** */
        SQL_ALL,

        /** */
        SQL_SUM
    }

    /**
     *
     */
    enum RestartMode {
        /**
         * Dedicated coordinator node is restarted during test.
         */
        RESTART_CRD,

        /** */
        RESTART_RND_SRV
    }

    /**
     *
     */
    static class CoordinatorNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(CRD_ATTR) == null;
        }
    }

    /**
     *
     */
    static class CoordinatorAssignClosure implements IgniteClosure<Collection<ClusterNode>, ClusterNode> {
        /** {@inheritDoc} */
        @Override public ClusterNode apply(Collection<ClusterNode> clusterNodes) {
            for (ClusterNode node : clusterNodes) {
                if (node.attribute(CRD_ATTR) != null) {
                    assert !CU.clientNode(node) : node;

                    return node;
                }
            }

            return null;
        }
    }

    /**
     *
     */
    static class TestCache<K, V> {
        /** */
        final IgniteCache<K, V> cache;

        /** Locks node to avoid node restart while test operation is in progress. */
        final ReadWriteLock stopLock = new ReentrantReadWriteLock();

        /**
         * @param cache Cache.
         */
        TestCache(IgniteCache cache) {
            this.cache = cache;
        }

        /**
         * @return {@code True} if locked.
         */
        boolean readLock() {
            return stopLock.readLock().tryLock();
        }

        /**
         *
         */
        void readUnlock() {
            stopLock.readLock().unlock();
        }
    }
}
