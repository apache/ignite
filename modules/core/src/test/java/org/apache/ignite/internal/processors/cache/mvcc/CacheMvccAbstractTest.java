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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionSerializationException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL_SUM;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.DML;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public abstract class CacheMvccAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final ObjectCodec<Integer> INTEGER_CODEC = new IntegerCodec();

    /** */
    protected static final ObjectCodec<MvccTestAccount> ACCOUNT_CODEC = new AccountCodec();

    /** */
    static final int DFLT_PARTITION_COUNT = RendezvousAffinityFunction.DFLT_PARTITION_COUNT;

    /** */
    static final String CRD_ATTR = "testCrd";

    /** */
    static final long DFLT_TEST_TIME = SF.applyLB(30_000, 3_000);

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

    /** */
    protected CacheConfiguration ccfg;

    /** */
    protected CacheConfiguration[] ccfgs;

    /** */
    protected boolean disableScheduledVacuum;

    /** */
    protected static final int TX_TIMEOUT = 3000;

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        if (disableScheduledVacuum)
            cfg.setMvccVacuumFrequency(Integer.MAX_VALUE);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        assert (ccfg == null) || (ccfgs == null);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        if (ccfgs != null)
            cfg.setCacheConfiguration(ccfgs);

        if (nodeAttr != null)
            cfg.setUserAttributes(F.asMap(nodeAttr, true));

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setWalMode(WALMode.LOG_ONLY);
        storageCfg.setPageSize(PAGE_SIZE);

        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        regionCfg.setPersistenceEnabled(persistence);
        regionCfg.setMaxSize(64L * 1024 * 1024);

        storageCfg.setDefaultDataRegionConfiguration(regionCfg);

        cfg.setDataStorageConfiguration(storageCfg);

        cfg.setConsistentId(gridName);

        cfg.setTransactionConfiguration(new TransactionConfiguration()
            .setDefaultTxConcurrency(TransactionConcurrency.PESSIMISTIC)
            .setDefaultTxIsolation(TransactionIsolation.REPEATABLE_READ));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DFLT_TEST_TIME + 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ccfg = null;
        ccfgs = null;

        MvccProcessorImpl.coordinatorAssignClosure(null);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        persistence = false;

        try {
            verifyOldVersionsCleaned();

            verifyCoordinatorInternalState();
        }
        finally {
            stopAllGrids();

            MvccProcessorImpl.coordinatorAssignClosure(null);

            cleanPersistenceDir();
        }

        super.afterTest();
    }

    /**
     * @param cfgC Optional closure applied to cache configuration.
     * @throws Exception If failed.
     */
    final void cacheRecreate(@Nullable IgniteInClosure<CacheConfiguration> cfgC) throws Exception {
        Ignite srv0 = startGrid(0);

        final int PARTS = 64;

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, PARTS);

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

        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, PARTS);

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

        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, PARTS);

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
     * @param writeMode Write mode.
     * @throws Exception If failed.
     */
    final void accountsTxReadAll(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC,
        final boolean withRmvs,
        final ReadMode readMode,
        final WriteMode writeMode
    ) throws Exception {
        accountsTxReadAll(srvs, clients, cacheBackups, cacheParts, cfgC, withRmvs, readMode, writeMode, DFLT_TEST_TIME, null);
    }

    /**
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param cfgC Optional closure applied to cache configuration.
     * @param withRmvs If {@code true} then in addition to puts tests also executes removes.
     * @param readMode Read mode.
     * @param writeMode Write mode.
     * @param testTime Test time.
     * @throws Exception If failed.
     */
    final void accountsTxReadAll(
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC,
        final boolean withRmvs,
        final ReadMode readMode,
        final WriteMode writeMode,
        long testTime,
        RestartMode restartMode
    ) throws Exception {
        final int ACCOUNTS = 20;

        final int ACCOUNT_START_VAL = 1000;

        final int writers = 4;

        final int readers = 4;

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                if (writeMode == WriteMode.PUT) {
                    Map<Integer, MvccTestAccount> accounts = new HashMap<>();

                    for (int i = 0; i < ACCOUNTS; i++)
                        accounts.put(i, new MvccTestAccount(ACCOUNT_START_VAL, 1));

                    try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.putAll(accounts);

                        tx.commit();
                    }
                }
                else if (writeMode == WriteMode.DML) {
                    try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        SqlFieldsQuery qry = new SqlFieldsQuery("insert into MvccTestAccount(_key, val, updateCnt) values " +
                                "(?," + ACCOUNT_START_VAL + ",1)");

                        for (int i = 0; i < ACCOUNTS; i++) {
                            try (FieldsQueryCursor<List<?>> cur = cache.query(qry.setArgs(i))) {
                                assertEquals(1L, cur.iterator().next().get(0));
                            }

                            tx.commit();
                        }
                    }
                }
                else
                    assert false : "Unknown write mode";
            }
        };

        final RemovedAccountsTracker rmvdTracker = new RemovedAccountsTracker(ACCOUNTS);

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

                            int i1 = rnd.nextInt(ACCOUNTS), i2 = rnd.nextInt(ACCOUNTS);

                            while (i2 == i1)
                                i2 = rnd.nextInt(ACCOUNTS);

                            Integer id1 = Math.min(i1, i2);
                            Integer id2 = Math.max(i1, i2);

                            Set<Integer> keys = new HashSet<>();

                            keys.add(id1);
                            keys.add(id2);

                            Integer cntr1 = null;
                            Integer cntr2 = null;

                            Integer rmvd = null;
                            Integer inserted = null;

                            MvccTestAccount a1;
                            MvccTestAccount a2;

                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                tx.timeout(TX_TIMEOUT);

                                Map<Integer, MvccTestAccount> accounts = null;

                                if (writeMode == WriteMode.PUT)
                                    accounts = cache.cache.getAll(keys);
                                else if (writeMode == WriteMode.DML)
                                    accounts = getAllSql(cache);
                                else
                                    assert false : "Unknown write mode";

                                a1 = accounts.get(id1);
                                a2 = accounts.get(id2);

                                if (!withRmvs) {
                                    assertNotNull(a1);
                                    assertNotNull(a2);

                                    cntr1 = a1.updateCnt + 1;
                                    cntr2 = a2.updateCnt + 1;

                                    if (writeMode == WriteMode.PUT) {
                                        cache.cache.put(id1, new MvccTestAccount(a1.val + 1, cntr1));
                                        cache.cache.put(id2, new MvccTestAccount(a2.val - 1, cntr2));
                                    }
                                    else if (writeMode == WriteMode.DML) {
                                        updateSql(cache, id1, a1.val + 1, cntr1);
                                        updateSql(cache, id2, a2.val - 1, cntr2);
                                    }
                                    else
                                        assert false : "Unknown write mode";
                                }
                                else {
                                    if (a1 != null || a2 != null) {
                                        if (a1 != null && a2 != null) {
                                            if (rnd.nextInt(10) == 0) {
                                                if (rmvdTracker.size() < ACCOUNTS / 2) {
                                                    rmvd = rnd.nextBoolean() ? id1 : id2;

                                                    assertTrue(rmvdTracker.markRemoved(rmvd));
                                                }
                                            }

                                            if (rmvd != null) {
                                                if (writeMode == WriteMode.PUT) {
                                                    if (rmvd.equals(id1)) {
                                                        cache.cache.remove(id1);
                                                        cache.cache.put(id2, new MvccTestAccount(a1.val + a2.val, 1));
                                                    }
                                                    else {
                                                        cache.cache.put(id1, new MvccTestAccount(a1.val + a2.val, 1));
                                                        cache.cache.remove(id2);
                                                    }
                                                }
                                                else if (writeMode == WriteMode.DML) {
                                                    if (rmvd.equals(id1)) {
                                                        removeSql(cache, id1);
                                                        updateSql(cache, id2,a1.val + a2.val, 1);
                                                    }
                                                    else {
                                                        updateSql(cache, id1,a1.val + a2.val, 1);
                                                        removeSql(cache, id2);
                                                    }
                                                }
                                                else
                                                    assert false : "Unknown write mode";
                                            }
                                            else {
                                                if (writeMode == WriteMode.PUT) {
                                                    cache.cache.put(id1, new MvccTestAccount(a1.val + 1, 1));
                                                    cache.cache.put(id2, new MvccTestAccount(a2.val - 1, 1));
                                                }
                                                else if (writeMode == WriteMode.DML) {
                                                    updateSql(cache, id1, a1.val + 1, 1);
                                                    updateSql(cache, id2, a2.val - 1, 1);
                                                }
                                                else
                                                    assert false : "Unknown write mode";
                                            }
                                        }
                                        else {
                                            if (a1 == null) {
                                                inserted = id1;

                                                if (writeMode == WriteMode.PUT) {
                                                    cache.cache.put(id1, new MvccTestAccount(100, 1));
                                                    cache.cache.put(id2, new MvccTestAccount(a2.val - 100, 1));
                                                }
                                                else if (writeMode == WriteMode.DML) {
                                                    insertSql(cache, id1, 100, 1);
                                                    updateSql(cache, id2, a2.val - 100, 1);
                                                }
                                                else
                                                    assert false : "Unknown write mode";
                                            }
                                            else {
                                                inserted = id2;

                                                if (writeMode == WriteMode.PUT) {
                                                    cache.cache.put(id1, new MvccTestAccount(a1.val - 100, 1));
                                                    cache.cache.put(id2, new MvccTestAccount(100, 1));
                                                }
                                                else if (writeMode == WriteMode.DML) {
                                                    updateSql(cache, id1, a1.val - 100, 1);
                                                    insertSql(cache, id2, 100, 1);
                                                }
                                                else
                                                    assert false : "Unknown write mode";
                                            }
                                        }
                                    }
                                }

                                tx.commit();

                                // In case of tx success mark inserted.
                                if (inserted != null) {
                                    assert withRmvs;

                                    assertTrue(rmvdTracker.unmarkRemoved(inserted));
                                }
                            }
                            catch (Throwable e) {
                                if (rmvd != null) {
                                    assert withRmvs;

                                    // If tx fails, unmark removed.
                                    assertTrue(rmvdTracker.unmarkRemoved(rmvd));
                                }

                                throw e;
                            }

                            if (!withRmvs) {
                                Map<Integer, MvccTestAccount> accounts = null;

                                if (writeMode == WriteMode.PUT)
                                    accounts = cache.cache.getAll(keys);
                                else if (writeMode == WriteMode.DML)
                                    accounts = getAllSql(cache);
                                else
                                    assert false : "Unknown write mode";

                                a1 = accounts.get(id1);
                                a2 = accounts.get(id2);

                                assertNotNull(a1);
                                assertNotNull(a2);

                                assertTrue(a1.updateCnt >= cntr1);
                                assertTrue(a2.updateCnt >= cntr2);
                            }
                        }
                        catch (Exception e) {
                            handleTxException(e);
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
                                case GET: {
                                    accounts = cache.cache.getAll(keys);

                                    break;
                                }

                                case SCAN: {
                                    accounts = new HashMap<>();

                                    Iterator<Cache.Entry<Integer, MvccTestAccount>> it = cache.cache.iterator();

                                    try {
                                        for (; it.hasNext(); ) {
                                            IgniteCache.Entry<Integer, MvccTestAccount> e = it.next();
                                            MvccTestAccount old = accounts.put(e.getKey(), e.getValue());

                                            assertNull("new=" + e + ", old=" + old, old);
                                        }
                                    } finally {
                                        U.closeQuiet((AutoCloseable) it);
                                    }

                                    break;
                                }

                                case SQL: {
                                    accounts = new HashMap<>();

                                    if (rnd.nextBoolean()) {
                                        SqlQuery<Integer, MvccTestAccount> qry =
                                            new SqlQuery<>(MvccTestAccount.class, "_key >= 0");

                                        for (IgniteCache.Entry<Integer, MvccTestAccount> e : cache.cache.query(qry).getAll()) {
                                            MvccTestAccount old = accounts.put(e.getKey(), e.getValue());

                                            assertNull(old);
                                        }
                                    }
                                    else {
                                        SqlFieldsQuery qry = new SqlFieldsQuery("select _key, val from MvccTestAccount");

                                        for (List<?> row : cache.cache.query(qry).getAll()) {
                                            Integer id = (Integer)row.get(0);
                                            Integer val = (Integer)row.get(1);

                                            MvccTestAccount old = accounts.put(id, new MvccTestAccount(val, 1));

                                            assertNull(old);
                                        }
                                    }

                                    break;
                                }

                                case SQL_SUM: {
                                    Long sum;

                                    if (rnd.nextBoolean()) {
                                        List<List<?>> res = cache.cache.query(sumQry).getAll();

                                        assertEquals(1, res.size());

                                        sum = (Long)res.get(0).get(0);
                                    }
                                    else {
                                        Map res = readAllByMode(cache.cache, keys, readMode, ACCOUNT_CODEC);

                                        sum = (Long)((Map.Entry)res.entrySet().iterator().next()).getValue();
                                    }

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

                        ReadMode readMode0 = readMode == SQL_SUM ? SQL : readMode;

                        try {
                            accounts = readAllByMode(cache.cache, keys, readMode0, ACCOUNT_CODEC);;
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
            restartMode,
            srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            testTime,
            cfgC,
            init,
            writer,
            reader);
    }

    /**
     * Returns all accounts from cache by means of SQL.
     *
     * @param cache Cache.
     * @return All accounts
     */
    protected static Map<Integer, MvccTestAccount> getAllSql(TestCache<Integer, MvccTestAccount> cache) {
        Map<Integer, MvccTestAccount> accounts = new HashMap<>();

        SqlFieldsQuery qry = new SqlFieldsQuery("select _key, val, updateCnt from MvccTestAccount");

        for (List<?> row : cache.cache.query(qry).getAll()) {
            Integer id = (Integer)row.get(0);
            Integer val = (Integer)row.get(1);
            Integer updateCnt = (Integer)row.get(2);

            MvccTestAccount old = accounts.put(id, new MvccTestAccount(val, updateCnt));

            assertNull(old);
        }

        return accounts;
    }

    /**
     * Updates account by means of SQL API.
     *
     * @param cache Cache.
     * @param key Key.
     * @param val Value.
     * @param updateCnt Update counter.
     */
    private static void updateSql(TestCache<Integer, MvccTestAccount> cache, Integer key, Integer val, Integer updateCnt) {
        SqlFieldsQuery qry = new SqlFieldsQuery("update MvccTestAccount set val=" + val + ", updateCnt=" +
            updateCnt + " where _key=" + key);

        cache.cache.query(qry).getAll();
    }

    /**
     * Removes account by means of SQL API.
     *
     * @param cache Cache.
     * @param key Key.
     */
    protected static void removeSql(TestCache<Integer, MvccTestAccount> cache, Integer key) {
        SqlFieldsQuery qry = new SqlFieldsQuery("delete from MvccTestAccount where _key=" + key);

        cache.cache.query(qry).getAll();
    }

    /**
     * Merge account by means of SQL API.
     *
     * @param cache Cache.
     * @param key Key.
     * @param val Value.
     * @param updateCnt Update counter.
     */
    protected static void mergeSql(TestCache<Integer, MvccTestAccount> cache, Integer key, Integer val, Integer updateCnt) {
        SqlFieldsQuery qry = new SqlFieldsQuery("merge into MvccTestAccount(_key, val, updateCnt) values " +
            " (" + key + ", " + val + ", " + updateCnt + ")");

        cache.cache.query(qry).getAll();
    }

    /**
     * Inserts account by means of SQL API.
     *
     * @param cache Cache.
     * @param key Key.
     * @param val Value.
     * @param updateCnt Update counter.
     */
    private static void insertSql(TestCache<Integer, MvccTestAccount> cache, int key, Integer val, Integer updateCnt) {
        SqlFieldsQuery qry = new SqlFieldsQuery("insert into MvccTestAccount(_key, val, updateCnt) values " +
            " (" + key + ", " + val + ", " + updateCnt + ")");

        cache.cache.query(qry).getAll();
    }

    /**
     * @param restartMode Restart mode.
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param readMode Read mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected void putAllGetAll(
        RestartMode restartMode,
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC,
        ReadMode readMode,
        WriteMode writeMode
    ) throws Exception {
        final int RANGE = 20;

        final int writers = 4;

        final int readers = 4;

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int min = idx * RANGE;
                    int max = min + RANGE;

                    info("Thread range [min=" + min + ", max=" + max + ']');

                    // Sorted map for put to avoid deadlocks.
                    Map<Integer, Integer> map = new TreeMap<>();

                    // Unordered key sequence.
                    Set<Integer> keys = new LinkedHashSet<>();

                    int v = idx * 1_000_000;

                    boolean first = true;

                    while (!stop.get()) {
                        while (keys.size() < RANGE) {
                            int key = rnd.nextInt(min, max);

                            if (keys.add(key))
                                map.put(key, v);
                        }

                        TestCache<Integer, Integer> cache = randomCache(caches, rnd);

                        try {
                            IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                if (!first && rnd.nextBoolean()) {
                                    Map<Integer, Integer> res = readAllByMode(cache.cache, keys, readMode, INTEGER_CODEC);

                                    for (Integer k : keys)
                                        assertEquals("res=" + res, v - 1, (Object)res.get(k));
                                }

                                writeAllByMode(cache.cache, map, writeMode, INTEGER_CODEC);

                                tx.commit();

                                v++;

                                first = false;
                            }

                            if (rnd.nextBoolean()) {
                                Map<Integer, Integer> res = readAllByMode(cache.cache, keys, readMode, INTEGER_CODEC);

                                for (Integer k : keys)
                                    assertEquals("key=" + k, v - 1, (Object)res.get(k));
                            }
                        }
                        catch (Exception e) {
                            handleTxException(e);
                        }
                        finally {
                            cache.readUnlock();

                            keys.clear();

                            map.clear();
                        }
                    }

                    info("Writer done, updates: " + v);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set<Integer> keys = new LinkedHashSet<>();

                    Map<Integer, Integer> readVals = new HashMap<>();

                    while (!stop.get()) {
                        int range = rnd.nextInt(0, writers);

                        int min = range * RANGE;
                        int max = min + RANGE;

                        keys.clear();

                        while (keys.size() < RANGE)
                            keys.add(rnd.nextInt(min, max));

                        TestCache<Integer, Integer> cache = randomCache(caches, rnd);

                        Map<Integer, Integer> map;

                        try {
                            map = readAllByMode(cache.cache, keys, readMode, INTEGER_CODEC);
                        }
                        catch (Exception e) {
                            handleTxException(e);

                            continue;
                        }
                        finally {
                            cache.readUnlock();
                        }

                        assertTrue("Invalid map size: " + map.size() + ", map=" + map, map.isEmpty() || map.size() == RANGE);

                        Integer val0 = null;

                        for (Map.Entry<Integer, Integer> e: map.entrySet()) {
                            Integer val = e.getValue();

                            assertNotNull(val);

                            if (val0 == null) {
                                Integer readVal = readVals.get(range);

                                if (readVal != null)
                                    assertTrue("readVal=" + readVal + ", val=" + val + ", map=" + map,readVal <= val);

                                readVals.put(range, val);

                                val0 = val;
                            }
                            else {
                                if (!F.eq(val0, val)) {
                                    assertEquals("Unexpected value [range=" + range + ", key=" + e.getKey() + ']' +
                                        ", map=" + map,
                                        val0,
                                        val);
                                }
                            }
                        }
                    }
                }
            };

        readWriteTest(
            restartMode,
            srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            DFLT_TEST_TIME,
            cfgC,
            null,
            writer,
            reader);

        for (Ignite node : G.allGrids())
            checkActiveQueriesCleanup(node);
    }



    /**
     * @param N Number of object to update in single transaction.
     * @param srvs Number of server nodes.
     * @param clients Number of client nodes.
     * @param cacheBackups Number of cache backups.
     * @param cacheParts Number of cache partitions.
     * @param time Test time.
     * @param readMode Read mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected void updateNObjectsTest(
        final int N,
        final int srvs,
        final int clients,
        int cacheBackups,
        int cacheParts,
        long time,
        @Nullable IgniteInClosure<CacheConfiguration> cfgC,
        ReadMode readMode,
        WriteMode writeMode,
        RestartMode restartMode
    )
        throws Exception
    {
        final int TOTAL = 20;

        assert N <= TOTAL;

        info("updateNObjectsTest [n=" + N + ", total=" + TOTAL + ']');

        final int writers = 4;

        final int readers = 4;

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                final IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

                Map<Integer, Integer> vals = new LinkedHashMap<>();

                for (int i = 0; i < TOTAL; i++)
                    vals.put(i, N);

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    writeAllByMode(cache, vals, writeMode, INTEGER_CODEC);

                    tx.commit();
                }
            }
        };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Integer, Integer> cache = randomCache(caches, rnd);
                        IgniteTransactions txs = cache.cache.unwrap(Ignite.class).transactions();

                        TreeSet<Integer> keys = new TreeSet<>();

                        while (keys.size() < N)
                            keys.add(rnd.nextInt(TOTAL));

                        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            tx.timeout(TX_TIMEOUT);

                            Map<Integer, Integer> curVals = readAllByMode(cache.cache, keys, readMode, INTEGER_CODEC);

                            assertEquals(N, curVals.size());

                            Map<Integer, Integer> newVals = new TreeMap<>();

                            for (Map.Entry<Integer, Integer> e : curVals.entrySet())
                                newVals.put(e.getKey(), e.getValue() + 1);

                            writeAllByMode(cache.cache, newVals, writeMode, INTEGER_CODEC);

                            tx.commit();
                        }
                        catch (Exception e) {
                            handleTxException(e);
                        }
                        finally {
                            cache.readUnlock();
                        }

                        cnt++;
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Set<Integer> keys = new LinkedHashSet<>();

                    while (!stop.get()) {
                        while (keys.size() < TOTAL)
                            keys.add(rnd.nextInt(TOTAL));

                        TestCache<Integer, Integer> cache = randomCache(caches, rnd);

                        Map<Integer, Integer> vals = null;

                        try {
                            vals = readAllByMode(cache.cache, keys, readMode, INTEGER_CODEC);
                        }
                        catch (Exception e) {
                            handleTxException(e);
                        }
                        finally {
                            cache.readUnlock();
                        }

                        assertEquals("vals=" + vals, TOTAL, vals.size());

                        int sum = 0;

                        for (int i = 0; i < TOTAL; i++) {
                            Integer val = vals.get(i);

                            assertNotNull(val);

                            sum += val;
                        }

                        assertEquals(0, sum % N);
                    }

                    if (idx == 0) {
                        TestCache<Integer, Integer> cache = randomCache(caches, rnd);

                        Map<Integer, Integer> vals;

                        try {
                            vals = readAllByMode(cache.cache, keys, readMode, INTEGER_CODEC);
                        }
                        finally {
                            cache.readUnlock();
                        }

                        int sum = 0;

                        for (int i = 0; i < TOTAL; i++) {
                            Integer val = vals.get(i);

                            info("Value [id=" + i + ", val=" + val + ']');

                            sum += val;
                        }

                        info("Sum [sum=" + sum + ", mod=" + sum % N + ']');
                    }
                }
            };

        readWriteTest(
            restartMode,
            srvs,
            clients,
            cacheBackups,
            cacheParts,
            writers,
            readers,
            time,
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
            MvccProcessorImpl.coordinatorAssignClosure(new CoordinatorAssignClosure());

        Ignite srv0 = startGridsMultiThreaded(srvs);

        if (clients > 0) {
            client = true;

            startGridsMultiThreaded(srvs, clients);

            client = false;
        }

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(cacheMode(),
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
            }, readers, "reader");

            GridTestUtils.runAsync(() -> {
                while (System.currentTimeMillis() < stopTime)
                    doSleep(1000);

                stop.set(true);
            });

            while (System.currentTimeMillis() < stopTime && !stop.get()) {
                Thread.sleep(1000);

                if (System.currentTimeMillis() >= stopTime || stop.get())
                    break;

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

                            cache0 = new TestCache(srv.cache(DEFAULT_CACHE_NAME));

                            synchronized (caches) {
                                caches.set(idx, cache0);
                            }

                            awaitPartitionMapExchange();

                            break;
                        }

                        default:
                            fail();
                    }
                }
            }

            Exception ex = null;

            try {
                writeFut.get();
            }
            catch (IgniteCheckedException e) {
                ex = e;
            }

            try {
                readFut.get();
            }
            catch (IgniteCheckedException e) {
                if (ex != null)
                    ex.addSuppressed(e);
                else
                    ex = e;
            }

            if (ex != null)
                throw ex;
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
        ccfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
        ccfg.setWriteSynchronizationMode(syncMode);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, parts));

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * Handles transaction exception.
     * @param e Exception.
     */
    protected void handleTxException(Exception e) {
        if (log.isDebugEnabled())
            log.debug("Exception during tx execution: " + X.getFullStackTrace(e));

        if (X.hasCause(e, IgniteFutureCancelledCheckedException.class))
            return;

        if (X.hasCause(e, ClusterTopologyException.class))
            return;

        if (X.hasCause(e, ClusterTopologyCheckedException.class))
            return;

        if (X.hasCause(e, IgniteTxRollbackCheckedException.class))
            return;

        if (X.hasCause(e, TransactionException.class))
            return;

        if (X.hasCause(e, IgniteTxTimeoutCheckedException.class))
            return;

        if (X.hasCause(e, TransactionSerializationException.class))
            return;

        if (X.hasCause(e, CacheException.class)) {
            CacheException cacheEx = X.cause(e, CacheException.class);

            if (cacheEx != null && cacheEx.getMessage() != null) {
                if (cacheEx.getMessage().contains("Data node has left the grid during query execution"))
                    return;
            }

            if (cacheEx != null && cacheEx.getMessage() != null) {
                if (cacheEx.getMessage().contains("Query was interrupted."))
                    return;
            }

            if (cacheEx != null && cacheEx.getMessage() != null) {
                if (cacheEx.getMessage().contains("Failed to fetch data from node"))
                    return;
            }

            if (cacheEx != null && cacheEx.getMessage() != null) {
                if (cacheEx.getMessage().contains("Failed to send message"))
                    return;
            }
        }

        if (X.hasCause(e, IgniteSQLException.class)) {
            IgniteSQLException sqlEx = X.cause(e, IgniteSQLException.class);

            if (sqlEx != null && sqlEx.getMessage() != null) {
                if (sqlEx.getMessage().contains("Transaction is already completed."))
                    return;
            }
        }

        fail("Unexpected tx exception. " + X.getFullStackTrace(e));
    }

    /**
     * @throws Exception If failed.
     */
    final void verifyCoordinatorInternalState() throws Exception {
        for (Ignite node : G.allGrids()) {
            final MvccProcessorImpl crd = mvccProcessor(node);

            if (!crd.mvccEnabled())
                continue;

            crd.stopVacuumWorkers(); // to prevent new futures creation.

            Map activeTxs = GridTestUtils.getFieldValue(crd, "activeTxs");
            Map<?, Map> cntrFuts = GridTestUtils.getFieldValue(crd, "snapLsnrs");
            Map ackFuts = GridTestUtils.getFieldValue(crd, "ackFuts");
            Map activeTrackers = GridTestUtils.getFieldValue(crd, "activeTrackers");

            GridAbsPredicate cond = () -> {
                log.info("activeTxs=" + activeTxs + ", cntrFuts=" + cntrFuts + ", ackFuts=" + ackFuts +
                    ", activeTrackers=" + activeTrackers);

                boolean empty = true;

                for (Map map : cntrFuts.values())
                    if (!(empty = map.isEmpty()))
                        break;

                return activeTxs.isEmpty() && empty && ackFuts.isEmpty() && activeTrackers.isEmpty();
            };

            GridTestUtils.waitForCondition(cond, TX_TIMEOUT);

            assertTrue("activeTxs: " + activeTxs, activeTxs.isEmpty());

            boolean empty = true;

            for (Map map : cntrFuts.values())
                if (!(empty = map.isEmpty())) break;

            assertTrue("cntrFuts: " + cntrFuts, empty);
            assertTrue("ackFuts: " + ackFuts, ackFuts.isEmpty());
            assertTrue("activeTrackers: " + activeTrackers, activeTrackers.isEmpty());

            checkActiveQueriesCleanup(node);
        }
    }

    /**
     * Checks if less than 2 versions remain after the vacuum cleanup.
     *
     * @throws Exception If failed.
     */
    protected void verifyOldVersionsCleaned() throws Exception {
        boolean retry;

        try {
            runVacuumSync();

            // Check versions.
            retry = !checkOldVersions(false);
        }
        catch (Exception e) {
            U.warn(log(), "Failed to perform vacuum, will retry.", e);

            retry = true;
        }

        if (retry) { // Retry on a stable topology with a newer snapshot.
            awaitPartitionMapExchange();

            waitMvccQueriesDone();

            runVacuumSync();

            checkOldVersions(true);
        }
    }

    /**
     * Waits until all active queries are terminated on the Mvcc coordinator.
     *
     * @throws Exception If failed.
     */
    private void waitMvccQueriesDone() throws Exception {
        for (Ignite node : G.allGrids()) {
            checkActiveQueriesCleanup(node);
        }
    }

    /**
     * Checks if outdated versions were cleaned after the vacuum process.
     *
     * @param failIfNotCleaned Fail test if not cleaned.
     * @return {@code False} if not cleaned.
     * @throws IgniteCheckedException If failed.
     */
    private boolean checkOldVersions(boolean failIfNotCleaned) throws IgniteCheckedException {
        for (Ignite node : G.allGrids()) {
            for (IgniteCacheProxy cache : ((IgniteKernal)node).caches()) {
                GridCacheContext cctx = cache.context();

                if (!cctx.userCache() || !cctx.group().mvccEnabled() || F.isEmpty(cctx.group().caches()) || cctx.shared().closed(cctx))
                    continue;

                try (GridCloseableIterator it = (GridCloseableIterator)cache.withKeepBinary().iterator()) {
                    while (it.hasNext()) {
                        IgniteBiTuple entry = (IgniteBiTuple)it.next();

                        KeyCacheObject key = cctx.toCacheKeyObject(entry.getKey());

                        List<IgniteBiTuple<Object, MvccVersion>> vers = cctx.offheap().mvccAllVersions(cctx, key)
                            .stream().filter(t -> t.get1() != null).collect(Collectors.toList());

                        if (vers.size() > 1) {
                            if (failIfNotCleaned)
                                fail("[key=" + key.value(null, false) + "; vers=" + vers + ']');
                            else
                                return false;
                        }
                    }
                }
            }
        }

        return true;
    }

    /**
     * Runs vacuum on all nodes and waits for its completion.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void runVacuumSync() throws IgniteCheckedException {
        GridCompoundIdentityFuture<VacuumMetrics> fut = new GridCompoundIdentityFuture<>();

        // Run vacuum manually.
        for (Ignite node : G.allGrids()) {
            if (!node.configuration().isClientMode()) {
                MvccProcessorImpl crd = mvccProcessor(node);

                if (!crd.mvccEnabled() || GridTestUtils.getFieldValue(crd, "vacuumWorkers") == null)
                    continue;

                assert GridTestUtils.getFieldValue(crd, "txLog") != null;

                fut.add(crd.runVacuum());
            }
        }

        fut.markInitialized();

        // Wait vacuum finished.
        fut.get(getTestTimeout());
    }

    /**
     * @param node Ignite node.
     * @return Mvcc processor.
     */
    protected MvccProcessorImpl mvccProcessor(Ignite node) {
        GridKernalContext ctx = ((IgniteEx)node).context();

        MvccProcessor crd = ctx.coordinators();

        assertNotNull(crd);

        return (MvccProcessorImpl)crd;
    }

    /**
     * @param node Node.
     * @throws Exception If failed.
     */
    protected final void checkActiveQueriesCleanup(Ignite node) throws Exception {
        final MvccProcessorImpl prc = mvccProcessor(node);

        MvccCoordinator crd = prc.currentCoordinator();

        if (!crd.local())
            return;

        assertTrue("Coordinator is not initialized: " + prc, GridTestUtils.waitForCondition(crd::initialized, 8_000));

        assertTrue("Active queries are not cleared: " + node.name(), GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    Object activeQueries = GridTestUtils.getFieldValue(prc, "activeQueries");

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

        assertTrue("Previous coordinator queries are not empty: " + node.name(), GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    PreviousQueries prevQueries = GridTestUtils.getFieldValue(prc, "prevQueries");

                    synchronized (prevQueries) {
                        Map queries = GridTestUtils.getFieldValue(prevQueries, "active");
                        Boolean prevDone = GridTestUtils.getFieldValue(prevQueries, "done");

                        if (!queries.isEmpty() || !prevDone)
                            log.info("Previous coordinator state [prevDone=" + prevDone + ", queries=" + queries + ']');

                        return queries.isEmpty();
                    }
                }
            }, 8_000)
        );
    }

    /**
     * @return Cache configurations.
     */
    protected List<CacheConfiguration<Object, Object>> cacheConfigurations() {
        List<CacheConfiguration<Object, Object>> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, RendezvousAffinityFunction.DFLT_PARTITION_COUNT));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1, RendezvousAffinityFunction.DFLT_PARTITION_COUNT));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2, RendezvousAffinityFunction.DFLT_PARTITION_COUNT));
        ccfgs.add(cacheConfiguration(REPLICATED, FULL_SYNC, 0, RendezvousAffinityFunction.DFLT_PARTITION_COUNT));

        return ccfgs;
    }

    /**
     * Reads value from cache for the given key using given read mode.
     *
     * @param cache Cache.
     * @param key Key.
     * @param readMode Read mode.
     * @param codec Sql object codec.
     * @return Value.
     */
    @SuppressWarnings("unchecked")
    protected Object readByMode(IgniteCache cache, final Object key, ReadMode readMode, ObjectCodec codec) {
        assert cache != null && key != null && readMode != null && readMode != SQL_SUM;
        assert readMode != SQL || codec != null;

        boolean emulateLongQry = ThreadLocalRandom.current().nextBoolean();

        switch (readMode) {
            case GET:
                return cache.get(key);

            case SCAN:
                ScanQuery scanQry = new ScanQuery(new IgniteBiPredicate() {
                    @Override public boolean apply(Object k, Object v) {
                        if (emulateLongQry)
                            doSleep(ThreadLocalRandom.current().nextInt(50));

                        return k.equals(key);
                    }
                });

                List res = cache.query(scanQry).getAll();

                assertTrue(res.size() <= 1);

                return res.isEmpty() ? null : ((IgniteBiTuple)res.get(0)).getValue();

            case SQL:
                String qry = "SELECT * FROM " + codec.tableName() + " WHERE _key=" + key;

                SqlFieldsQuery sqlFieldsQry = new SqlFieldsQuery(qry);

                if (emulateLongQry)
                    sqlFieldsQry.setLazy(true).setPageSize(1);

                List<List> rows;

                if (emulateLongQry) {
                    FieldsQueryCursor<List> cur = cache.query(sqlFieldsQry);

                    rows = new ArrayList<>();

                    for (List row : cur) {
                        rows.add(row);

                        doSleep(ThreadLocalRandom.current().nextInt(50));
                    }
                }
                else
                    rows = cache.query(sqlFieldsQry).getAll();

                assertTrue(rows.size() <= 1);

                return rows.isEmpty() ? null : codec.decode(rows.get(0));

            default:
                throw new AssertionError("Unsupported read mode: " + readMode);
        }
    }

    /**
     * Writes value into cache using given write mode.
     *
     * @param cache Cache.
     * @param key Key.
     * @param val Value.
     * @param writeMode Write mode.
     * @param codec Sql object codec.
     */
    @SuppressWarnings("unchecked")
    protected void writeByMode(IgniteCache cache, final Object key, Object val, WriteMode writeMode, ObjectCodec codec) {
        assert writeMode != DML || codec != null;
        assert cache != null && key != null && writeMode != null && val != null;

        switch (writeMode) {
            case PUT:
                cache.put(key, val);

                return;

            case DML:
                String qry = "MERGE INTO " + codec.tableName() + " (" + codec.columnsNames() + ") VALUES " +
                    '(' + key + ", " + codec.encode(val) + ')';

                List<List> rows = cache.query(new SqlFieldsQuery(qry)).getAll();

                assertTrue(rows.size() <= 1);

                return;

            default:
                throw new AssertionError("Unsupported write mode: " + writeMode);
        }
    }


    /**
     * Reads value from cache for the given key using given read mode.
     *
     * @param cache Cache.
     * @param keys Key.
     * @param readMode Read mode.
     * @param codec Value codec.
     * @return Value.
     */
    @SuppressWarnings("unchecked")
    protected Map readAllByMode(IgniteCache cache, Set keys, ReadMode readMode, ObjectCodec codec) {
        assert cache != null && keys != null && readMode != null;
        assert readMode != SQL || codec != null;

        boolean emulateLongQry = ThreadLocalRandom.current().nextBoolean();

        switch (readMode) {
            case GET:
                return cache.getAll(keys);

            case SCAN:
                ScanQuery scanQry = new ScanQuery(new IgniteBiPredicate() {
                    @Override public boolean apply(Object k, Object v) {
                        if (emulateLongQry)
                            doSleep(ThreadLocalRandom.current().nextInt(50));

                        return keys.contains(k);
                    }
                });

                Map res;

                try (QueryCursor qry = cache.query(scanQry)) {
                    res = (Map)qry.getAll()
                        .stream()
                        .collect(Collectors.toMap(v -> ((IgniteBiTuple)v).getKey(), v -> ((IgniteBiTuple)v).getValue()));

                    assertTrue("res.size()=" + res.size() + ", keys.size()=" + keys.size(), res.size() <= keys.size());
                }

                return res;

            case SQL:
                StringBuilder b = new StringBuilder("SELECT " + codec.columnsNames() + " FROM " + codec.tableName() + " WHERE _key IN (");

                boolean first = true;

                for (Object key : keys) {
                    if (first)
                        first = false;
                    else
                        b.append(", ");

                    b.append(key);
                }

                b.append(')');

                String qry = b.toString();

                SqlFieldsQuery sqlFieldsQry = new SqlFieldsQuery(qry);

                if (emulateLongQry)
                    sqlFieldsQry.setLazy(true).setPageSize(1);

                List<List> rows;

                try (FieldsQueryCursor<List> cur = cache.query(sqlFieldsQry)) {
                    if (emulateLongQry) {
                        rows = new ArrayList<>();

                        for (List row : cur) {
                            rows.add(row);

                            doSleep(ThreadLocalRandom.current().nextInt(50));
                        }
                    }
                    else
                        rows = cur.getAll();
                }

                if (rows.isEmpty())
                    return Collections.emptyMap();

                res = new HashMap();

                for (List row : rows)
                    res.put(row.get(0), codec.decode(row));

                return res;

            case SQL_SUM:
                b = new StringBuilder("SELECT SUM(" + codec.aggregateColumnName() + ") FROM " + codec.tableName() + " WHERE _key IN (");

                first = true;

                for (Object key : keys) {
                    if (first)
                        first = false;
                    else
                        b.append(", ");

                    b.append(key);
                }

                b.append(')');

                qry = b.toString();

                FieldsQueryCursor<List> cur = cache.query(new SqlFieldsQuery(qry));

                rows = cur.getAll();

                if (rows.isEmpty())
                    return Collections.emptyMap();

                res = new HashMap();

                for (List row : rows)
                    res.put(row.get(0), row.get(0));

                return res;

            default:
                throw new AssertionError("Unsupported read mode: " + readMode);
        }
    }

    /**
     * Writes all entries using given write mode.
     *
     * @param cache Cache.
     * @param entries Entries to write.
     * @param writeMode Write mode.
     * @param codec Entry codec.
     */
    @SuppressWarnings("unchecked")
    protected void writeAllByMode(IgniteCache cache, final Map entries, WriteMode writeMode, ObjectCodec codec) {
        assert cache != null && entries != null && writeMode != null;
        assert writeMode != DML || codec != null;

        switch (writeMode) {
            case PUT:
                cache.putAll(entries);

                return;

            case DML:
                StringBuilder b = new StringBuilder("MERGE INTO " + codec.tableName() + " (" + codec.columnsNames() + ") VALUES ");

                boolean first = true;

                for (Object entry : entries.entrySet()) {
                    Map.Entry e = (Map.Entry)entry;
                    if (first)
                        first = false;
                    else
                        b.append(", ");

                    b.append('(')
                        .append(e.getKey())
                        .append(", ")
                        .append(codec.encode(e.getValue()))
                        .append(')');
                }

                String qry = b.toString();

                cache.query(new SqlFieldsQuery(qry)).getAll();

                return;

            default:
                throw new AssertionError("Unsupported write mode: " + writeMode);
        }
    }

    /**
     * Object codec for SQL queries.
     *
     * @param <T> Type.
     */
    private interface ObjectCodec<T> {
        /**
         * Decodes object from SQL request result.
         *
         * @param row SQL request result.
         * @return Decoded object.
         */
        T decode(List<?> row);

        /**
         * Encodes object into SQL string for INSERT clause.
         *
         * @param obj Object.
         * @return Sql string.
         */
        String encode(T obj);

        /**
         * @return Table name.
         */
        String tableName();

        /**
         * @return Columns names.
         */
        String columnsNames();

        /**
         * @return Column for aggregate functions.
         */
        String aggregateColumnName();
    }

    /**
     * Codec for {@code Integer} table.
     */
    private static class IntegerCodec implements ObjectCodec<Integer> {
        /** {@inheritDoc} */
        @Override public Integer decode(List<?> row) {
            return (Integer)row.get(1);
        }

        /** {@inheritDoc} */
        @Override public String encode(Integer obj) {
            return String.valueOf(obj);
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return "Integer";
        }

        /** {@inheritDoc} */
        @Override public String columnsNames() {
            return "_key, _val";
        }

        /** {@inheritDoc} */
        @Override public String aggregateColumnName() {
            return "_val";
        }
    }

    /**
     * Codec for {@code MvccTestAccount} table.
     */
    private static class AccountCodec implements ObjectCodec<MvccTestAccount> {
        /** {@inheritDoc} */
        @Override public MvccTestAccount decode(List<?> row) {
            Integer val = (Integer)row.get(1);
            Integer updateCnt = (Integer)row.get(2);

            return new MvccTestAccount(val, updateCnt);
        }

        /** {@inheritDoc} */
        @Override public String encode(MvccTestAccount obj) {
            return String.valueOf(obj.val) + ", " + String.valueOf(obj.updateCnt);
        }

        /** {@inheritDoc} */
        @Override public String tableName() {
            return "MvccTestAccount";
        }

        /** {@inheritDoc} */
        @Override public String columnsNames() {
            return "_key, val, updateCnt";
        }

        /** {@inheritDoc} */
        @Override public String aggregateColumnName() {
            return "val";
        }
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
        @QuerySqlField
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
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MvccTestAccount account = (MvccTestAccount)o;
            return val == account.val &&
                updateCnt == account.updateCnt;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {

            return Objects.hash(val, updateCnt);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MvccTestAccount{" +
                "val=" + val +
                ", updateCnt=" + updateCnt +
                '}';
        }
    }

    /**
     *
     */
    enum ReadMode {
        /** */
        GET,

        /** */
        SCAN,

        /** */
        SQL,

        /** */
        SQL_SUM,

        /** */
        INVOKE
    }

    /**
     *
     */
    enum WriteMode {
        /** */
        DML,

        /** */
        PUT,

        /** */
        INVOKE
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
                    assert !node.isClient();

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

    /**
     *
     */
    static class InitIndexing implements IgniteInClosure<CacheConfiguration> {
        /** */
        private final Class[] idxTypes;

        /**
         * @param idxTypes Indexed types.
         */
        InitIndexing(Class<?>... idxTypes) {
            this.idxTypes = idxTypes;
        }

        /** {@inheritDoc} */
        @Override public void apply(CacheConfiguration cfg) {
            cfg.setIndexedTypes(idxTypes);
        }
    }

    /**
     * Removed accounts tracker.
     */
    private static class RemovedAccountsTracker {
        /** */
        private final Map<Integer, Integer> rmvdKeys;

        /**
         * @param size Size.
         */
        RemovedAccountsTracker(int size) {
            this.rmvdKeys = new HashMap<>(size);

            for (int i = 0; i < size; i++)
                rmvdKeys.put(i, 0);
        }

        /**
         * @return Size.
         */
        public synchronized int size() {
            int size = 0;

            for (int i = 0; i < rmvdKeys.size(); i++) {
                if (rmvdKeys.get(i) > 0)
                    size++;
            }

            return size;
        }

        /**
         * @param id Id.
         * @return {@code True} if success.
         */
        synchronized boolean markRemoved(Integer id) {
            Integer rmvdCntr = rmvdKeys.get(id);

            Integer newCntr = rmvdCntr + 1;

            rmvdKeys.put(id, newCntr);

            return newCntr >= 0;
        }

        /**
         * @param id Id.
         * @return {@code True} if success.
         */
        synchronized boolean unmarkRemoved(Integer id) {
            Integer rmvdCntr = rmvdKeys.get(id);

            Integer newCntr = rmvdCntr - 1;

            rmvdKeys.put(id, newCntr);

            return newCntr >= 0;
        }
    }
}
