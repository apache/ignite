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

package org.apache.ignite.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.client.Config.DEFAULT_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.queryProcessor;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * General tests for the cancel command.
 */
class KillCommandsTests {
    /** */
    public static final String SVC_NAME = "my-svc";

    /** */
    public static final int PAGE_SZ = 5;

    /** */
    public static final int TIMEOUT = 10_000;

    /** */
    private static volatile CyclicBarrier svcStartBarrier;

    /** */
    private static volatile CyclicBarrier svcCancelBarrier;

    /**
     * Test cancel of the scan query.
     *
     * @param cli Client node.
     * @param srvs Server nodes.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestScanQueryCancel(IgniteEx cli, List<IgniteEx> srvs, Consumer<T3<UUID, String, Long>> qryCanceler) {
        IgniteCache<Object, Object> cache = cli.cache(GridAbstractTest.DEFAULT_CACHE_NAME);

        QueryCursor<Cache.Entry<Object, Object>> qry1 = cache.query(new ScanQuery<>().setPageSize(PAGE_SZ));
        Iterator<Cache.Entry<Object, Object>> iter1 = qry1.iterator();

        // Fetch first entry and therefore caching first page.
        assertNotNull(iter1.next());

        List<List<?>> scanQries0 = SqlViewExporterSpiTest.execute(srvs.get(0),
            "SELECT ORIGIN_NODE_ID, CACHE_NAME, QUERY_ID FROM SYS.SCAN_QUERIES");

        assertEquals(1, scanQries0.size());

        UUID originNodeId = (UUID)scanQries0.get(0).get(0);
        String cacheName = (String)scanQries0.get(0).get(1);
        long qryId = (Long)scanQries0.get(0).get(2);

        // Opens second query.
        QueryCursor<Cache.Entry<Object, Object>> qry2 = cache.query(new ScanQuery<>().setPageSize(PAGE_SZ));
        Iterator<Cache.Entry<Object, Object>> iter2 = qry2.iterator();

        // Fetch first entry and therefore caching first page.
        assertNotNull(iter2.next());

        // Cancel first query.
        qryCanceler.accept(new T3<>(originNodeId, cacheName, qryId));

        // Fetch all cached entries. It's size equal to the {@code PAGE_SZ * NODES_CNT}.
        for (int i = 0; i < PAGE_SZ * srvs.size() - 1; i++)
            assertNotNull(iter1.next());

        // Fetch of the next page should throw the exception.
        assertThrowsWithCause(iter1::next, IgniteCheckedException.class);

        // Checking that second query works fine after canceling first.
        for (int i = 0; i < PAGE_SZ * PAGE_SZ - 1; i++)
            assertNotNull(iter2.next());

        // Checking all server node objects cleared after cancel.
        for (int i = 0; i < srvs.size(); i++) {
            IgniteEx ignite = srvs.get(i);

            int cacheId = CU.cacheId(DEFAULT_CACHE_NAME);

            GridCacheContext<?, ?> ctx = ignite.context().cache().context().cacheContext(cacheId);

            ConcurrentMap<UUID, ? extends GridCacheQueryManager<?, ?>.RequestFutureMap> qryIters =
                ctx.queries().queryIterators();

            assertTrue(qryIters.size() <= 1);

            if (qryIters.isEmpty())
                return;

            GridCacheQueryManager<?, ?>.RequestFutureMap futs = qryIters.get(cli.localNode().id());

            assertNotNull(futs);
            assertFalse(futs.containsKey(qryId));
        }
    }

    /**
     * Test cancel of the SQL query.
     *
     * @param cli Client node.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestCancelSQLQuery(IgniteEx cli, Consumer<String> qryCanceler) throws Exception {
        String qryStr = "SELECT * FROM \"default\".Integer";

        SqlFieldsQuery qry = new SqlFieldsQuery(qryStr).setPageSize(PAGE_SZ);
        Iterator<List<?>> iter = queryProcessor(cli).querySqlFields(qry, true).iterator();

        assertNotNull(iter.next());

        List<List<?>> sqlQries = SqlViewExporterSpiTest.execute(cli,
            "SELECT * FROM SYS.SQL_QUERIES ORDER BY START_TIME");
        assertEquals(2, sqlQries.size());

        String qryId = (String)sqlQries.get(0).get(0);
        assertEquals(qryStr, sqlQries.get(0).get(1));

        qryCanceler.accept(qryId);

        for (int i=0; i < PAGE_SZ - 2; i++)
            assertNotNull(iter.next());

        assertThrowsWithCause(iter::next, CacheException.class);
    }

    /**
     * Test cancel of the transaction.
     *
     * @param cli Client node.
     * @param srvs Server nodes.
     * @param txCanceler Transaction cancel closure.
     */
    public static void doTestCancelTx(IgniteEx cli, List<IgniteEx> srvs, Consumer<String> txCanceler) throws Exception {
        IgniteCache<Object, Object> cache = cli.cache(DEFAULT_CACHE_NAME);

        int testKey = PAGE_SZ * PAGE_SZ + 42;

        for (TransactionConcurrency txConc : TransactionConcurrency.values()) {
            for (TransactionIsolation txIsolation : TransactionIsolation.values()) {
                try (Transaction tx = cli.transactions().txStart(txConc, txIsolation)) {
                    cache.put(testKey, 1);

                    List<List<?>> txs = SqlViewExporterSpiTest.execute(cli, "SELECT xid FROM SYS.TRANSACTIONS");
                    assertEquals(1, txs.size());

                    String xid = (String)txs.get(0).get(0);

                    txCanceler.accept(xid);

                    assertThrowsWithCause(tx::commit, IgniteException.class);

                    for (int i = 0; i < srvs.size(); i++) {
                        txs = SqlViewExporterSpiTest.execute(srvs.get(i), "SELECT xid FROM SYS.TRANSACTIONS");

                        assertEquals(0, txs.size());
                    }
                }

                assertNull(cache.get(testKey));
            }
        }
    }

    /**
     * Test cancel of the continuous query.
     *
     * @param cli Client node.
     * @param srvs Server nodes.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestCancelContinuousQuery(IgniteEx cli, List<IgniteEx> srvs, Consumer<UUID> qryCanceler) throws Exception {
        IgniteCache<Object, Object> cache = cli.cache(DEFAULT_CACHE_NAME);

        ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();

        AtomicInteger cntr = new AtomicInteger();

        cq.setInitialQuery(new ScanQuery<>());
        cq.setTimeInterval(1_000L);
        cq.setPageSize(PAGE_SZ);
        cq.setLocalListener(events -> {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> e : events) {
                assertNotNull(e);

                cntr.incrementAndGet();
            }
        });

        QueryCursor<Cache.Entry<Integer, Integer>> qry = cache.query(cq);

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        boolean res = waitForCondition(() -> cntr.get() == PAGE_SZ * PAGE_SZ, TIMEOUT);
        assertTrue(res);

        List<List<?>> cqQries = SqlViewExporterSpiTest.execute(cli,
            "SELECT ROUTINE_ID FROM SYS.CONTINUOUS_QUERIES");
        assertEquals(1, cqQries.size());

        UUID routineId = (UUID)cqQries.get(0).get(0);

        qryCanceler.accept(routineId);

        long cnt = cntr.get();

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        res = waitForCondition(() -> cntr.get() > cnt, TIMEOUT);

        assertFalse(res);

        for (int i = 0; i < srvs.size(); i++) {
            IgniteEx srv = srvs.get(i);

            res = waitForCondition(() -> SqlViewExporterSpiTest.execute(srv,
                    "SELECT ROUTINE_ID FROM SYS.CONTINUOUS_QUERIES").isEmpty(), TIMEOUT);

            assertTrue(srv.configuration().getIgniteInstanceName(), res);
        }
    }

    /**
     * Test cancel of the compute task.
     *
     * @param cli Client node.
     * @param srvs Server nodes.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestCancelComputeTask(IgniteEx cli, List<IgniteEx> srvs, Consumer<String> qryCanceler) throws Exception {
        IgniteFuture<Collection<Integer>> fut = cli.compute().broadcastAsync(() -> {
            Thread.sleep(10 * TIMEOUT);

            fail("Task should be killed!");

            return 1;
        });

        String[] id = new String[1];

        boolean res = waitForCondition(() -> {
            List<List<?>> tasks = SqlViewExporterSpiTest.execute(srvs.get(0), "SELECT SESSION_ID FROM SYS.JOBS");

            if (tasks.size() == 1) {
                id[0] = (String)tasks.get(0).get(0);

                return true;
            }

            return false;
        }, TIMEOUT);

        assertTrue(res);

        qryCanceler.accept(id[0]);

        for (IgniteEx srv : srvs) {
            res = waitForCondition(() -> {
                List<List<?>> tasks = SqlViewExporterSpiTest.execute(srv, "SELECT SESSION_ID FROM SYS.JOBS");

                return tasks.isEmpty();
            }, TIMEOUT);

            assertTrue(res);
        }

        assertThrowsWithCause(() -> fut.get(TIMEOUT), IgniteException.class);
    }

    /**
     * Test cancel of the compute task.
     *
     * @param cli Client node.
     * @param srv Server node.
     * @param svcCanceler Service cancel closure.
     */
    public static void doTestCancelService(IgniteEx cli, IgniteEx srv, Consumer<String> svcCanceler) throws Exception {
        ServiceConfiguration scfg = new ServiceConfiguration();

        scfg.setName(SVC_NAME);
        scfg.setMaxPerNodeCount(1);
        scfg.setNodeFilter(n -> n.id().equals(srv.localNode().id()));
        scfg.setService(new TestServiceImpl());

        cli.services().deploy(scfg);

        SystemView<ServiceView> svcView = srv.context().systemView().view(SVCS_VIEW);

        boolean res = waitForCondition(() -> svcView.size() == 1, TIMEOUT);

        assertTrue(res);

        TestService svc = cli.services().serviceProxy("my-svc", TestService.class, true);

        assertNotNull(svc);

        svcStartBarrier = new CyclicBarrier(2);
        svcCancelBarrier = new CyclicBarrier(2);

        IgniteInternalFuture<?> fut = runAsync(svc::doTheJob);

        svcStartBarrier.await(TIMEOUT, MILLISECONDS);

        svcCanceler.accept(SVC_NAME);

        res = waitForCondition(() -> svcView.size() == 0, TIMEOUT);

        assertTrue(res);

        fut.get(TIMEOUT);
    }

    /** */
    public interface TestService extends Service {
        /** */
        public void doTheJob();
    }

    public static class TestServiceImpl implements TestService {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            try {
                if (svcCancelBarrier != null)
                    svcCancelBarrier.await(TIMEOUT, MILLISECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void doTheJob() {
            try {
                svcStartBarrier.await(TIMEOUT, MILLISECONDS);

                svcCancelBarrier.await(TIMEOUT, MILLISECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
