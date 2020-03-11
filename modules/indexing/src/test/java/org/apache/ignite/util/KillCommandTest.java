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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ComputeMXBeanImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.QueryMXBeanImpl;
import org.apache.ignite.internal.ServiceMXBeanImpl;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.mxbean.ServiceMXBean;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.queryProcessor;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class KillCommandTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    public static final String SVC_NAME = "my-svc";

    /** */
    private static final int PAGE_SZ = 5;

    /** */
    private static final int  NODES_CNT = 3;

    /** @throws Exception If failed. */
    @Test
    public void testCancelScanQuery() throws Exception {
        injectTestSystemOut();

        IgniteEx ignite0 = startGrids(NODES_CNT);
        IgniteEx client = startClientGrid("client");

        ignite0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);
        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Object, Object>> qry1 = cache.query(new ScanQuery<>().setPageSize(PAGE_SZ));
        Iterator<Cache.Entry<Object, Object>> iter1 = qry1.iterator();

        // Fetch first entry and therefore caching first page.
        assertNotNull(iter1.next());

        List<List<?>> scanQries0 = SqlViewExporterSpiTest.execute(ignite0,
            "SELECT ORIGIN_NODE_ID, CACHE_NAME, QUERY_ID FROM SYS.SCAN_QUERIES");

        assertEquals(1, scanQries0.size());

        QueryMXBean qryMBean = getMxBean(client.name(), "Query",
            QueryMXBeanImpl.class.getSimpleName(), QueryMXBean.class);

        UUID originNodeId = (UUID)scanQries0.get(0).get(0);
        String cacheName = (String)scanQries0.get(0).get(1);
        long qryId = (Long)scanQries0.get(0).get(2);

        // Opens second query.
        QueryCursor<Cache.Entry<Object, Object>> qry2 = cache.query(new ScanQuery<>().setPageSize(PAGE_SZ));
        Iterator<Cache.Entry<Object, Object>> iter2 = qry2.iterator();

        // Fetch first entry and therefore caching first page.
        assertNotNull(iter2.next());

        // Cancel first query.
        qryMBean.cancelScan(originNodeId.toString(), cacheName, qryId);

        // Fetch all cached entries. It's size equal to the {@code PAGE_SZ}.
        for (int i = 0; i < PAGE_SZ * NODES_CNT - 1; i++)
            assertNotNull(iter1.next());

        // Fetch of the next page should throw the exception.
        assertThrowsWithCause(iter1::next, IgniteCheckedException.class);

        // Checking that second query works fine after canceling first.
        for (int i = 0; i < PAGE_SZ * PAGE_SZ - 1; i++)
            assertNotNull(iter2.next());

        // Checking all server node objects cleared after cancel.
        for (int i = 0; i < NODES_CNT; i++) {
            IgniteEx ignite = grid(i);

            int cacheId = CU.cacheId(DEFAULT_CACHE_NAME);

            GridCacheContext<?, ?> ctx = ignite.context().cache().context().cacheContext(cacheId);

            ConcurrentMap<UUID, ? extends GridCacheQueryManager<?, ?>.RequestFutureMap> qryIters =
                ctx.queries().queryIterators();

            assertTrue(qryIters.size() <= 1);

            if (qryIters.isEmpty())
                return;

            GridCacheQueryManager<?, ?>.RequestFutureMap futs = qryIters.get(client.localNode().id());

            assertNotNull(futs);
            assertFalse(futs.containsKey(qryId));
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelSQLQuery() throws Exception {
        startGrids(NODES_CNT);
        IgniteEx client = startClientGrid("client");

        client.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Integer.class));

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT _KEY, _VAL FROM INTEGER").setSchema("default").setPageSize(10);
        Iterator<List<?>> iter = queryProcessor(client).querySqlFields(qry, true).iterator();

        assertNotNull(iter.next());

        List<List<?>> sqlQries = SqlViewExporterSpiTest.execute(client,
            "SELECT * FROM SYS.SQL_QUERIES ORDER BY START_TIME");
        assertEquals(2, sqlQries.size());

        String qryId = (String)sqlQries.get(0).get(0);
        assertEquals("SELECT _KEY, _VAL FROM INTEGER", sqlQries.get(0).get(1));

        QueryMXBean qryMBean = getMxBean(client.name(), "Query",
            QueryMXBeanImpl.class.getSimpleName(), QueryMXBean.class);

        qryMBean.cancelSQL(qryId);

        while(iter.hasNext())
            assertNotNull(iter.next());

        fail("You shouldn't be here!");
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelTx() throws Exception {
        IgniteEx ignite0 = startGrids(NODES_CNT);
        IgniteEx client = startClientGrid("client");

        ignite0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        for (TransactionConcurrency txConc : TransactionConcurrency.values()) {
            for (TransactionIsolation txIsolation : TransactionIsolation.values()) {
                try (Transaction tx = client.transactions().txStart(txConc, txIsolation)) {
                    cache.put(1, 1);

                    List<List<?>> txs = SqlViewExporterSpiTest.execute(client, "SELECT xid FROM SYS.TRANSACTIONS");
                    assertEquals(1, txs.size());

                    String xid = (String)txs.get(0).get(0);

                    TransactionsMXBean txMBean = getMxBean(client.name(), "Transactions",
                        TransactionsMXBeanImpl.class.getSimpleName(), TransactionsMXBean.class);

                    txMBean.cancel(xid);

                    assertThrowsWithCause(tx::commit, IgniteException.class);

                    for (int i = 0; i < NODES_CNT; i++) {
                        txs = SqlViewExporterSpiTest.execute(grid(i), "SELECT xid FROM SYS.TRANSACTIONS");
                        assertEquals(0, txs.size());
                    }
                }

                assertNull(cache.get(1));
            }
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelContinuousQuery() throws Exception {
        IgniteEx ignite0 = startGrids(NODES_CNT);
        IgniteEx client = startClientGrid("client");

        ignite0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = client.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

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

        boolean res = waitForCondition(() -> cntr.get() == PAGE_SZ * PAGE_SZ, 10_000);
        assertTrue(res);

        List<List<?>> cqQries = SqlViewExporterSpiTest.execute(client,
            "SELECT ROUTINE_ID FROM SYS.CONTINUOUS_QUERIES");
        assertEquals(1, cqQries.size());

        QueryMXBean qryMBean = getMxBean(client.name(), "Query",
            QueryMXBeanImpl.class.getSimpleName(), QueryMXBean.class);

        qryMBean.cancelContinuous(((UUID)cqQries.get(0).get(0)).toString());

        long cnt = cntr.get();

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        res = waitForCondition(() -> cntr.get() > cnt, 5_000);
        assertFalse(res);

        for (int i = 0; i < NODES_CNT; i++) {
            cqQries = SqlViewExporterSpiTest.execute(grid(i),
                "SELECT ROUTINE_ID FROM SYS.CONTINUOUS_QUERIES");

            assertTrue(cqQries.isEmpty());
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelComputeTask() throws Exception {
        IgniteEx ignite0 = startGrids(1);
        IgniteEx client = startClientGrid("client");

        ignite0.cluster().state(ACTIVE);

        IgniteFuture<Collection<Integer>> fut = client.compute().broadcastAsync(() -> {
            Thread.sleep(60_000L);

            fail("Task should be killed!");

            return 1;
        });

        String[] id = new String[1];

        boolean res = waitForCondition(() -> {
            List<List<?>> tasks = SqlViewExporterSpiTest.execute(ignite0, "SELECT SESSION_ID FROM SYS.JOBS");

            if (tasks.size() == 1) {
                id[0] = (String)tasks.get(0).get(0);

                return true;
            }

            return false;
        }, 10_000);

        assertTrue(res);

        ComputeMXBean computeMBean = getMxBean(client.name(), "Compute",
            ComputeMXBeanImpl.class.getSimpleName(), ComputeMXBean.class);

        computeMBean.cancel(id[0]);

        assertThrowsWithCause((Callable<Collection<Integer>>)fut::get, IgniteException.class);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelService() throws Exception {
        IgniteEx ignite0 = startGrids(1);
        IgniteEx client = startClientGrid("client");

        ignite0.cluster().state(ACTIVE);

        ServiceConfiguration scfg = new ServiceConfiguration();

        scfg.setName(SVC_NAME);
        scfg.setMaxPerNodeCount(1);
        scfg.setNodeFilter(n -> n.id().equals(ignite0.localNode().id()));
        scfg.setService(new TestServiceImpl());

        client.services().deploy(scfg);

        SystemView<ServiceView> svcView = ignite0.context().systemView().view(SVCS_VIEW);

        boolean res = waitForCondition(() -> svcView.size() == 1, 5_000L);

        assertTrue(res);

        TestService svc = client.services().serviceProxy("my-svc", TestService.class, true);

        assertNotNull(svc);

        IgniteInternalFuture fut = runAsync(svc::doTheJob);

        ServiceMXBean svcMxBean = getMxBean(client.name(), "Service",
            ServiceMXBeanImpl.class.getSimpleName(), ServiceMXBean.class);

        svcMxBean.cancel("my-svc");

        res = waitForCondition(() -> svcView.size() == 1, 5_000L);

        assertTrue(res);
    }

    /** */
    public interface TestService extends Service {
        /** */
        public void doTheJob();
    }

    public static class TestServiceImpl implements TestService {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
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
                Thread.sleep(60_000L);

                fail("Task should be killed!");
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
