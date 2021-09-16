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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for KILL queries.
 */
public class KillCommandDdlIntegrationTest extends AbstractDdlIntegrationTest {
    /** Page size. */
    public static final int PAGE_SZ = 5;

    /** Number of pages to insert. */
    public static final int PAGES_CNT = 1000;

    /** Operations timeout. */
    public static final int TIMEOUT = 10_000;


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteCache<Object, Object> cache = client.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, Integer.class)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        // There must be enough cache entries to keep scan query cursor opened.
        // Cursor may be concurrently closed when all the data retrieved.
        for (int i = 0; i < PAGES_CNT * PAGE_SZ; i++)
            cache.put(i, i);
    }

    /** */
    @Override public void cleanUp() {
        // No-op.
    }

    /** */
    @Test
    public void testCancelScanQuery() {
        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        QueryCursor<Cache.Entry<Object, Object>> scanQry = cache.query(new ScanQuery<>().setPageSize(PAGE_SZ));
        Iterator<Cache.Entry<Object, Object>> scanQryIter = scanQry.iterator();

        // Fetch first entry and therefore caching first page.
        assertNotNull(scanQryIter.next());

        ConcurrentMap<UUID, GridCacheQueryManager<Object, Object>.RequestFutureMap> qryIters =
            grid(0).context().cache().cache(DEFAULT_CACHE_NAME).context().queries().queryIterators();

        assertEquals(qryIters.values().size(), 1);

        long qryId = qryIters.values().iterator().next().keySet().iterator().next();
        UUID originNodeId = client.cluster().localNode().id();

        executeSql(client, "KILL SCAN '" + originNodeId + "' '" + DEFAULT_CACHE_NAME + "' " + qryId);

        // Fetch all cached entries.
        for (int i = 0; i < PAGE_SZ * servers().size() - 1; i++)
            assertNotNull(scanQryIter.next());

        // Fetch of the next page should throw the exception.
        assertThrowsWithCause(scanQryIter::next, IgniteCheckedException.class);
    }

    /** */
    @Test
    public void testCancelComputeTask() {
        CountDownLatch computeLatch = new CountDownLatch(1);

        IgniteFuture<Collection<Integer>> fut = client.compute().broadcastAsync(() -> {
            computeLatch.await();

            return 1;
        });

        try {
            IgniteUuid taskId = client.compute().activeTaskFutures().keySet().iterator().next();

            executeSql(client, "KILL COMPUTE '" + taskId + "'");

            assertThrowsWithCause(() -> fut.get(TIMEOUT), IgniteException.class);
        } finally {
            computeLatch.countDown();
        }
    }

    /** */
    @Test
    public void testCancelTx() {
        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        int testKey = PAGES_CNT * (PAGE_SZ + 1);

        try (Transaction tx = client.transactions().txStart()) {
            cache.put(testKey, 1);

            executeSql(client, "KILL TRANSACTION '" + tx.xid() + "'");

            assertThrowsWithCause(tx::commit, IgniteException.class);
        }

        assertNull(cache.get(testKey));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelService() throws Exception {
        String serviceName = "MY_SERVICE";

        ServiceConfiguration scfg = new ServiceConfiguration();
        scfg.setName(serviceName);
        scfg.setMaxPerNodeCount(1);
        scfg.setNodeFilter(grid(0).cluster().predicate());
        scfg.setService(new TestServiceImpl());

        client.services().deploy(scfg);

        TestService svc = client.services().serviceProxy(serviceName, TestService.class, true);
        assertNotNull(svc);

        executeSql(client, "KILL SERVICE '" + serviceName + "'");
        boolean res = waitForCondition(() -> grid(0).services().serviceDescriptors().isEmpty(), TIMEOUT);
        assertTrue(res);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelContinuousQuery() throws Exception {
        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

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

        cache.query(cq);

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        boolean res = waitForCondition(() -> cntr.get() == PAGE_SZ * PAGE_SZ, TIMEOUT);
        assertTrue(res);

        Map<UUID, Object> routines = getFieldValue(grid(0).context().continuous(), "rmtInfos");
        assertEquals(1, routines.size());
        Map.Entry<UUID, Object> entry = routines.entrySet().iterator().next();

        UUID nodeId = getFieldValue(entry.getValue(), "nodeId");
        UUID routineId = entry.getKey();

        executeSql(client, "KILL CONTINUOUS '" + nodeId + "' '" + routineId + "'");

        long cnt = cntr.get();

        for (int i = 0; i < PAGE_SZ * PAGE_SZ; i++)
            cache.put(i, i);

        res = waitForCondition(() -> cntr.get() > cnt, TIMEOUT);

        assertFalse(res);
    }

    /** */
    @Test
    public void testCancelUnknownScanQuery() {
        executeSql(client, "KILL SCAN '" + client.localNode().id() + "' 'unknown' 1");
    }

    /** */
    @Test
    public void testCancelUnknownComputeTask() {
        executeSql(client, "KILL COMPUTE '" + IgniteUuid.randomUuid() + "'");
    }

    /** */
    @Test
    public void testCancelUnknownService() {
        executeSql(client, "KILL SERVICE 'unknown'");
    }

    /** */
    @Test
    public void testCancelUnknownTx() {
        executeSql(client, "KILL TRANSACTION 'unknown'");
    }


    /** */
    @Test
    public void testCancelUnknownContinuousQuery() {
        executeSql(client, "KILL CONTINUOUS '" + grid(0).localNode().id() + "' '" + UUID.randomUUID() + "'");
    }

    private static List<Ignite> servers() {
        return G.allGrids().stream().filter(g -> !g.cluster().localNode().isClient()).collect(Collectors.toList());
    }

    /** */
    public interface TestService extends Service {
        /** */
        public void doTheJob();
    }

    /** */
    public static class TestServiceImpl implements TestService {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void doTheJob() {
            // No-op.
        }
    }
}
