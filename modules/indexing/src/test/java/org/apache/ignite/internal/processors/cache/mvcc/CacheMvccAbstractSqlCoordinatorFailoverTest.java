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

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SCAN;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.DML;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Mvcc SQL API coordinator failover test.
 */
public abstract class CacheMvccAbstractSqlCoordinatorFailoverTest extends CacheMvccAbstractBasicCoordinatorFailoverTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxSql_Server_Backups0_CoordinatorFails() throws Exception {
        accountsTxReadAll(2, 1, 0, 64,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11311")
    @Test
    public void testAccountsTxSql_SingleNode_CoordinatorFails_Persistence() throws Exception {
        persistence = true;

        accountsTxReadAll(1, 0, 0, 1,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllGetAll_ClientServer_Backups0_RestartCoordinator_ScanDml() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD, 2, 1, 0, 64,
            new InitIndexing(Integer.class, Integer.class), SCAN, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10767")
    @Test
    public void testPutAllGetAll_SingleNode_RestartCoordinator_ScanDml_Persistence() throws Exception {
        persistence = true;

        putAllGetAll(RestartMode.RESTART_CRD, 1, 0, 0, 1,
            new InitIndexing(Integer.class, Integer.class), SCAN, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllGetAll_ClientServer_Backups0_RestartCoordinator_SqlDml() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD, 2, 1, 0, DFLT_PARTITION_COUNT,
            new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllGetAll_SingleNode_RestartCoordinator_SqlDml_Persistence() throws Exception {
        persistence = true;

        putAllGetAll(RestartMode.RESTART_CRD, 1, 0, 0, 1,
            new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate_N_Objects_ClientServer_Backups0_Sql_Persistence() throws Exception {
        persistence = true;

        updateNObjectsTest(5, 2, 0, 0, 64, DFLT_TEST_TIME,
            new InitIndexing(Integer.class, Integer.class), SQL, DML, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate_N_Objects_SingleNode_Sql_Persistence() throws Exception {
        updateNObjectsTest(3, 1, 0, 0, 1, DFLT_TEST_TIME,
            new InitIndexing(Integer.class, Integer.class), SQL, DML, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorFailureSimplePessimisticTxSql() throws Exception {
        coordinatorFailureSimple(PESSIMISTIC, REPEATABLE_READ, SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxInProgressCoordinatorChangeSimple_Readonly() throws Exception {
        txInProgressCoordinatorChangeSimple(PESSIMISTIC, REPEATABLE_READ,
            new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadInProgressCoordinatorFailsSimple_FromClient() throws Exception {
        readInProgressCoordinatorFailsSimple(true, new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorChangeActiveQueryClientFails_Simple() throws Exception {
        checkCoordinatorChangeActiveQueryClientFails_Simple(new InitIndexing(Integer.class, Integer.class), SQL, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorChangeActiveQueryClientFails_SimpleScan() throws Exception {
        checkCoordinatorChangeActiveQueryClientFails_Simple(new InitIndexing(Integer.class, Integer.class), SCAN, DML);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxReadAfterCoordinatorChangeDirectOrder() throws Exception {
        testTxReadAfterCoordinatorChange(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxReadAfterCoordinatorChangeReverseOrder() throws Exception {
        testTxReadAfterCoordinatorChange(false);
    }

    /** */
    private void testTxReadAfterCoordinatorChange(boolean directOrder) throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class).setNodeFilter(new CoordinatorNodeFilter());

        MvccProcessorImpl.coordinatorAssignClosure(new CoordinatorAssignClosure());

        IgniteEx node = startGrid(0);

        nodeAttr = CRD_ATTR;

        startGrid(1);

        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

        cache.put(1,1);

        Semaphore sem = new Semaphore(0);

        IgniteInternalFuture future = GridTestUtils.runAsync(() -> {
            IgniteCache<Integer, Integer> cache0 = node.cache(DEFAULT_CACHE_NAME);

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Integer res = cache0.get(1);

                sem.release();

                // wait for coordinator change.
                assertTrue(sem.tryAcquire(2, getTestTimeout(), TimeUnit.MILLISECONDS));

                assertEquals(res, cache0.get(1));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(sem.tryAcquire(getTestTimeout(), TimeUnit.MILLISECONDS));

        if (directOrder)
            stopGrid(1);

        MvccProcessorImpl prc = mvccProcessor(startGrid(2));

        if (!directOrder)
            stopGrid(1);

        awaitPartitionMapExchange();

        MvccCoordinator crd = prc.currentCoordinator();

        assert crd.local() && crd.initialized();

        cache.put(1,2);
        cache.put(1,3);

        sem.release(2);

        future.get(getTestTimeout());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartLastServerFails() throws Exception {
        testSpi = true;

        startGrids(3);

        CacheConfiguration<Object, Object> cfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        cfg.setNodeFilter(new TestNodeFilter(getTestIgniteInstanceName(1)));

        Ignite srv1 = ignite(1);

        srv1.createCache(cfg);

        client = true;

        final Ignite c = startGrid(3);

        client = false;

        TestRecordingCommunicationSpi.spi(srv1).blockMessages(GridDhtAffinityAssignmentResponse.class, c.name());

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                c.cache(DEFAULT_CACHE_NAME);

                return null;
            }
        }, "start-cache");

        U.sleep(1000);

        assertFalse(fut.isDone());

        stopGrid(1);

        fut.get();

        final IgniteCache<Object, Object> clientCache = c.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10; i++) {
            final int k = i;

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.get(k);

                    return null;
                }
            }, CacheServerNotFoundException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.put(k, k);

                    return null;
                }
            }, CacheServerNotFoundException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.remove(k);

                    return null;
                }
            }, CacheServerNotFoundException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.query(new SqlFieldsQuery("SELECT * FROM INTEGER")).getAll();

                    return null;
                }
            }, CacheServerNotFoundException.class, "Failed to find data nodes for cache");

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.query(new SqlFieldsQuery("SELECT * FROM INTEGER ORDER BY _val")).getAll();

                    return null;
                }
            }, CacheServerNotFoundException.class, "Failed to find data nodes for cache");

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.query(new SqlFieldsQuery("DELETE FROM Integer WHERE 1 = 1")).getAll();

                    return null;
                }
            }, CacheServerNotFoundException.class, "Failed to find data nodes for cache");

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.query(new SqlFieldsQuery("INSERT INTO Integer (_key, _val) VALUES (1, 2)")).getAll();

                    return null;
                }
            }, CacheServerNotFoundException.class, "Failed to get primary node");

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    clientCache.query(new SqlFieldsQuery("UPDATE Integer SET _val=42 WHERE _key IN (SELECT DISTINCT _val FROM INTEGER)")).getAll();

                    return null;
                }
            }, CacheServerNotFoundException.class, "Failed to find data nodes for cache");
        }

        startGrid(1);

        awaitPartitionMapExchange();

        for (int i = 0; i < 100; i++) {
            assertNull(clientCache.get(i));

            clientCache.put(i, i);

            assertEquals(i, clientCache.get(i));
        }
    }

    /**
     *
     */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private final String includeName;

        /**
         * @param includeName Node to include.
         */
        public TestNodeFilter(String includeName) {
            this.includeName = includeName;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return includeName.equals(node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME));
        }
    }
}
