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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.client.thin.TestTask;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET_AND_PUT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET_AND_REMOVE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT_ALL_CONFLICT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_REMOVE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_REMOVE_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_REMOVE_ALL_CONFLICT;
import static org.junit.Assume.assumeTrue;

/**
 * Tests thin client performance statistics.
 */
@RunWith(Parameterized.class)
public class PerformanceStatisticsThinClientTest extends AbstractPerformanceStatisticsTest {
    /** Test task name. */
    public static final String TEST_TASK_NAME = "TestTask";

    /** Active tasks limit. */
    private static final int ACTIVE_TASKS_LIMIT = 50;

    /** Grids count. */
    private static final int GRIDS_CNT = 2;

    /** Thin client. */
    private static IgniteClient thinClient;

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameters(name = "atomicityMode={0}")
    public static Collection<?> parameters() {
        return EnumSet.of(ATOMIC, TRANSACTIONAL);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setThinClientConfiguration(
                new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(ACTIVE_TASKS_LIMIT)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx ignite = startGrids(GRIDS_CNT);

        ignite.compute().localDeployTask(TestTask.class, TestTask.class.getClassLoader());

        thinClient = Ignition.startClient(new ClientConfiguration()
            // Disable endpoints discovery, required connection to exact one node (node 0).
            .setAddressesFinder(() -> new String[] {Config.SERVER}));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
        thinClient.close();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        thinClient.createCache(new ClientCacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicityMode));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        thinClient.destroyCache(DEFAULT_CACHE_NAME);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCompute() throws Exception {
        int executions = 5;
        long startTime = U.currentTimeMillis();

        startCollectStatistics();

        for (int i = 0; i < executions; i++)
            thinClient.compute().execute(TEST_TASK_NAME, null);

        HashMap<IgniteUuid, Integer> sessions = new HashMap<>();
        AtomicInteger tasks = new AtomicInteger();
        AtomicInteger jobs = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long taskStartTime,
                long duration, int affPartId) {
                sessions.compute(sesId, (uuid, cnt) -> cnt == null ? 1 : ++cnt);

                tasks.incrementAndGet();

                assertTrue(F.nodeIds(grid(0).cluster().forServers().nodes()).contains(nodeId));
                assertEquals(TEST_TASK_NAME, taskName);
                assertTrue(taskStartTime >= startTime);
                assertTrue(duration >= 0);
                assertEquals(-1, affPartId);
            }

            @Override public void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long jobStartTime, long duration,
                boolean timedOut) {
                sessions.compute(sesId, (uuid, cnt) -> cnt == null ? 1 : ++cnt);

                jobs.incrementAndGet();

                assertTrue(F.nodeIds(grid(0).cluster().forServers().nodes()).contains(nodeId));
                assertTrue(queuedTime >= 0);
                assertTrue(jobStartTime >= startTime);
                assertTrue(duration >= 0);
                assertFalse(timedOut);
            }
        });

        assertEquals(executions, tasks.get());
        assertEquals(executions * GRIDS_CNT, jobs.get());

        Collection<Integer> vals = sessions.values();

        assertEquals(executions, vals.size());
        assertTrue("Invalid sessions: " + sessions, vals.stream().allMatch(val -> val == GRIDS_CNT + 1));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheOperation() throws Exception {
        checkCacheOperation(CACHE_PUT, cache -> cache.put(1, 1));

        checkCacheOperation(CACHE_PUT_ALL, cache -> cache.putAll(Collections.singletonMap(3, 3)));

        checkCacheOperation(CACHE_GET, cache -> cache.get(1));

        checkCacheOperation(CACHE_GET_AND_PUT, cache -> cache.getAndPut(1, 1));

        checkCacheOperation(CACHE_GET_ALL, cache -> cache.getAll(Collections.singleton(1)));

        checkCacheOperation(CACHE_REMOVE, cache -> cache.remove(1));

        checkCacheOperation(CACHE_REMOVE_ALL, cache -> cache.removeAll(Collections.singleton(3)));

        checkCacheOperation(CACHE_GET_AND_REMOVE, cache -> cache.getAndRemove(5));

        GridCacheVersion confl = new GridCacheVersion(1, 0, 1, (byte)2);

        checkCacheOperation(CACHE_PUT_ALL_CONFLICT, cache -> ((TcpClientCache<Object, Object>)cache)
            .putAllConflict(F.asMap(6, new T3<>(1, confl, CU.EXPIRE_TIME_ETERNAL))));

        checkCacheOperation(CACHE_REMOVE_ALL_CONFLICT, cache -> ((TcpClientCache<Object, Object>)cache)
            .removeAllConflict(F.asMap(6, confl)));

        checkCacheOperation(CACHE_PUT_ALL_CONFLICT, cache -> ((TcpClientCache<Object, Object>)cache)
            .putAllConflictAsync(F.asMap(7, new T3<>(2, confl, CU.EXPIRE_TIME_ETERNAL))).get());

        checkCacheOperation(CACHE_REMOVE_ALL_CONFLICT, cache -> ((TcpClientCache<Object, Object>)cache)
            .removeAllConflictAsync(F.asMap(7, confl)).get());
    }

    /** Checks cache operation. */
    private void checkCacheOperation(OperationType op, ConsumerX<ClientCache<Object, Object>> clo) throws Exception {
        long startTime = U.currentTimeMillis();

        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        clo.accept(thinClient.cache(DEFAULT_CACHE_NAME));

        AtomicInteger ops = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long opStartTime,
                long duration) {
                ops.incrementAndGet();

                assertTrue(F.nodeIds(grid(0).cluster().forServers().nodes()).contains(nodeId));
                assertEquals(op, type);
                assertEquals(CU.cacheId(DEFAULT_CACHE_NAME), cacheId);
                assertTrue(opStartTime >= startTime);
                assertTrue(duration >= 0);
            }
        });

        assertEquals(1, ops.get());
    }

    /** @throws Exception If failed. */
    @Test
    public void testTransaction() throws Exception {
        assumeTrue(atomicityMode == TRANSACTIONAL);

        checkTx(true);

        checkTx(false);
    }

    /** @param commited {@code True} if check transaction commited. */
    private void checkTx(boolean commited) throws Exception {
        long startTime = U.currentTimeMillis();

        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        try (ClientTransaction tx = thinClient.transactions().txStart()) {
            for (int i = 0; i < 10; i++)
                thinClient.cache(DEFAULT_CACHE_NAME).put(i, i * 2);

            if (commited)
                tx.commit();
            else
                tx.rollback();
        }

        AtomicInteger txs = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void transaction(UUID nodeId, GridIntList cacheIds, long txStartTime, long duration,
                boolean txCommited) {
                txs.incrementAndGet();

                assertTrue(F.nodeIds(grid(0).cluster().forServers().nodes()).contains(nodeId));
                assertEquals(1, cacheIds.size());
                assertEquals(CU.cacheId(DEFAULT_CACHE_NAME), cacheIds.get(0));
                assertTrue(txStartTime >= startTime);
                assertTrue(duration >= 0);
                assertEquals(commited, txCommited);
            }
        });

        assertEquals(1, txs.get());
    }
}
