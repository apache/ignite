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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.Test;

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

/**
 * Tests thin client performance statistics.
 */
public class PerformanceStatisticsThinClientTest extends AbstractPerformanceStatisticsTest {
    /** Test task name. */
    public static final String TEST_TASK_NAME = "TestTask";

    /** Active tasks limit. */
    private static final int ACTIVE_TASKS_LIMIT = 50;

    /** Grids count. */
    private static final int GRIDS_CNT = 2;

    /** Thin client. */
    private static IgniteClient thinClient;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

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

        thinClient.close();
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
    }

    /**
     * Cache {@link TcpClientCache#putAllConflict} operation performed.
     * @throws Exception If failed.
     */
    @Test
    public void testCachePutAllConflict() throws Exception {
        checkCacheAllConflictOperations(CACHE_PUT_ALL_CONFLICT, false);
    }

    /**
     * Cache {@link TcpClientCache#removeAllConflict} operation performed.
     * @throws Exception If failed.
     */
    @Test
    public void testCacheRemoveAllConflict() throws Exception {
        checkCacheAllConflictOperations(CACHE_REMOVE_ALL_CONFLICT, false);
    }

    /**
     * Cache {@link TcpClientCache#putAllConflictAsync} operation performed.
     * @throws Exception If failed.
     */
    @Test
    public void testCachePutAllConflictAsync() throws Exception {
        checkCacheAllConflictOperations(CACHE_PUT_ALL_CONFLICT, true);
    }

    /**
     * Cache {@link TcpClientCache#removeAllConflictAsync} operation performed.
     * @throws Exception If failed.
     */
    @Test
    public void testCacheRemoveAllConflictAsync() throws Exception {
        checkCacheAllConflictOperations(CACHE_REMOVE_ALL_CONFLICT, true);
    }

    /**
     * @param opType {@link OperationType} cache operation type.
     * @param isAsync boolean flag for asynchronous cache operation processing.
     */
    private void checkCacheAllConflictOperations(OperationType opType, boolean isAsync) throws Exception {
        checkCacheOperation(opType, cache -> {
            try {
                if (opType == CACHE_PUT_ALL_CONFLICT && !isAsync)
                    ((TcpClientCache<Object, Object>)cache).putAllConflict(getPutAllConflictMap(6, 1));
                else if (opType == CACHE_REMOVE_ALL_CONFLICT && !isAsync)
                    ((TcpClientCache<Object, Object>)cache).removeAllConflict(getRemoveAllConflictMap(6));
                else if (opType == CACHE_PUT_ALL_CONFLICT)
                    ((TcpClientCache<Object, Object>)cache).putAllConflictAsync(getPutAllConflictMap(7, 2)).get();
                else if (opType == CACHE_REMOVE_ALL_CONFLICT)
                    ((TcpClientCache<Object, Object>)cache).removeAllConflictAsync(getRemoveAllConflictMap(7)).get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * @param key Key
     * @param val Value
     * @return {@link Map} with data to send with {@link TcpClientCache#putAllConflict(Map)} or
     * {@link TcpClientCache#putAllConflictAsync(Map)} cache operations.
     */
    private Map<?, T3<?, GridCacheVersion, Long>> getPutAllConflictMap(int key, int val) {
        GridCacheVersion confl = new GridCacheVersion(1, 0, 1, (byte)2);

        return F.asMap(key, new T3<>(val, confl, CU.EXPIRE_TIME_ETERNAL));
    }

    /**
     * @param key Key
     * @return {@link Map} with keys to remove with {@link TcpClientCache#removeAllConflict(Map)} or
     * {@link TcpClientCache#removeAllConflict(Map)} cache operations.
     */
    private Map<?, GridCacheVersion> getRemoveAllConflictMap(int key) {
        GridCacheVersion confl = new GridCacheVersion(1, 0, 1, (byte)2);

        return F.asMap(key, confl);
    }

    /** Checks cache operation. */
    private void checkCacheOperation(OperationType op, Consumer<ClientCache<Object, Object>> clo) throws Exception {
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
