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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.ClientType.CLIENT;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.ClientType.SERVER;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET_AND_PUT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET_AND_REMOVE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_INVOKE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_INVOKE_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_LOCK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_REMOVE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_REMOVE_ALL;

/**
 * Tests performance statistics.
 */
@RunWith(Parameterized.class)
@SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
public class PerformanceStatisticsSelfTest extends AbstractPerformanceStatisticsTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Cache entry count. */
    private static final int ENTRY_COUNT = 100;

    /** Test entry processor. */
    private static final EntryProcessor<Object, Object, Object> ENTRY_PROC =
        new EntryProcessor<Object, Object, Object>() {
            @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments)
                throws EntryProcessorException {
                return null;
            }
        };

    /** Test cache entry processor. */
    private static final CacheEntryProcessor<Object, Object, Object> CACHE_ENTRY_PROC =
        new CacheEntryProcessor<Object, Object, Object>() {
            @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments)
                throws EntryProcessorException {
                return null;
            }
        };

    /** Client type to run operations from. */
    @Parameterized.Parameter
    public ClientType clientType;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "clientType={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{SERVER}, {CLIENT}});
    }

    /** Ignite. */
    private static IgniteEx srv;

    /** Ignite node to run load from. */
    private static IgniteEx node;

    /** Test cache. */
    private static IgniteCache<Object, Object> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        srv = startGrid(NODES_CNT - 1);

        IgniteEx client = startClientGrid(NODES_CNT);

        node = clientType == SERVER ? srv : client;

        cache = node.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ENTRY_COUNT; i++)
            cache.put(i, i);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCompute() throws Exception {
        String testTaskName = "testTask";
        int executions = 5;
        long startTime = U.currentTimeMillis();

        startCollectStatistics();

        IgniteRunnable task = new IgniteRunnable() {
            @Override public void run() {
                // No-op.
            }
        };

        for (int i = 0; i < executions; i++)
            node.compute().withName(testTaskName).run(task);

        HashMap<IgniteUuid, Integer> sessions = new HashMap<>();
        AtomicInteger tasks = new AtomicInteger();
        AtomicInteger jobs = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long taskStartTime,
                long duration, int affPartId) {
                sessions.compute(sesId, (uuid, cnt) -> cnt == null ? 1 : ++cnt);

                tasks.incrementAndGet();

                assertEquals(node.context().localNodeId(), nodeId);
                assertEquals(testTaskName, taskName);
                assertTrue(taskStartTime >= startTime);
                assertTrue(duration >= 0);
                assertEquals(-1, affPartId);
            }

            @Override public void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long jobStartTime, long duration,
                boolean timedOut) {
                sessions.compute(sesId, (uuid, cnt) -> cnt == null ? 1 : ++cnt);

                jobs.incrementAndGet();

                assertEquals(srv.context().localNodeId(), nodeId);
                assertTrue(queuedTime >= 0);
                assertTrue(jobStartTime >= startTime);
                assertTrue(duration >= 0);
                assertFalse(timedOut);
            }
        });

        assertEquals(executions, tasks.get());
        assertEquals(executions, jobs.get());

        Collection<Integer> vals = sessions.values();

        assertEquals(executions, vals.size());
        assertTrue("Invalid sessions: " + sessions, vals.stream().allMatch(cnt -> cnt == NODES_CNT));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheOperation() throws Exception {
        checkCacheOperation(CACHE_PUT, cache -> cache.put(1, 1));
        checkCacheOperation(CACHE_PUT, cache -> cache.putAsync(2, 2).get());

        checkCacheOperation(CACHE_PUT_ALL, cache -> cache.putAll(Collections.singletonMap(3, 3)));
        checkCacheOperation(CACHE_PUT_ALL, cache -> cache.putAllAsync(Collections.singletonMap(4, 4)).get());

        checkCacheOperation(CACHE_GET, cache -> cache.get(1));
        checkCacheOperation(CACHE_GET, cache -> cache.getAsync(2).get());

        checkCacheOperation(CACHE_GET_AND_PUT, cache -> cache.getAndPut(1, 1));
        checkCacheOperation(CACHE_GET_AND_PUT, cache -> cache.getAndPutAsync(2, 2).get());

        checkCacheOperation(CACHE_GET_ALL, cache -> cache.getAll(Collections.singleton(1)));
        checkCacheOperation(CACHE_GET_ALL, cache -> cache.getAllAsync(Collections.singleton(2)).get());

        checkCacheOperation(CACHE_GET_ALL, cache -> cache.getAllOutTx(Collections.singleton(1)));
        checkCacheOperation(CACHE_GET_ALL, cache -> cache.getAllOutTxAsync(Collections.singleton(2)).get());

        checkCacheOperation(CACHE_REMOVE, cache -> cache.remove(1));
        checkCacheOperation(CACHE_REMOVE, cache -> cache.removeAsync(2).get());

        checkCacheOperation(CACHE_REMOVE_ALL, cache -> cache.removeAll(Collections.singleton(3)));
        checkCacheOperation(CACHE_REMOVE_ALL, cache -> cache.removeAllAsync(Collections.singleton(4)).get());

        checkCacheOperation(CACHE_GET_AND_REMOVE, cache -> cache.getAndRemove(5));
        checkCacheOperation(CACHE_GET_AND_REMOVE, cache -> cache.getAndRemoveAsync(6).get());

        checkCacheOperation(CACHE_LOCK, cache -> {
            Lock lock = cache.lock(7);

            lock.lock();
            lock.unlock();
        });

        checkCacheOperation(CACHE_LOCK, cache -> {
            Lock lock = cache.lockAll(Collections.singleton(8));

            lock.lock();
            lock.unlock();
        });

        checkCacheOperation(CACHE_INVOKE, cache -> cache.invoke(10, ENTRY_PROC));
        checkCacheOperation(CACHE_INVOKE, cache -> cache.invokeAsync(10, ENTRY_PROC).get());

        checkCacheOperation(CACHE_INVOKE, cache -> cache.invoke(10, CACHE_ENTRY_PROC));
        checkCacheOperation(CACHE_INVOKE, cache -> cache.invokeAsync(10, CACHE_ENTRY_PROC).get());

        checkCacheOperation(CACHE_INVOKE_ALL, cache -> cache.invokeAll(Collections.singleton(10), ENTRY_PROC));
        checkCacheOperation(CACHE_INVOKE_ALL,
            cache -> cache.invokeAllAsync(Collections.singleton(10), ENTRY_PROC).get());

        checkCacheOperation(CACHE_INVOKE_ALL, cache -> cache.invokeAll(Collections.singleton(10), CACHE_ENTRY_PROC));
        checkCacheOperation(CACHE_INVOKE_ALL,
            cache -> cache.invokeAllAsync(Collections.singleton(10), CACHE_ENTRY_PROC).get());
    }

    /** Checks cache operation. */
    private void checkCacheOperation(OperationType op, Consumer<IgniteCache<Object, Object>> clo) throws Exception {
        long startTime = U.currentTimeMillis();

        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        clo.accept(cache);

        AtomicInteger ops = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long opStartTime,
                long duration) {
                ops.incrementAndGet();

                assertEquals(node.context().localNodeId(), nodeId);
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

        try (Transaction tx = node.transactions().txStart()) {
            for (int i = 0; i < 10; i++)
                cache.put(i, i * 2);

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

                assertEquals(node.context().localNodeId(), nodeId);
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
