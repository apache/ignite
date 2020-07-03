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

package org.apache.ignite.internal.performancestatistics;

import java.util.Collections;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_GET_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_INVOKE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_INVOKE_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_LOCK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_PUT_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_REMOVE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_REMOVE_ALL;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 * Tests performance statistics.
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
public class PerformanceStatisticsSelfTest extends AbstractPerformanceStatisticsTest {
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

    /** Ignite. */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        log = new ListeningTestLogger(GridAbstractTest.log);

        ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCompute() throws Exception {
        String taskName = "testTask";
        int executions = 5;

        LogListener taskLsnr = matches("task ").andMatches("taskName=" + taskName).times(executions).build();
        LogListener jobLsnr = matches("job ").times(executions).build();

        startCollectStatistics();

        IgniteRunnable task = new IgniteRunnable() {
            @Override public void run() {
                // No-op.
            }
        };

        for (int i = 0; i < executions; i++)
            ignite.compute().withName(taskName).run(task);

        stopCollectStatisticsAndCheck(taskLsnr, jobLsnr);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheOperations() throws Exception {
        checkCacheOperation(CACHE_PUT, cache -> cache.put(1, 1));
        checkCacheOperation(CACHE_PUT, cache -> cache.putAsync(2, 2).get());

        checkCacheOperation(CACHE_PUT_ALL, cache -> cache.putAll(Collections.singletonMap(3, 3)));
        checkCacheOperation(CACHE_PUT_ALL, cache -> cache.putAllAsync(Collections.singletonMap(4, 4)).get());

        checkCacheOperation(CACHE_GET, cache -> cache.get(1));
        checkCacheOperation(CACHE_GET, cache -> cache.getAsync(2).get());

        checkCacheOperation(CACHE_GET_ALL, cache -> cache.getAll(Collections.singleton(1)));
        checkCacheOperation(CACHE_GET_ALL, cache -> cache.getAllAsync(Collections.singleton(2)).get());

        checkCacheOperation(CACHE_REMOVE, cache -> cache.remove(1));
        checkCacheOperation(CACHE_REMOVE, cache -> cache.removeAsync(2).get());

        checkCacheOperation(CACHE_REMOVE_ALL, cache -> cache.removeAll(Collections.singleton(3)));
        checkCacheOperation(CACHE_REMOVE_ALL, cache -> cache.removeAllAsync(Collections.singleton(4)).get());

        checkCacheOperation(CACHE_LOCK, cache -> {
            Lock lock = cache.lock(5);

            lock.lock();
            lock.unlock();
        });

        checkCacheOperation(CACHE_LOCK, cache -> {
            Lock lock = cache.lockAll(Collections.singleton(5));

            lock.lock();
            lock.unlock();
        });

        checkCacheOperation(CACHE_INVOKE, cache -> cache.invoke(10, ENTRY_PROC));
        checkCacheOperation(CACHE_INVOKE, cache -> cache.invokeAsync(10, ENTRY_PROC).get());

        checkCacheOperation(CACHE_INVOKE, cache -> cache.invoke(10, CACHE_ENTRY_PROC));
        checkCacheOperation(CACHE_INVOKE, cache -> cache.invokeAsync(10, CACHE_ENTRY_PROC).get());

        checkCacheOperation(CACHE_INVOKE_ALL, cache -> cache.invokeAll(Collections.singleton(10), ENTRY_PROC));
        checkCacheOperation(CACHE_INVOKE_ALL, cache -> cache.invokeAllAsync(Collections.singleton(10), ENTRY_PROC).get());

        checkCacheOperation(CACHE_INVOKE_ALL, cache -> cache.invokeAll(Collections.singleton(10), CACHE_ENTRY_PROC));
        checkCacheOperation(CACHE_INVOKE_ALL,
            cache -> cache.invokeAllAsync(Collections.singleton(10), CACHE_ENTRY_PROC).get());
    }

    /** */
    private void checkCacheOperation(OperationType op, Consumer<IgniteCache<Object, Object>> clo) throws Exception {
        LogListener lsnr = matches("cacheOperation ")
            .andMatches("type=" + op)
            .andMatches("cacheId=" + ignite.context().cache().cache(DEFAULT_CACHE_NAME).context().cacheId())
            .build();

        startCollectStatistics();

        clo.accept(ignite.cache(DEFAULT_CACHE_NAME));

        stopCollectStatisticsAndCheck(lsnr);
    }

    /** @throws Exception If failed. */
    @Test
    public void testTransactions() throws Exception {
        checkTx(true);

        checkTx(false);
    }

    /** @param commit {@code True} if check transaction commit. */
    private void checkTx(boolean commit) throws Exception {
        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        LogListener lsnr = matches("transaction ")
            .andMatches("cacheIds=[" + ignite.context().cache().cache(DEFAULT_CACHE_NAME).context().cacheId() + ']')
            .andMatches("commited=" + commit).build();

        startCollectStatistics();

        try (Transaction tx = ignite.transactions().txStart()) {
            for (int i = 0; i < 10; i++)
                cache.put(i, i * 2);

            if (commit)
                tx.commit();
            else
                tx.rollback();
        }

        stopCollectStatisticsAndCheck(lsnr);
    }
}
