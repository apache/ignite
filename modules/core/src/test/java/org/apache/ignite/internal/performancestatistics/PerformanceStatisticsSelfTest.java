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
import org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType.GET;
import static org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType.GET_ALL;
import static org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType.INVOKE;
import static org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType.INVOKE_ALL;
import static org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType.LOCK;
import static org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType.PUT;
import static org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType.PUT_ALL;
import static org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType.REMOVE;
import static org.apache.ignite.internal.performancestatistics.IgnitePerformanceStatistics.CacheOperationType.REMOVE_ALL;
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

    /** Log to register performance statistics. */
    private static ListeningTestLogger log;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        cfg.setGridLogger(log);

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        log.clearListeners();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCompute() throws Exception {
        LogListener taskLsnr = matches("task ").andMatches("taskName=" + getClass().getName()).times(1).build();
        LogListener jobLsnr = matches("job ").times(1).build();

        log.registerListener(taskLsnr);
        log.registerListener(jobLsnr);

        startStatistics();

        ignite.compute().broadcast(new IgniteRunnable() {
            @Override public void run() {
                // No-op.
            }
        });

        stopStatisticsAndCheck(taskLsnr, jobLsnr);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheOperations() throws Exception {
        checkCacheOperation(PUT, cache -> cache.put(1, 1));
        checkCacheOperation(PUT, cache -> cache.putAsync(2, 2).get());

        checkCacheOperation(PUT_ALL, cache -> cache.putAll(Collections.singletonMap(3, 3)));
        checkCacheOperation(PUT_ALL, cache -> cache.putAllAsync(Collections.singletonMap(4, 4)).get());

        checkCacheOperation(GET, cache -> cache.get(1));
        checkCacheOperation(GET, cache -> cache.getAsync(2).get());

        checkCacheOperation(GET_ALL, cache -> cache.getAll(Collections.singleton(1)));
        checkCacheOperation(GET_ALL, cache -> cache.getAllAsync(Collections.singleton(2)).get());

        checkCacheOperation(REMOVE, cache -> cache.remove(1));
        checkCacheOperation(REMOVE, cache -> cache.removeAsync(2).get());

        checkCacheOperation(REMOVE_ALL, cache -> cache.removeAll(Collections.singleton(3)));
        checkCacheOperation(REMOVE_ALL, cache -> cache.removeAllAsync(Collections.singleton(4)).get());

        checkCacheOperation(LOCK, cache -> {
            Lock lock = cache.lock(5);

            lock.lock();
            lock.unlock();
        });

        checkCacheOperation(LOCK, cache -> {
            Lock lock = cache.lockAll(Collections.singleton(5));

            lock.lock();
            lock.unlock();
        });

        checkCacheOperation(INVOKE, cache -> cache.invoke(10, ENTRY_PROC));
        checkCacheOperation(INVOKE, cache -> cache.invokeAsync(10, ENTRY_PROC).get());

        checkCacheOperation(INVOKE, cache -> cache.invoke(10, CACHE_ENTRY_PROC));
        checkCacheOperation(INVOKE, cache -> cache.invokeAsync(10, CACHE_ENTRY_PROC).get());

        checkCacheOperation(INVOKE_ALL, cache -> cache.invokeAll(Collections.singleton(10), ENTRY_PROC));
        checkCacheOperation(INVOKE_ALL, cache -> cache.invokeAllAsync(Collections.singleton(10), ENTRY_PROC).get());

        checkCacheOperation(INVOKE_ALL, cache -> cache.invokeAll(Collections.singleton(10), CACHE_ENTRY_PROC));
        checkCacheOperation(INVOKE_ALL,
            cache -> cache.invokeAllAsync(Collections.singleton(10), CACHE_ENTRY_PROC).get());
    }

    /** */
    private void checkCacheOperation(CacheOperationType op, Consumer<IgniteCache<Object, Object>> clo) throws Exception {
        LogListener lsnr = matches("cacheOperation ")
            .andMatches("type=" + op)
            .andMatches("cacheId=" + ignite.context().cache().cache(DEFAULT_CACHE_NAME).context().cacheId())
            .build();

        log.registerListener(lsnr);

        startStatistics();

        clo.accept(ignite.cache(DEFAULT_CACHE_NAME));

        stopStatisticsAndCheck(lsnr);
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
            .andMatches("commit=" + commit).build();

        log.registerListener(lsnr);

        startStatistics();

        try (Transaction tx = ignite.transactions().txStart()) {
            for (int i = 0; i < 10; i++)
                cache.put(i, i * 2);

            if (commit)
                tx.commit();
            else
                tx.rollback();
        }

        stopStatisticsAndCheck(lsnr);
    }
}
