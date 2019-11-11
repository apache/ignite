/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridDebug;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Test for OOM if case of using large entries. This OOM may be caused by inaccurate results caching on
 * some stage of the query execution.
 */
public class IgniteCacheQueryLargeRecordsOomTest extends GridCommonAbstractTest {
    /** */
    private static final int READERS = 2;

    /** */
    private static final int ITERATIONS = 10;

    /** Heap dump file name. */
    private static final String HEAP_DUMP_FILE_NAME = "test.hprof";

    /** Allowed leak size in MB. */
    private static final float MAX_LEAK = 30;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMemoryLeakDistributed() throws Exception {
        checkMemoryLeak(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMemoryLeakDistributedLazy() throws Exception {
        checkMemoryLeak(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMemoryLeakLocal() throws Exception {
        checkMemoryLeak(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMemoryLeakLocalLazy() throws Exception {
        checkMemoryLeak(true, true);
    }

    /**
     * @param loc Local.
     * @param lazy Lazy.
     * @throws Exception If failed.
     */
    private void checkMemoryLeak(boolean loc, boolean lazy) throws Exception {
        startGridsMultiThreaded(2);

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (long i = 0; i < 1000; i++) {
            Person val = new Person(new byte[1024 * 1024]); // 1 MB entry.

            cache.put(i, val);
        }

        if (log.isInfoEnabled())
            log.info("Data loaded");

        QueriesRunner runner = new QueriesRunner(cache, READERS, getTestTimeout());

        // Warm up run.
        runner.runQueries(3, loc, lazy);

        GridDebug.dumpHeap(HEAP_DUMP_FILE_NAME, true);

        File dumpFile = new File(HEAP_DUMP_FILE_NAME);

        final long size0 = dumpFile.length();

        runner.runQueries(ITERATIONS, loc, lazy);

        GridDebug.dumpHeap(HEAP_DUMP_FILE_NAME, true);

        final float leakSize = (float)(dumpFile.length() - size0) / 1024 / 1024;

        if (log.isInfoEnabled())
            log.info("Current leak size=" + leakSize + "MB, heap size after warm up=" + (size0 / 1024 / 1024) + "MB.");

        assertTrue("The memory leak detected : " + leakSize + "MB. See heap dump '" + dumpFile.getAbsolutePath() + "'",
            leakSize < MAX_LEAK);

        // Remove dump if successful.
        dumpFile.delete();

        runner.shutdown();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Long.class, Person.class));
        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMetricsEnabled(true)));

        TcpCommunicationSpi communicationSpi = (TcpCommunicationSpi)cfg.getCommunicationSpi();
        communicationSpi.setAckSendThreshold(1);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 120 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** */
    private static class Person {
        /** */
        byte[] b;

        /** */
        Person(byte[] b) {
            this.b = b;
        }
    }

    /**
     * Query runner. We need to run queries using the same threads (due to thread local statements caching),
     * so let's encapsulate this logic in the dedicated class.
     */
    private static class QueriesRunner {
        /** */
        private final ExecutorService exec;

        /** */
        private final int runners;

        /** */
        private final long timeout;

        /** */
        private final IgniteCache<Object, Object> cache;

        /**
         * @param cache Cache.
         * @param runners Runners number.
         * @param timeout Timeout.
         */
        private QueriesRunner(IgniteCache<Object, Object> cache, int runners, long timeout) {
            this.cache = cache;
            this.timeout = timeout;
            this.exec = Executors.newFixedThreadPool(runners, new ThreadFactory() {
                @Override public Thread newThread(@NotNull Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("test-async-query-runner-" + t.hashCode());
                    return t;
                }
            });
            this.runners = runners;
        }

        /**
         * Runs queries.
         * @throws Exception If failed.
         */
        void runQueries(final int iterations, boolean loc, boolean lazy) throws Exception {
            List<Future> futs = new ArrayList<>(runners);

            for (int i = 0; i < runners; i++) {
                Future f = exec.submit(new Callable<Object>() {
                    @Override public Object call() {
                            for (int j = 0; j < iterations; j++) {
                                if (Thread.currentThread().isInterrupted())
                                    return null;

                                if (log.isInfoEnabled())
                                    log.info("Iteration " + j);

                                String sql = "select * from Person limit " + (j + 1) + (j % 2 == 0 ? "" : " offset 1");

                                FieldsQueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(sql)
                                        .setLazy(lazy)
                                        .setLocal(loc));

                                qry.getAll();
                                qry.close();
                            }

                        return null;
                    }
                });

                futs.add(f);
            }

            for (Future f : futs)
                f.get();
        }

        /**
         * Terminates runner.
         * @throws InterruptedException If failed.
         */
        public void shutdown() throws InterruptedException {
            exec.shutdownNow();
            exec.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        }
    }
}
