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

package org.apache.ignite.loadtests.colocation;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Accenture collocated example.
 */
public class GridTestMain {
    /**
     * Main method.
     *
     * @param args Parameters.
     * @throws IgniteCheckedException If failed.
     */
    public static void main(String[] args) throws Exception {
        BeanFactory ctx = new ClassPathXmlApplicationContext("org/apache/ignite/loadtests/colocation/spring-colocation.xml");

        // Initialize Spring factory.
        try (Ignite g = G.start((IgniteConfiguration)ctx.getBean("grid.cfg"))) {
            final IgniteCache<GridTestKey, Long> cache = g.cache("partitioned");

            assert cache != null;

            // Uncomment if you plan to load cache using AccentureCacheStore.
            // generateAndLoad();

            // Uncomment if you plan to load cache from cache store.
            // Note that you could also do this automatically from lifecycle bean.
            // To configure lifecycle bean, uncomment 'lifecycleBeans' property in
            // spring-accenture.xml file.
            loadFromStore(cache);

            X.println("Number of entries in cache: " + cache.size());

            colocateJobs();
            //localPoolRun();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private static void colocateJobs() throws Exception {
        X.println("Collocating jobs...");

        Ignite g = G.ignite();

        final IgniteCache<GridTestKey, Long> cache = g.cache("partitioned");

        final BlockingQueue<IgniteFuture> q = new ArrayBlockingQueue<>(400);

        long start = System.currentTimeMillis();

        IgniteCompute comp = g.compute().withAsync();

        // Collocate computations and data.
        for (long i = 0; i < GridTestConstants.ENTRY_COUNT; i++) {
            final long key = i;

            comp.affinityRun("partitioned", GridTestKey.affinityKey(key), new IgniteRunnable() {
                // This code will execute on remote nodes by collocating keys with cached data.
                @Override public void run() {
                    Long val = cache.localPeek(new GridTestKey(key), CachePeekMode.ONHEAP);

                    if (val == null || val != key)
                        throw new RuntimeException("Invalid value found [key=" + key + ", val=" + val + ']');
                }
            });

            final IgniteFuture<?> f = comp.future();

            q.put(f);

            f.listen(new CI1<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> o) {
                    q.poll();
                }
            });

            if (i % 10000 == 0)
                X.println("Executed jobs: " + i);
        }

        long end = System.currentTimeMillis();

        X.println("Executed " + GridTestConstants.ENTRY_COUNT + " computations in " + (end - start) + "ms.");
    }

    /**
     *
     */
    private static void localPoolRun() {
        X.println("Local thread pool run...");

        ExecutorService exe = new IgniteThreadPoolExecutor(400, 400, 0, new ArrayBlockingQueue<Runnable>(400) {
            @Override public boolean offer(Runnable runnable) {
                try {
                    put(runnable);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return true;
            }
        });

        long start = System.currentTimeMillis();

        final IgniteCache<GridTestKey, Long> cache = G.ignite().cache("partitioned");

        // Collocate computations and data.
        for (long i = 0; i < GridTestConstants.ENTRY_COUNT; i++) {
            final long key = i;

            exe.submit(new Runnable() {
                @Override public void run() {
                    Long val = cache.localPeek(new GridTestKey(key), CachePeekMode.ONHEAP);

                    if (val == null || val != key)
                        throw new RuntimeException("Invalid value found [key=" + key + ", val=" + val + ']');
                }
            });

            if (i % 10000 == 0)
                X.println("Executed jobs: " + i);
        }

        long end = System.currentTimeMillis();

        X.println("Executed " + GridTestConstants.ENTRY_COUNT + " computations in " + (end - start) + "ms.");
    }

    /**
     * Load cache from data store. Also take a look at
     * {@link GridTestCacheStore#loadAll} method.
     *
     * @param cache Cache to load.
     */
    private static void loadFromStore(IgniteCache<GridTestKey, Long> cache) {
        cache.loadCache(null, 0, GridTestConstants.LOAD_THREADS, GridTestConstants.ENTRY_COUNT);
    }

    /**
     * Generates and loads data directly through cache API using data streamer.
     * This method is provided as example and is not called directly because
     * data is loaded through {@link GridTestCacheStore} store.
     *
     * @throws Exception If failed.
     */
    private static void generateAndLoad() throws Exception {
        int numThreads = Runtime.getRuntime().availableProcessors() * 2;

        ExecutorCompletionService<Object> execSvc =
            new ExecutorCompletionService<>(Executors.newFixedThreadPool(numThreads));

        try (IgniteDataStreamer<GridTestKey, Long> ldr = G.ignite().dataStreamer("partitioned")) {
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;

                final int perThreadKeys = GridTestConstants.ENTRY_COUNT / numThreads;

                execSvc.submit(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        int start = threadId * perThreadKeys;
                        int end = start + perThreadKeys;

                        for (long i = start; i < end; i++)
                            ldr.addData(new GridTestKey(i), i);

                        return null;
                    }
                });
            }

            for (int i = 0; i < numThreads; i++)
                execSvc.take().get();
        }
    }
}