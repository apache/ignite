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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.thread.*;
import org.springframework.beans.factory.*;
import org.springframework.context.support.*;

import java.util.concurrent.*;

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
            final IgniteCache<GridTestKey, Long> cache = g.jcache("partitioned");

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

        final IgniteCache<GridTestKey, Long> cache = g.jcache("partitioned");

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

        final IgniteCache<GridTestKey, Long> cache = G.ignite().jcache("partitioned");

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
