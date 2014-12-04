/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.colocation;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dataload.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.typedef.*;
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
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws Exception {
        BeanFactory ctx = new ClassPathXmlApplicationContext("org/gridgain/loadtests/colocation/spring-colocation.xml");

        // Initialize Spring factory.
        try (Ignite g = G.start((IgniteConfiguration)ctx.getBean("grid.cfg"))) {
            final GridCache<GridTestKey, Long> cache = g.cache("partitioned");

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

        Ignite g = G.grid();

        final GridCache<GridTestKey, Long> cache = g.cache("partitioned");

        final BlockingQueue<IgniteFuture> q = new ArrayBlockingQueue<>(400);

        long start = System.currentTimeMillis();

        IgniteCompute comp = g.compute().enableAsync();

        // Collocate computations and data.
        for (long i = 0; i < GridTestConstants.ENTRY_COUNT; i++) {
            final long key = i;

            comp.affinityRun("partitioned", GridTestKey.affinityKey(key), new Runnable() {
                // This code will execute on remote nodes by collocating keys with cached data.
                @Override public void run() {
                    Long val = cache.peek(new GridTestKey(key));

                    if (val == null || val != key)
                        throw new RuntimeException("Invalid value found [key=" + key + ", val=" + val + ']');
                }
            });

            final IgniteFuture<?> f = comp.future();

            q.put(f);

            f.listenAsync(new CI1<IgniteFuture<?>>() {
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

        ExecutorService exe = new GridThreadPoolExecutor(400, 400, 0, new ArrayBlockingQueue<Runnable>(400) {
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

        final GridCache<GridTestKey, Long> cache = G.grid().cache("partitioned");

        // Collocate computations and data.
        for (long i = 0; i < GridTestConstants.ENTRY_COUNT; i++) {
            final long key = i;

            exe.submit(new Runnable() {
                @Override public void run() {
                    Long val = cache.peek(new GridTestKey(key));

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
     * @throws GridException If failed.
     */
    private static void loadFromStore(GridCache<GridTestKey, Long> cache) throws GridException {
        cache.loadCache(null, 0, GridTestConstants.LOAD_THREADS, GridTestConstants.ENTRY_COUNT);
    }

    /**
     * Generates and loads data directly through cache API using data loader.
     * This method is provided as example and is not called directly because
     * data is loaded through {@link GridTestCacheStore} store.
     *
     * @throws Exception If failed.
     */
    private static void generateAndLoad() throws Exception {
        int numThreads = Runtime.getRuntime().availableProcessors() * 2;

        ExecutorCompletionService<Object> execSvc =
            new ExecutorCompletionService<>(Executors.newFixedThreadPool(numThreads));

        try (IgniteDataLoader<GridTestKey, Long> ldr = G.grid().dataLoader("partitioned")) {
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
