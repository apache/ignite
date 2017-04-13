package org.apache.ignite.internal.processors.cache.store;

import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.GridCacheTestStore;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sbelyak on 12.04.17.
 */
public class GridCacheWriteBehindStorePerfTest extends GridCacheWriteBehindStoreAbstractSelfTest {

    final AtomicBoolean running = new AtomicBoolean(true);

    protected CacheStore delegate = new CacheStore(){
        @Override
        public void write(Cache.Entry entry) throws CacheWriterException {

        }

        @Override
        public void delete(Object key) throws CacheWriterException {

        }

        @Override
        public void deleteAll(Collection keys) throws CacheWriterException {

        }

        @Override
        public void writeAll(Collection collection) throws CacheWriterException {

        }

        @Override
        public Object load(Object key) throws CacheLoaderException {
            return null;
        }

        @Override
        public Map loadAll(Iterable keys) throws CacheLoaderException {
            return null;
        }

        @Override
        public void loadCache(IgniteBiInClosure clo, @Nullable Object... args) throws CacheLoaderException {

        }

        @Override
        public void sessionEnd(boolean commit) throws CacheWriterException {

        }
    };

    /**
     * Performs multiple put, get and remove operations in several threads on a store. After
     * all threads finished their operations, returns the total set of keys that should be
     * in underlying store.
     *
     * @param threadCnt Count of threads that should update keys.
     * @param keysPerThread Count of unique keys assigned to a thread.
     * @return Set of keys that was totally put in store.
     * @throws Exception If failed.
     */
    protected Set<Integer> runPutGetRemoveMultithreaded(int threadCnt, final int keysPerThread) throws Exception {
        final ConcurrentMap<String, Set<Integer>> perThread = new ConcurrentHashMap<>();



        final AtomicInteger cntr = new AtomicInteger();

        final AtomicInteger operations = new AtomicInteger();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @SuppressWarnings({"NullableProblems"})
            @Override public void run() {
                // Initialize key set for this thread.
                Set<Integer> set = new HashSet<>();

                Set<Integer> old = perThread.putIfAbsent(Thread.currentThread().getName(), set);

                if (old != null)
                    set = old;

                List<Integer> original = new ArrayList<>();

                Random rnd = new Random();

                for (int i = 0; i < keysPerThread; i++)
                    //original.add(cntr.getAndIncrement());
                    original.add(rnd.nextInt());
                int i=0;
                int BATCH_SIZE = 1000;
                try {
                    while (running.get()) {
                        while (i < BATCH_SIZE) {
                            int op = rnd.nextInt(3);
                            int idx = rnd.nextInt(keysPerThread);

                            int key = original.get(idx);

                            switch (op) {
                                case 0:
                                    store.write(new CacheEntryImpl<>(key, "val" + key));
                                    set.add(key);
                                    i++;
                                    break;

                                case 1:
                                    store.delete(key);
                                    set.remove(key);
                                    i++;
                                    break;

                                case 2:
                                default:
                                    store.write(new CacheEntryImpl<>(key, "broken"));

                                    String val = store.load(key);

                                    //assertEquals("Invalid intermediate value: " + val, "broken", val);

                                    store.write(new CacheEntryImpl<>(key, "val" + key));

                                    set.add(key);

                                    // 2 put operations performed here.
                                    i+=3;
                                    break;
                            }
                        }
                        operations.getAndAdd(i);
                        i=0;
                    }
                }
                catch (Exception e) {
                    error("Unexpected exception in put thread", e);

                    assert false;
                }
            }
        }, threadCnt, "put");

        long startTime;
        int warmUp = 60;
        long processedInMinute = 0;
        while(true) {
            U.sleep(1000);

            int processed = operations.getAndSet(0);
            processedInMinute += processed;
            warmUp--;
            if (warmUp == 0) {
                startTime = new Date().getTime();
                System.out.println("Processed while warmup:" + processedInMinute + " av:" + processedInMinute/60);
                processedInMinute = 0;
            }
            if (warmUp == -60) {
                warmUp = 0;
                System.out.println("Processed in minute:" + processedInMinute + " av:" + processedInMinute/60);
                processedInMinute = 0;
            }


            System.out.println("Processed: " + processed + " time: " + new Date());
        }


    }

    private void testPutGetRemoveWithCoalescing() throws Exception {
        testPutGetRemove(true);
    }

    public void testPutGetRemoveWithoutCoalescing() throws Exception {
        testPutGetRemove(false);
    }

    /**
     * This test performs complex set of operations on store from multiple threads.
     *
     * @throws Exception If failed.
     */
    private void testPutGetRemove(boolean writeCoalescing) throws Exception {
        initStore(2, writeCoalescing);

        Set<Integer> exp;

        try {
            exp = runPutGetRemoveMultithreaded(4, 100000);
        }
        finally {
            shutdownStore();
        }
    }


}
