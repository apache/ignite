package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The test for the destruction of the cache during the execution of the query
 */
public class GridCacheQueryAdapterTest extends GridCommonAbstractTest {

    /**
     * The main test code.
     */
    public void testQueue() throws Throwable {
        final IgniteThread t1 = new IgniteThread("1");
        final IgniteThread t2 = new IgniteThread("2");
        final AtomicReference<Throwable> expRef1 = new AtomicReference<>();
        final AtomicReference<Throwable> expRef2 = new AtomicReference<>();

        final Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable) {
                if (thread==t1)
                    expRef1.set(throwable);
                else if (thread==t2)
                        expRef2.set(throwable);
            }
        };

        t1.createCache();
        t2.createCache();
        t1.setUncaughtExceptionHandler(handler);
        t2.setUncaughtExceptionHandler(handler);
        t1.start();
        t2.start();

        t1.destroyCache();
        Thread.sleep(100);
        t2.destroyCache();
        t1.eventuallyStop();
        t2.eventuallyStop();
        t1.join();
        t2.join();

        Throwable exp1 = expRef1.get();
        Throwable exp2 = expRef2.get();
        if (exp1!=null) {
            throw exp1;
        }
        if (exp2!=null) {
            throw exp2;
        }
    }

    /**
     * Inner class view of thread with running query.
     */
    static class IgniteThread extends Thread {
        /** */
        private final static String cacheName = "EXAMPLE";
        /** */
        final Ignite start;
        /** */
        private volatile boolean working = true;

        /**
         *
         */
        public IgniteThread(String gridName) {
            IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
            igniteConfiguration.setGridName(gridName);
            start = Ignition.start(igniteConfiguration);
            setName(gridName);
        }

        /**
         *
         */
        public void eventuallyStop() {
            working = false;
        }

        /**
         * The main test loop.
         */
        public void run() {
            while (working) {
                ScanQuery<String, String> scanQuery = new ScanQuery<String, String>()
                        .setLocal(true)
                        .setFilter(new IgniteBiPredicate<String, String>() {
                            @Override
                            public boolean apply(String key, String p) {
                                return key.equals("");
                            }
                        });

                IgniteCache<String, String> example = start.cache(cacheName);

                for(int partition : start.affinity(cacheName).primaryPartitions(start.cluster().localNode())) {
                    scanQuery.setPartition(partition);

                    try (QueryCursor cursor = example.query(scanQuery)) {
                        for (Object p : cursor) {
                            String value = (String) ((CacheEntryImpl) p).getValue();
                            System.out.println(value);
                        }
                    } catch (Exception e) {
                        if (check(e))
                            throw new RuntimeException(e);
                        else {
                            if (!start.cacheNames().contains(cacheName)) {
                                working=false;
                            }
                        }
                    }
                }
            }
        }

        /**
         *
         */
        public void createCache() {
            CacheConfiguration configuration = new CacheConfiguration();
            configuration.setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED)
                    .setRebalanceMode(CacheRebalanceMode.SYNC)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                    .setRebalanceThrottle(100)
                    .setRebalanceBatchSize(2*1024*1024)
                    .setBackups(1)
                    .setName(cacheName)
                    .setEagerTtl(false);
            start.getOrCreateCache(configuration);
        }

        /**
         *
         */
        public void destroyCache() {
            start.destroyCache(cacheName);
        }

        /**
         * Recursive function check there aren't NPE in exception or it's parent.
         *
         * @param e Exception which can contain NPE.
         * @return True if NPE is contained.
         */
        private static boolean check(Throwable e) {
            if (e instanceof NullPointerException)
                return true;
            else if (e.getCause()==null)
                return false;
            else {
                return check(e.getCause());
            }
        }
    }
}
