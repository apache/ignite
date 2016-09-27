package com.rc.cqlost;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.NullLogger;

import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

//TODO: Remove before merge
public class CQLostRunner {
    /**
     * Number of stable nodes.
     */
    private static final int STABLE_NODE_CNT = 1;

    /**
     * Batch size.
     */
    private static final int BATCH_SIZE = 1000;

    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "sessionSignal";

    /**
     * Listener counter.
     */
    private static final AtomicLong LSNR_CTR = new AtomicLong();

    /**
     * Listener map.
     */
    private static final Map<Integer, CacheEntryEvent> LSNR_MAP = new ConcurrentHashMap<>();

    /**
     * Entry point.
     */
    public static void main(String[] args) throws Exception {
        // Start stable nodes.
        for (int i = 0; i < STABLE_NODE_CNT; i++)
            startAndQuery(i);

        for (int i = 0; i < STABLE_NODE_CNT; i++)
            System.out.println(name(i) + ": " + Ignition.ignite(name(i)).cluster().localNode().id());

        sleep(5000);

        startGeneratorThread();
        startNodeRestartThread();
    }

    /**
     * Start thread generating data.
     */
    @SuppressWarnings("InfiniteLoopStatement")
    private static void startGeneratorThread() {
        Thread t = new Thread(() -> {
            IgniteCache<Integer, Integer> cache = Ignition.ignite(name(0)).cache(CACHE_NAME);

            long putCnt = 0;

            int iteration = 0;

            while (true) {
                for (int i = 0; i < BATCH_SIZE; i++)
                    cache.put(i, iteration);

                putCnt += BATCH_SIZE;

                while (true) {
                    long cnt = LSNR_CTR.get();

                    long expCnt = putCnt * STABLE_NODE_CNT;

                    if (cnt != expCnt) {
                        print("EXPECTED=" + expCnt + ", ACTUAL=" + cnt + ", ITERATION=" + iteration);

                        for (int i = 0; i < BATCH_SIZE; i++) {
                            Integer key = i;
                            Integer val = cache.get(key);

                            if (!F.eq(val, iteration))
                                print(">>> WRONG CACHE VALUE (lost data?) [key=" + key + ", val=" + val + ']');
                        }

                        for (Map.Entry<Integer, CacheEntryEvent> entry : LSNR_MAP.entrySet()) {
                            Integer key = entry.getKey();
                            CacheEntryEvent evt = entry.getValue();

                            if (!F.eq(evt.getValue(), iteration)) {
                                print(">>> WRONG LISTENER VALUE (lost event?) [key=" + key + ", val=" + evt.getValue() + ']');
                            }
                        }
                    } else
                        break;

                    sleep(1000);
                }

                iteration++;
            }
        });

        t.setDaemon(true);

        t.start();
    }

    /**
     * Start thread which restarts a node over and over again.
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public static void startNodeRestartThread() {
        Thread t = new Thread(() -> {
            sleep(2000);
            while (true) {
                Ignite node = Ignition.start(config(STABLE_NODE_CNT).setGridLogger(new NullLogger()));

                sleep(2000);

                Ignition.stop(node.name(), true);

                sleep(2000);
            }
        });

        t.setDaemon(true);

        t.start();
    }

    /**
     * Start node, start CQ on cache and return the cache.
     *
     * @param idx Index.
     * @return Cache.
     */
    private static IgniteCache<Integer, Integer> startAndQuery(int idx) {
        IgniteConfiguration cfg = config(idx);

        Ignite node = Ignition.start(cfg);

        IgniteCache<Integer, Integer> cache = node.cache(CACHE_NAME);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setLocalListener(new Listener());
        qry.setRemoteFilterFactory(new FilterFactory());

        cache.query(qry);

        return cache;
    }

    /**
     * Create Ignite config.
     *
     * @param idx Index.
     * @return Config.
     */
    private static IgniteConfiguration config(int idx) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(name(idx));
        cfg.setLocalHost("127.0.0.1");

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY);
        ccfg.setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 43200)));
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setLateAffinityAssignment(false);

        return cfg;
    }

    /**
     * Get node name for index.
     *
     * @param idx Index.
     * @return Name.
     */
    private static String name(int idx) {
        return "grid-" + idx;
    }

    /**
     * Sleep for the given duration.
     *
     * @param dur Duration.
     */
    private static void sleep(long dur) {
        try {
            Thread.sleep(dur);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException("Interrupted.", e);
        }
    }

    /**
     * Print message with thread name.
     *
     * @param msg Message.
     */
    private static void print(Object msg) {
        System.out.println(Thread.currentThread().getName() + ": " + msg);
    }

    /**
     * Listener.
     */
    private static class Listener implements CacheEntryUpdatedListener<Integer, Integer> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
                LSNR_CTR.incrementAndGet();

                CacheEntryEvent prev = LSNR_MAP.put(evt.getKey(), evt);

                if (prev!=null && prev.getValue().equals(evt.getValue()))
                    System.out.println("prev: " + prev + ", " + evt);
            }
        }
    }

    /**
     * Filter.
     */
    private static class Filter implements CacheEntryEventSerializableFilter<Integer, Integer> {
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Integer> evt)
                throws CacheEntryListenerException {
            return true;
        }
    }

    /**
     * Filter factory.
     */
    private static class FilterFactory implements Factory<Filter>, Serializable {
        /**
         * {@inheritDoc}
         */
        @Override
        public Filter create() {
            return new Filter();
        }
    }
}