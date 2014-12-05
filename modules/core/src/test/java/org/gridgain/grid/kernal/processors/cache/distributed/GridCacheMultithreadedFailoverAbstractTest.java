/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Base test for all multithreaded cache scenarios w/ and w/o failover.
 */
public class GridCacheMultithreadedFailoverAbstractTest extends GridCommonAbstractTest {
    /** Node name prefix. */
    private static final String NODE_PREFIX = "node";

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Put condition lock (used to suspend put threads when caches are compared). */
    private final Lock lock = new ReentrantLock();

    /** Node kill lock (used to prevent killing while cache data is compared). */
    private final Lock killLock = new ReentrantLock();

    /** Proceed put condition. */
    private final Condition putCond = lock.newCondition();

    /** Shared IP finder. */
    private final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Caches comparison start latch. */
    private CountDownLatch cmpLatch;

    /** Caches comparison request flag. */
    private volatile boolean cmp;

    /**
     * @return Number of threads executing put.
     */
    protected int putThreads() {
        return 15;
    }

    /**
     * @return Test duration in seconds.
     */
    protected int duration() {
        return 3 * 60 * 1000;
    }

    /**
     * @return Frequency of cache data comparison.
     */
    protected int cacheComparisonFrequency() {
        return 20 * 1000;
    }

    /**
     * @return Put key range.
     */
    protected int keyRange() {
        return 10_000;
    }

    /**
     * @return Cache mode.
     */
    protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return Atomic write order mode.
     */
    protected GridCacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return null;
    }

    /**
     * @return Distribution mode.
     */
    protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /**
     * @return Number of data nodes.
     */
    protected int dataNodes() {
        return 3;
    }

    /**
     * @return Number of backups.
     */
    protected int backups() {
        return 1;
    }

    /**
     * @return Probability of killing data node.
     */
    protected int nodeKillProbability() {
        return 1;
    }

    /**
     * @return Min and max value for delay between node killings.
     */
    protected T2<Long, Long> killDelay() {
        return new T2<>(5000L, 10000L);
    }

    /**
     * @return Min and max value for delay between node killing and restarting.
     */
    protected T2<Long, Long> restartDelay() {
        return new T2<>(5000L, 10000L);
    }

    /**
     * Get node name by index.
     *
     * @param idx Node index.
     * @return Node name.
     */
    private String nodeName(int idx) {
        return NODE_PREFIX + idx;
    }

    /**
     * Start up routine.
     *
     * @throws Exception If failed.
     */
    private void startUp() throws Exception {
        assert dataNodes() > 0;
        assert cacheMode() != null;
        assert atomicityMode() != null;

        for (int i = 0; i < dataNodes(); i++)
            G.start(configuration(i));
    }

    /**
     * Node configuration.
     *
     * @param idx Node index.
     * @return Node configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration configuration(int idx) throws Exception {
        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(cacheMode());
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setPreloadMode(SYNC);
        ccfg.setSwapEnabled(false);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setEvictionPolicy(null);
        ccfg.setNearEvictionPolicy(null);

        ccfg.setDgcFrequency(0);

        if (cacheMode() == PARTITIONED)
            ccfg.setBackups(backups());

        if (atomicityMode() == ATOMIC) {
            assert atomicWriteOrderMode() != null;

            ccfg.setAtomicWriteOrderMode(atomicWriteOrderMode());

            if (cacheMode() == PARTITIONED)
                ccfg.setDistributionMode(PARTITIONED_ONLY);
        }
        else {
            if (cacheMode() == PARTITIONED) {
                assert distributionMode() != null;

                ccfg.setDistributionMode(distributionMode());
            }
        }

        IgniteConfiguration cfg = getConfiguration(nodeName(idx));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setLocalHost("127.0.0.1");
        cfg.setCacheConfiguration(ccfg);
        cfg.setRestEnabled(false);

        return cfg;
    }

    /**
     * Actual test.
     *
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        startUp();

        final CyclicBarrier startBarrier = new CyclicBarrier(putThreads());

        final ConcurrentHashMap<Integer, Integer> expVals = new ConcurrentHashMap<>();

        final int keysPerThread = keyRange() / putThreads();

        final AtomicLong ctr = new AtomicLong();
        final AtomicLong errCtr = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean();

        assert keysPerThread > 0;

        Thread[] putThreads = new Thread[putThreads()];

        for (int i = 0; i < putThreads(); i++) {
            final int idx = i;

            Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        startBarrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException ignore) {
                        return ;
                    }

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Ignite ignite = G.grid(nodeName(0));

                    GridCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

                    int startKey = keysPerThread * idx;
                    int endKey = keysPerThread * (idx + 1);

                    Map<Integer, Integer> putMap = new HashMap<>();
                    Collection<Integer> rmvSet = new HashSet<>();

                    while (!stop.get()) {
                        for (int i = 0; i < 100; i++) {
                            int key = rnd.nextInt(startKey, endKey);

                            if (rnd.nextInt(0, 10) > 0) {
                                putMap.put(key, i);

                                rmvSet.remove(key);
                            }
                            else {
                                rmvSet.add(key);

                                putMap.remove(key);
                            }
                        }
                        try {
                            GridCacheTx tx = atomicityMode() == TRANSACTIONAL ? cache.txStart() : null;

                            try {
                                cache.putAll(putMap);
                                cache.removeAll(rmvSet);

                                if (tx != null)
                                    tx.commit();
                            }
                            finally {
                                if (tx != null)
                                    tx.close();
                            }

                            expVals.putAll(putMap);

                            for (Integer key : rmvSet)
                                expVals.remove(key);
                        }
                        catch (GridException e) {
                            log.error("Cache update failed [putMap=" + putMap+ ", rmvSet=" + rmvSet + ']', e);

                            errCtr.incrementAndGet();
                        }

                        ctr.addAndGet(putMap.size() + rmvSet.size());

                        try {
                            if (cmp) {
                                cmpLatch.countDown();

                                lock.lock();

                                try {
                                    while (cmp)
                                        putCond.await();
                                }
                                finally {
                                    lock.unlock();
                                }
                            }
                        }
                        catch (InterruptedException ignore) {
                            return;
                        }
                    }
                }
            });

            thread.setName("put-thread-" + i);

            thread.start();

            putThreads[i] = thread;
        }

        IgniteFuture<?> killNodeFut = null;

        if (nodeKillProbability() > 0) {
            killNodeFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        U.sleep(ThreadLocalRandom.current().nextLong(killDelay().get1(), killDelay().get2()));

                        killLock.lock();

                        try {
                            int idx = ThreadLocalRandom.current().nextInt(1, dataNodes());

                            String gridName = nodeName(idx);

                            if (stop.get())
                                return null;

                            log.info("Killing node [gridName=" + gridName + ']');

                            stopGrid(gridName);

                            U.sleep(ThreadLocalRandom.current().nextLong(restartDelay().get1(), restartDelay().get2()));

                            if (stop.get())
                                return null;

                            log.info("Restarting node [gridName=" + gridName + ']');

                            G.start(configuration(idx));
                        }
                        finally {
                            killLock.unlock();
                        }
                    }

                    return null;
                }
            });
        }

        boolean failed = false;

        try {
            long stopTime = U.currentTimeMillis() + duration();

            long nextCmp = U.currentTimeMillis() + cacheComparisonFrequency();

            while (!failed && U.currentTimeMillis() < stopTime) {
                long start = System.nanoTime();

                long ops = ctr.longValue();

                U.sleep(1000);

                long diff = ctr.longValue() - ops;

                double time = (System.nanoTime() - start) / 1_000_000_000d;

                long opsPerSecond = (long)(diff / time);

                log.info("Operations/second: " + opsPerSecond);

                if (U.currentTimeMillis() >= nextCmp) {
                    failed = !compare(expVals);

                    nextCmp = System.currentTimeMillis() + cacheComparisonFrequency();
                }
            }
        }
        finally {
            stop.set(true);
        }

        if (killNodeFut != null)
            killNodeFut.get();

        for (Thread thread : putThreads)
            U.join(thread);

        log.info("Test finished. Put errors: " + errCtr.get());

        assertFalse("Test failed", failed);
    }

    /**
     * Compare cache content.
     *
     * @param expVals Expected values.
     * @return {@code True} if check passed successfully.
     * @throws Exception If failed.
     */
    private boolean compare(Map<Integer, Integer> expVals) throws Exception {
        cmpLatch = new CountDownLatch(putThreads());

        cmp = true;

        killLock.lock();

        try {
            log.info("Comparing cache content.");

            if (!cmpLatch.await(60_000, TimeUnit.MILLISECONDS))
                throw new GridException("Failed to suspend threads executing put.");

            if (compareCaches(expVals)) {
                log.info("Cache comparison succeeded.");

                return true;
            }
            else {
                log.error("Cache comparison failed.");

                return false;
            }
        }
        finally {
            killLock.unlock();

            lock.lock();

            try {
                cmp = false;

                putCond.signalAll();
            }
            finally {
                lock.unlock();
            }

            U.sleep(500);
        }
    }

    /**
     * Compare caches.
     *
     * @param expVals Expected values.
     * @return {@code True} if check passed successfully.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope", "ConstantIfStatement"})
    private boolean compareCaches(Map<Integer, Integer> expVals) throws Exception {
        List<GridCache<Integer, Integer>> caches = new ArrayList<>(dataNodes());
        List<GridDhtCacheAdapter<Integer, Integer>> dhtCaches = null;

        for (int i = 0 ; i < dataNodes(); i++) {
            GridCache<Integer, Integer> cache = G.grid(nodeName(i)).cache(CACHE_NAME);

            assert cache != null;

            caches.add(cache);

            GridCacheAdapter<Integer, Integer> cache0 =
                (GridCacheAdapter<Integer, Integer>)cache.<Integer, Integer>cache();

            if (cache0.isNear()) {
                if (dhtCaches == null)
                    dhtCaches = new ArrayList<>(dataNodes());

                dhtCaches.add(((GridNearCacheAdapter<Integer, Integer>)cache0).dht());
            }
        }

        // Compare key sets on each cache.
        Collection<Integer> cacheKeys = new HashSet<>();
        Collection<Integer> dhtCacheKeys = new HashSet<>();

        for (int i = 0; i < dataNodes(); i++) {
            cacheKeys.addAll(caches.get(i).keySet());

            if (dhtCaches != null)
                dhtCacheKeys.addAll(dhtCaches.get(i).keySet());
        }

        boolean failed = false;

        if (!F.eq(expVals.keySet(), cacheKeys)) {
            Collection<Integer> expOnly = new HashSet<>();
            Collection<Integer> cacheOnly = new HashSet<>();

            expOnly.addAll(expVals.keySet());
            expOnly.removeAll(cacheKeys);

            cacheOnly.addAll(cacheKeys);
            cacheOnly.removeAll(expVals.keySet());

            if (!expOnly.isEmpty())
                log.error("Cache does not contain expected keys: " + expOnly);

            if (!cacheOnly.isEmpty())
                log.error("Cache does contain unexpected keys: " + cacheOnly);

            failed = true;
        }

        if (dhtCaches != null && !F.eq(expVals.keySet(), dhtCacheKeys)) {
            Collection<Integer> expOnly = new HashSet<>();
            Collection<Integer> cacheOnly = new HashSet<>();

            expOnly.addAll(expVals.keySet());
            expOnly.removeAll(dhtCacheKeys);

            cacheOnly.addAll(dhtCacheKeys);
            cacheOnly.removeAll(expVals.keySet());

            if (!expOnly.isEmpty())
                log.error("DHT cache does not contain expected keys: " + expOnly);

            if (!cacheOnly.isEmpty())
                log.error("DHT cache does contain unexpected keys: " + cacheOnly);

            failed = true;
        }

        // Compare values.
        Collection<Integer> failedKeys = new HashSet<>();

        for (Map.Entry<Integer, Integer> entry : expVals.entrySet()) {
            for (int i = 0; i < dataNodes(); i++) {
                if (!F.eq(caches.get(i).get(entry.getKey()), entry.getValue()))
                    failedKeys.add(entry.getKey());
            }
        }

        if (!failedKeys.isEmpty()) {
            log.error("Cache content is incorrect for " + failedKeys.size() + " keys:");

            for (Integer key : failedKeys) {
                for (int i = 0; i < dataNodes(); i++) {
                    GridCacheEntry<Integer, Integer> cacheEntry = caches.get(i).entry(key);

                    UUID nodeId = G.grid(nodeName(i)).cluster().localNode().id();

                    if (!F.eq(cacheEntry.get(), expVals.get(key)))
                        log.error("key=" + key + ", expVal=" + expVals.get(key) + ", cacheVal=" + cacheEntry.get() +
                            ", primary=" + cacheEntry.primary() + ", backup=" + cacheEntry.backup() +
                            ", nodeId=" + nodeId);
                }
            }

            failed = true;
        }

        return !failed;
    }
}
