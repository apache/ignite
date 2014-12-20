/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.transactions.GridCacheTxConcurrency.*;
import static org.apache.ignite.transactions.GridCacheTxIsolation.*;

/**
 * Abstract class for cache tests.
 */
public class GridCacheFinishPartitionsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 1;

    /** Grid kernal. */
    private GridKernal grid;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid = (GridKernal)grid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxFinishPartitions() throws Exception {
        String key = "key";
        String val = "value";

        GridCache<String, String> cache = grid.cache(null);

        int keyPart = grid.<String, String>internalCache().context().affinity().partition(key);

        cache.put(key, val);

        // Wait for tx-enlisted partition.
        long waitTime = runTransactions(key, keyPart, F.asList(keyPart));

        info("Wait time, ms: " + waitTime);

        // Wait for not enlisted partition.
        waitTime = runTransactions(key, keyPart, F.asList(keyPart + 1));

        info("Wait time, ms: " + waitTime);

        // Wait for both partitions.
        waitTime = runTransactions(key, keyPart, F.asList(keyPart, keyPart + 1));

        info("Wait time, ms: " + waitTime);
    }

    /**
     * @param key Key.
     * @param keyPart Key partition.
     * @param waitParts Partitions to wait.
     * @return Wait time.
     * @throws Exception If failed.
     */
    private long runTransactions(final String key, final int keyPart, final Collection<Integer> waitParts)
        throws Exception {
        int threadNum = 1;

        final CyclicBarrier barrier = new CyclicBarrier(threadNum);
        final CountDownLatch latch = new CountDownLatch(threadNum);

        final AtomicLong start = new AtomicLong();

        GridTestUtils.runMultiThreaded(new Callable() {
            @Override public Object call() throws Exception {
                if (barrier.await() == 0)
                    start.set(System.currentTimeMillis());

                GridCache<String, String> cache = grid(0).cache(null);

                IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ);

                cache.get(key);

                IgniteFuture<?> fut = grid.context().cache().context().partitionReleaseFuture(GRID_CNT + 1);

                fut.listenAsync(new CI1<IgniteFuture<?>>() {
                    @Override public void apply(IgniteFuture<?> e) {
                        latch.countDown();
                    }
                });

                assert !fut.isDone() : "Failed waiting for locks " +
                    "[keyPart=" + keyPart + ", waitParts=" + waitParts + ", done=" + fut.isDone() + ']';

                tx.commit();

                return null;
            }
        }, threadNum, "test-finish-partitions-thread");

        latch.await();

        return System.currentTimeMillis() - start.get();
    }

    /**
     * Tests method {@link GridCacheMvccManager#finishLocks(org.apache.ignite.lang.IgnitePredicate, long)}.
     *
     * @throws Exception If failed.
     */
    public void testMvccFinishPartitions() throws Exception {
        String key = "key";

        int keyPart = grid.internalCache().context().affinity().partition(key);

        // Wait for tx-enlisted partition.
        long waitTime = runLock(key, keyPart, F.asList(keyPart));

        info("Wait time, ms: " + waitTime);

        // Wait for not enlisted partition.
        waitTime = runLock(key, keyPart, F.asList(keyPart + 1));

        info("Wait time, ms: " + waitTime);

        // Wait for both partitions.
        waitTime = runLock(key, keyPart, F.asList(keyPart, keyPart + 1));

        info("Wait time, ms: " + waitTime);
    }

    /**
     * Tests finish future for particular set of keys.
     *
     * @throws Exception If failed.
     */
    public void testMvccFinishKeys() throws Exception {
        GridCache<String, Integer> cache = grid(0).cache(null);

        try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            final String key = "key";

            cache.get(key);

            GridCacheAdapter<String, Integer> internal = grid.internalCache();

            IgniteFuture<?> nearFut = internal.context().mvcc().finishKeys(Collections.singletonList(key), 2);

            IgniteFuture<?> dhtFut = internal.context().near().dht().context().mvcc().finishKeys(
                Collections.singletonList(key), 2);

            assert !nearFut.isDone();
            assert !dhtFut.isDone();

            tx.commit();
        }
    }

    /**
     * Tests chained locks and partitions release future.
     *
     * @throws Exception If failed.
     */
    public void testMvccFinishPartitionsContinuousLockAcquireRelease() throws Exception {
        int key = 1;

        GridCacheSharedContext<Object, Object> ctx = grid.context().cache().context();

        final AtomicLong end = new AtomicLong(0);

        final CountDownLatch latch = new CountDownLatch(1);

        GridCache<Integer, String> cache = grid.cache(null);

        assert cache.lock(key, 0);

        long start = System.currentTimeMillis();

        info("Start time: " + start);

        IgniteFuture<?> fut = ctx.partitionReleaseFuture(GRID_CNT + 1);

        assert fut != null;

        fut.listenAsync(new CI1<IgniteFuture<?>>() {
            @Override public void apply(IgniteFuture<?> e) {
                end.set(System.currentTimeMillis());

                latch.countDown();

                info("End time: " + end.get());
            }
        });

        assert cache.lock(key + 1, 0);

        cache.unlock(key);

        assert cache.lock(key + 2, 0);

        cache.unlock(key + 1);

        assert !fut.isDone() : "Failed waiting for locks";

        cache.unlock(key + 2);

        latch.await();
    }

    /**
     * @param key Key.
     * @param keyPart Key partition.
     * @param waitParts Partitions to wait.
     * @return Wait time.
     * @throws Exception If failed.
     */
    private long runLock(String key, int keyPart, Collection<Integer> waitParts) throws Exception {

        GridCacheSharedContext<Object, Object> ctx = grid.context().cache().context();

        final AtomicLong end = new AtomicLong(0);

        final CountDownLatch latch = new CountDownLatch(1);

        GridCache<String, String> cache = grid.cache(null);

        assert cache.lock(key, 0);

        long start;
        try {
            start = System.currentTimeMillis();

            info("Start time: " + start);

            IgniteFuture<?> fut = ctx.partitionReleaseFuture(GRID_CNT + 1);

            assert fut != null;

            fut.listenAsync(new CI1<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> e) {
                    end.set(System.currentTimeMillis());

                    latch.countDown();

                    info("End time: " + end.get());
                }
            });

            assert !fut.isDone() : "Failed waiting for locks [keyPart=" + keyPart + ", waitParts=" + waitParts + ", done="
                + fut.isDone() + ']';
        }
        finally {
            cache.unlock(key);
        }

        latch.await();

        return end.get() - start;
    }
}
