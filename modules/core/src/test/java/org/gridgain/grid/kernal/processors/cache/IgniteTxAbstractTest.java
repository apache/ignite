/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Tests for local transactions.
 */
@SuppressWarnings( {"BusyWait"})
abstract class IgniteTxAbstractTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Execution count. */
    private static final AtomicInteger cntr = new AtomicInteger();

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Start grid by default.
     */
    protected IgniteTxAbstractTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /**
     * @return Grid count.
     */
    protected abstract int gridCount();

    /**
     * @return Key count.
     */
    protected abstract int keyCount();

    /**
     * @return Maximum key value.
     */
    protected abstract int maxKeyValue();

    /**
     * @return Thread iterations.
     */
    protected abstract int iterations();

    /**
     * @return True if in-test logging is enabled.
     */
    protected abstract boolean isTestDebug();

    /**
     * @return {@code True} if memory stats should be printed.
     */
    protected abstract boolean printMemoryStats();

    /** {@inheritDoc} */
    private void debug(String msg) {
        if (isTestDebug())
            info(msg);
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            startGrid(i);
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param i Grid index.
     * @return Cache.
     */
    @SuppressWarnings("unchecked")
    @Override protected GridCache<Integer, String> cache(int i) {
        return grid(i).cache(null);
    }

    /**
     * @return Keys.
     */
    protected Iterable<Integer> getKeys() {
        List<Integer> keys = new ArrayList<>(keyCount());

        for (int i = 0; i < keyCount(); i++)
            keys.add(RAND.nextInt(maxKeyValue()) + 1);

        Collections.sort(keys);

        return Collections.unmodifiableList(keys);
    }

    /**
     * @return Random cache operation.
     */
    protected OP getOp() {
        switch (RAND.nextInt(3)) {
            case 0: { return OP.READ; }
            case 1: { return OP.WRITE; }
            case 2: { return OP.REMOVE; }

            // Should never be reached.
            default: { assert false; return null; }
        }
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If check failed.
     */
    protected void checkCommit(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) throws Exception {
        int gridIdx = RAND.nextInt(gridCount());

        Ignite ignite = grid(gridIdx);

        if (isTestDebug())
            debug("Checking commit on grid: " + ignite.cluster().localNode().id());

        for (int i = 0; i < iterations(); i++) {
            GridCache<Integer, String> cache = cache(gridIdx);

            IgniteTx tx = cache.txStart(concurrency, isolation, 0, 0);

            try {
                int prevKey = -1;

                for (Integer key : getKeys()) {
                    // Make sure we have the same locking order for all concurrent transactions.
                    assert key >= prevKey : "key: " + key + ", prevKey: " + prevKey;

                    if (isTestDebug()) {
                        GridCacheAffinityFunction aff = cache.configuration().getAffinity();

                        int part = aff.partition(key);

                        debug("Key affinity [key=" + key + ", partition=" + part + ", affinity=" +
                            U.toShortString(cache.affinity().mapPartitionToPrimaryAndBackups(part)) + ']');
                    }

                    String val = Integer.toString(key);

                    switch (getOp()) {
                        case READ: {
                            if (isTestDebug())
                                debug("Reading key [key=" + key + ", i=" + i + ']');

                            val = cache.get(key);

                            if (isTestDebug())
                                debug("Read value for key [key=" + key + ", val=" + val + ']');

                            break;
                        }

                        case WRITE: {
                            if (isTestDebug())
                                debug("Writing key and value [key=" + key + ", val=" + val + ", i=" + i + ']');

                            cache.put(key, val);

                            break;
                        }

                        case REMOVE: {
                            if (isTestDebug())
                                debug("Removing key [key=" + key + ", i=" + i  + ']');

                            cache.remove(key);

                            break;
                        }

                        default: { assert false; }
                    }
                }

                tx.commit();

                if (isTestDebug())
                    debug("Committed transaction [i=" + i + ", tx=" + tx + ']');
            }
            catch (GridCacheTxOptimisticException e) {
                if (concurrency != OPTIMISTIC || isolation != SERIALIZABLE) {
                    error("Received invalid optimistic failure.", e);

                    throw e;
                }

                if (isTestDebug())
                    info("Optimistic transaction failure (will rollback) [i=" + i + ", msg=" + e.getMessage() +
                        ", tx=" + tx.xid() + ']');

                try {
                    tx.rollback();
                }
                catch (IgniteCheckedException ex) {
                    error("Failed to rollback optimistic failure: " + tx, ex);

                    throw ex;
                }
            }
            catch (Exception e) {
                error("Transaction failed (will rollback): " + tx, e);

                tx.rollback();

                throw e;
            }
            catch (Error e) {
                error("Error when executing transaction (will rollback): " + tx, e);

                tx.rollback();

                throw e;
            }
            finally {
                IgniteTx t = cache.tx();

                assert t == null : "Thread should not have transaction upon completion ['t==tx'=" + (t == tx) +
                    ", t=" + t + (t != tx ? "tx=" + tx : "tx=''") + ']';
            }
        }

        if (printMemoryStats()) {
            if (cntr.getAndIncrement() % 100 == 0)
                // Print transaction memory stats.
                ((GridKernal)grid(gridIdx)).internalCache().context().tm().printMemoryStats();
        }
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws IgniteCheckedException If check failed.
     */
    protected void checkRollback(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation)
        throws Exception {
        checkRollback(new ConcurrentHashMap<Integer, String>(), concurrency, isolation);
    }

    /**
     * @param map Map to check.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws IgniteCheckedException If check failed.
     */
    protected void checkRollback(ConcurrentMap<Integer, String> map, IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation) throws Exception {
        int gridIdx = RAND.nextInt(gridCount());

        Ignite ignite = grid(gridIdx);

        if (isTestDebug())
            debug("Checking commit on grid: " + ignite.cluster().localNode().id());

        for (int i = 0; i < iterations(); i++) {
            GridCache<Integer, String> cache = cache(gridIdx);

            IgniteTx tx = cache.txStart(concurrency, isolation, 0, 0);

            try {
                for (Integer key : getKeys()) {
                    if (isTestDebug()) {
                        GridCacheAffinityFunction aff = cache.configuration().getAffinity();

                        int part = aff.partition(key);

                        debug("Key affinity [key=" + key + ", partition=" + part + ", affinity=" +
                            U.toShortString(cache.affinity().mapPartitionToPrimaryAndBackups(part)) + ']');
                    }

                    String val = Integer.toString(key);

                    switch (getOp()) {
                        case READ: {
                            debug("Reading key: " + key);

                            checkMap(map, key, cache.get(key));

                            break;
                        }

                        case WRITE: {
                            debug("Writing key and value [key=" + key + ", val=" + val + ']');

                            checkMap(map, key, cache.put(key, val));

                            break;
                        }

                        case REMOVE: {
                            debug("Removing key: " + key);

                            checkMap(map, key, cache.remove(key));

                            break;
                        }

                        default: { assert false; }
                    }
                }

                tx.rollback();

                debug("Rolled back transaction: " + tx);
            }
            catch (GridCacheTxOptimisticException e) {
                tx.rollback();

                log.warning("Rolled back transaction due to optimistic exception [tx=" + tx + ", e=" + e + ']');

                throw e;
            }
            catch (Exception e) {
                tx.rollback();

                error("Rolled back transaction due to exception [tx=" + tx + ", e=" + e + ']');

                throw e;
            }
            finally {
                IgniteTx t1 = cache.tx();

                debug("t1=" + t1);

                assert t1 == null : "Thread should not have transaction upon completion ['t==tx'=" + (t1 == tx) +
                    ", t=" + t1 + ']';
            }
        }
    }

    /**
     * @param map Map to check against.
     * @param key Key.
     * @param val Value.
     */
    private void checkMap(ConcurrentMap<Integer, String> map, Integer key, String val) {
        if (val != null) {
            String v = map.putIfAbsent(key, val);

            assert v == null || v.equals(val);
        }
    }

    /**
     * Checks integrity of all caches after tests.
     *
     * @throws IgniteCheckedException If check failed.
     */
    @SuppressWarnings({"ErrorNotRethrown"})
    protected void finalChecks() throws Exception {
        for (int i = 1; i <= maxKeyValue(); i++) {
            for (int k = 0; k < 3; k++) {
                try {
                    GridCacheEntry<Integer, String> e1 = null;

                    String v1 = null;

                    for (int j = 0; j < gridCount(); j++) {
                        GridCache<Integer, String> cache = cache(j);

                        IgniteTx tx = cache.tx();

                        assertNull("Transaction is not completed: " + tx, tx);

                        if (j == 0) {
                            e1 = cache.entry(i);

                            v1 = e1.get();
                        }
                        else {
                            GridCacheEntry<Integer, String> e2 = cache.entry(i);

                            String v2 = e2.get();

                            if (!F.eq(v2, v1)) {
                                v1 = e1.get();
                                v2 = e2.get();
                            }

                            assert F.eq(v2, v1) :
                                "Invalid cached value [key=" + i + ", v1=" + v1 + ", v2=" + v2 + ", e1=" + e1 +
                                    ", e2=" + e2 + ", grid=" + j + ']';
                        }
                    }

                    break;
                }
                catch (AssertionError e) {
                    if (k == 2)
                        throw e;
                    else
                        // Wait for transactions to complete.
                        Thread.sleep(500);
                }
            }
        }

        for (int i = 1; i <= maxKeyValue(); i++) {
            for (int k = 0; k < 3; k++) {
                try {
                    for (int j = 0; j < gridCount(); j++) {
                        GridCacheProjection<Integer, String> cache = cache(j);

                        cache.removeAll();

//                        assert cache.keySet().isEmpty() : "Cache is not empty: " + cache.entrySet();
                    }

                    break;
                }
                catch (AssertionError e) {
                    if (k == 2)
                        throw e;
                    else
                        // Wait for transactions to complete.
                        Thread.sleep(500);
                }
            }
        }
    }

    /**
     * Cache operation.
     */
    protected enum OP {
        /** Cache read. */
        READ,

        /** Cache write. */
        WRITE,

        /** Cache remove. */
        REMOVE
    }
}
