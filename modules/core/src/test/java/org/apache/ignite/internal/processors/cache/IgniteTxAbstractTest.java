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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

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
    protected void checkCommit(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        int gridIdx = RAND.nextInt(gridCount());

        Ignite ignite = grid(gridIdx);

        if (isTestDebug())
            debug("Checking commit on grid: " + ignite.cluster().localNode().id());

        for (int i = 0; i < iterations(); i++) {
            IgniteCache<Integer, String> cache = jcache(gridIdx);

            try (Transaction tx = ignite(gridIdx).transactions().txStart(concurrency, isolation, 0, 0)) {
                int prevKey = -1;

                for (Integer key : getKeys()) {
                    // Make sure we have the same locking order for all concurrent transactions.
                    assert key >= prevKey : "key: " + key + ", prevKey: " + prevKey;

                    if (isTestDebug()) {
                        AffinityFunction aff = cache.getConfiguration(CacheConfiguration.class).getAffinity();

                        int part = aff.partition(key);

                        debug("Key affinity [key=" + key + ", partition=" + part + ", affinity=" +
                            U.toShortString(ignite(gridIdx).affinity(null).mapPartitionToPrimaryAndBackups(part)) + ']');
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
            catch (TransactionOptimisticException e) {
                if (!(concurrency == OPTIMISTIC && isolation == SERIALIZABLE)) {
                    log.error("Unexpected error: " + e, e);

                    throw e;
                }
            }
            catch (Throwable e) {
                log.error("Unexpected error: " + e, e);

                throw e;
            }
        }

        Transaction tx = ignite(gridIdx).transactions().tx();

        assertNull("Thread should not have transaction upon completion", tx);

        if (printMemoryStats()) {
            if (cntr.getAndIncrement() % 100 == 0)
                // Print transaction memory stats.
                ((IgniteKernal)grid(gridIdx)).internalCache().context().tm().printMemoryStats();
        }
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws IgniteCheckedException If check failed.
     */
    protected void checkRollback(TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws Exception {
        checkRollback(new ConcurrentHashMap<Integer, String>(), concurrency, isolation);
    }

    /**
     * @param map Map to check.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws IgniteCheckedException If check failed.
     */
    protected void checkRollback(ConcurrentMap<Integer, String> map, TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {
        int gridIdx = RAND.nextInt(gridCount());

        Ignite ignite = grid(gridIdx);

        if (isTestDebug())
            debug("Checking commit on grid: " + ignite.cluster().localNode().id());

        for (int i = 0; i < iterations(); i++) {
            IgniteCache<Integer, String> cache = jcache(gridIdx);

            Transaction tx = ignite(gridIdx).transactions().txStart(concurrency, isolation, 0, 0);

            try {
                for (Integer key : getKeys()) {
                    if (isTestDebug()) {
                        AffinityFunction aff = cache.getConfiguration(CacheConfiguration.class).getAffinity();

                        int part = aff.partition(key);

                        debug("Key affinity [key=" + key + ", partition=" + part + ", affinity=" +
                            U.toShortString(ignite(gridIdx).affinity(null).mapPartitionToPrimaryAndBackups(part)) + ']');
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

                            checkMap(map, key, cache.getAndPut(key, val));

                            break;
                        }

                        case REMOVE: {
                            debug("Removing key: " + key);

                            checkMap(map, key, cache.getAndRemove(key));

                            break;
                        }

                        default: { assert false; }
                    }
                }

                tx.rollback();

                debug("Rolled back transaction: " + tx);
            }
            catch (TransactionOptimisticException e) {
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
                Transaction t1 = ignite(gridIdx).transactions().tx();

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
                    String v1 = null;

                    for (int j = 0; j < gridCount(); j++) {
                        IgniteCache<Integer, String> cache = jcache(j);

                        Transaction tx = ignite(j).transactions().tx();

                        assertNull("Transaction is not completed: " + tx, tx);

                        if (j == 0) {
                            v1 = cache.get(i);
                        }
                        else {
                            String v2 = cache.get(i);

                            if (!F.eq(v2, v1)) {
                                v1 = this.<Integer, String>jcache(0).get(i);
                                v2 = cache.get(i);
                            }

                            assert F.eq(v2, v1) :
                                "Invalid cached value [key=" + i + ", v1=" + v1 + ", v2=" + v2 + ", grid=" + j + ']';
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
                        IgniteCache<Integer, String> cache = jcache(j);

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
