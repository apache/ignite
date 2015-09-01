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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Nested transaction emulation.
 */
public class GridCacheNestedTxAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Counter key. */
    private static final String CNTR_KEY = "CNTR_KEY";

    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Number of threads. */
    private static final int THREAD_CNT = 10;

    /**  */
    private static final int RETRIES = 10;

    /** */
    private static final AtomicInteger globalCntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < GRID_CNT; i++) {
            grid(i).cache(null).removeAll();

            assert grid(i).cache(null).localSize() == 0;
        }
    }

    /**
     * Default constructor.
     *
     */
    protected GridCacheNestedTxAbstractTest() {
        super(false /** Start grid. */);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testTwoTx() throws Exception {
        final IgniteCache<String, Integer> c = grid(0).cache(null);

        GridKernalContext ctx = ((IgniteKernal)grid(0)).context();

        c.put(CNTR_KEY, 0);

        for (int i = 0; i < 10; i++) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                c.get(CNTR_KEY);

                ctx.closure().callLocalSafe((new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        assertFalse(((GridCacheAdapter)c).context().tm().inUserTx());

                        assertNull(((GridCacheAdapter)c).context().tm().userTx());

                        return true;
                    }
                }), true);

                tx.commit();
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testLockAndTx() throws Exception {
        final IgniteCache<String, Integer> c = grid(0).cache(null);

        Collection<Thread> threads = new LinkedList<>();

        c.put(CNTR_KEY, 0);

        for (int i = 0; i < THREAD_CNT; i++) {
            info("*** Init tx thread: " + i);

            threads.add(new Thread(new Runnable() {
                @Override public void run() {
                    Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                    try {
                        int cntr = c.get(CNTR_KEY);

                        info("*** Cntr in tx thread: " + cntr);

                        c.put(CNTR_KEY, ++cntr);

                        tx.commit();
                    }
                    catch (IgniteException e) {
                        error("Failed tx thread", e);
                    }
                }
            }));
        }

        for (int i = 0; i < THREAD_CNT; i++) {
            info("*** Init lock thread: " + i);

            threads.add(new Thread(new Runnable() {
                @Override public void run() {

                    Lock lock = c.lock(CNTR_KEY);

                    try {
                        lock.lock();

                        int cntr = c.get(CNTR_KEY);

                        info("*** Cntr in lock thread: " + cntr);

                        c.put(CNTR_KEY, --cntr);
                    }
                    catch (Exception e) {
                        error("Failed lock thread", e);
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }));
        }

        for (Thread t : threads)
            t.start();

        for (Thread t : threads)
            t.join();

        int cntr = c.get(CNTR_KEY);

        assertEquals(0, cntr);

        for (int i = 0; i < THREAD_CNT; i++)
            assertNull(c.get(Integer.toString(i)));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testLockAndTx1() throws Exception {
        final IgniteCache<String, Integer> c = grid(0).cache(null);

        final IgniteCache<Integer, Integer> c1 = grid(0).cache(null);

        Collection<Thread> threads = new LinkedList<>();

        c.put(CNTR_KEY, 0);

        for (int i = 0; i < THREAD_CNT; i++) {
            info("*** Init lock thread: " + i);

            threads.add(new Thread(new Runnable() {
                @Override public void run() {

                    Lock lock = c.lock(CNTR_KEY);

                    try {
                        lock.lock();

                        int cntr = c.get(CNTR_KEY);

                        info("*** Cntr in lock thread: " + cntr);

                        Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, READ_COMMITTED);

                        try {

                            Map<Integer, Integer> data = new HashMap<>();

                            for (int i = 0; i < RETRIES; i++) {
                                int val = globalCntr.getAndIncrement();

                                data.put(val, val);
                            }

                            c1.putAll(data);

                            tx.commit();
                        }
                        catch (IgniteException e) {
                            error("Failed tx thread", e);
                        }

                        c.put(CNTR_KEY, ++cntr);
                    }
                    catch (Exception e) {
                        error("Failed lock thread", e);
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }));
        }

        for (Thread t : threads)
            t.start();

        for (Thread t : threads)
            t.join();

        int cntr = c.get(CNTR_KEY);

        assertEquals(THREAD_CNT, cntr);

        for (int i = 0; i < globalCntr.get(); i++)
            assertNotNull(c1.get(i));
    }
}