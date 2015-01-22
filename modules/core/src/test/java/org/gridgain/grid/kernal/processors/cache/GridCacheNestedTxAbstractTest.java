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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

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

            assert grid(i).cache(null).isEmpty();
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
        final GridCache<String, Integer> c = grid(0).cache(null);

        GridKernalContext ctx = ((GridKernal)grid(0)).context();

        c.put(CNTR_KEY, 0);

        for (int i = 0; i < 10; i++) {
            try (IgniteTx tx = c.txStart(PESSIMISTIC, REPEATABLE_READ)) {
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
        final IgniteCache<String, Integer> c = grid(0).jcache(null);

        Collection<Thread> threads = new LinkedList<>();

        c.put(CNTR_KEY, 0);

        for (int i = 0; i < THREAD_CNT; i++) {
            info("*** Init tx thread: " + i);

            threads.add(new Thread(new Runnable() {
                @Override public void run() {
                    IgniteTx tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

                    try {
                        int cntr = c.get(CNTR_KEY);

                        info("*** Cntr in tx thread: " + cntr);

                        c.put(CNTR_KEY, ++cntr);

                        tx.commit();
                    }
                    catch (IgniteCheckedException e) {
                        error("Failed tx thread", e);
                    }
                }
            }));
        }

        for (int i = 0; i < THREAD_CNT; i++) {
            info("*** Init lock thread: " + i);

            threads.add(new Thread(new Runnable() {
                @Override public void run() {

                    try {
                        c.lock(CNTR_KEY).lock();

                        int cntr = c.get(CNTR_KEY);

                        info("*** Cntr in lock thread: " + cntr);

                        c.put(CNTR_KEY, --cntr);
                    }
                    catch (Exception e) {
                        error("Failed lock thread", e);
                    }
                    finally {
                        c.lock(CNTR_KEY).unlock();
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
        final IgniteCache<String, Integer> c = grid(0).jcache(null);

        final IgniteCache<Integer, Integer> c1 = grid(0).jcache(null);

        Collection<Thread> threads = new LinkedList<>();

        c.put(CNTR_KEY, 0);

        for (int i = 0; i < THREAD_CNT; i++) {
            info("*** Init lock thread: " + i);

            threads.add(new Thread(new Runnable() {
                @Override public void run() {

                    try {
                        c.lock(CNTR_KEY).lock();

                        int cntr = c.get(CNTR_KEY);

                        info("*** Cntr in lock thread: " + cntr);

                        IgniteTx tx = grid(0).transactions().txStart(OPTIMISTIC, READ_COMMITTED);

                        try {

                            Map<Integer, Integer> data = new HashMap<>();

                            for (int i = 0; i < RETRIES; i++) {
                                int val = globalCntr.getAndIncrement();

                                data.put(val, val);
                            }

                            c1.putAll(data);

                            tx.commit();
                        }
                        catch (IgniteCheckedException e) {
                            error("Failed tx thread", e);
                        }

                        c.put(CNTR_KEY, ++cntr);
                    }
                    catch (Exception e) {
                        error("Failed lock thread", e);
                    }
                    finally {
                        c.lock(CNTR_KEY).unlock();
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
