/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Nested transaction emulation.
 */
public class GridCacheNestedTxAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

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
            try (GridCacheTx tx = c.txStart(PESSIMISTIC, REPEATABLE_READ)) {
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
        final GridCache<String, Integer> c = grid(0).cache(null);

        Collection<Thread> threads = new LinkedList<>();

        c.put(CNTR_KEY, 0);

        for (int i = 0; i < THREAD_CNT; i++) {
            info("*** Init tx thread: " + i);

            threads.add(new Thread(new Runnable() {
                @Override public void run() {
                    GridCacheTx tx = c.txStart(PESSIMISTIC, REPEATABLE_READ);

                    try {
                        int cntr = c.get(CNTR_KEY);

                        info("*** Cntr in tx thread: " + cntr);

                        c.put(CNTR_KEY, ++cntr);

                        tx.commit();
                    }
                    catch (GridException e) {
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
                        c.lock(CNTR_KEY, 0);

                        int cntr = c.get(CNTR_KEY);

                        info("*** Cntr in lock thread: " + cntr);

                        c.put(CNTR_KEY, --cntr);
                    }
                    catch (Exception e) {
                        error("Failed lock thread", e);
                    }
                    finally {
                        try {
                            c.unlock(CNTR_KEY);
                        }
                        catch (GridException e) {
                            error("Failed unlock", e);
                        }
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
        final GridCache<String, Integer> c = grid(0).cache(null);

        final GridCache<Integer, Integer> c1 = grid(0).cache(null);

        Collection<Thread> threads = new LinkedList<>();

        c.put(CNTR_KEY, 0);

        for (int i = 0; i < THREAD_CNT; i++) {
            info("*** Init lock thread: " + i);

            threads.add(new Thread(new Runnable() {
                @Override public void run() {

                    try {
                        c.lock(CNTR_KEY, 0);

                        int cntr = c.get(CNTR_KEY);

                        info("*** Cntr in lock thread: " + cntr);

                        GridCacheTx tx = c.txStart(OPTIMISTIC, READ_COMMITTED);

                        try {

                            Map<Integer, Integer> data = new HashMap<>();

                            for (int i = 0; i < RETRIES; i++) {
                                int val = globalCntr.getAndIncrement();

                                data.put(val, val);
                            }

                            c1.putAll(data);

                            tx.commit();
                        }
                        catch (GridException e) {
                            error("Failed tx thread", e);
                        }

                        c.put(CNTR_KEY, ++cntr);
                    }
                    catch (Exception e) {
                        error("Failed lock thread", e);
                    }
                    finally {
                        try {
                            c.unlock(CNTR_KEY);
                        }
                        catch (GridException e) {
                            error("Failed unlock", e);
                        }
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
