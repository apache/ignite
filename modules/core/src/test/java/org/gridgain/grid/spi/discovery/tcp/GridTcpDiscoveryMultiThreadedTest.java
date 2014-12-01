/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test for {@link GridTcpDiscoverySpi}.
 */
public class GridTcpDiscoveryMultiThreadedTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 2;

    /** */
    private static final int CLIENT_GRID_CNT = 5;

    /** */
    private static final ThreadLocal<Boolean> clientFlagPerThread = new ThreadLocal<>();

    /** */
    private static volatile boolean clientFlagGlobal;

    /**
     * @return Client node flag.
     */
    private static boolean client() {
        Boolean client = clientFlagPerThread.get();

        return client != null ? client : clientFlagGlobal;
    }

    /** */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     * @throws Exception If fails.
     */
    public GridTcpDiscoveryMultiThreadedTest() throws Exception {
        super(false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional"})
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        if (client()) {
            GridTcpClientDiscoverySpi spi = new GridTcpClientDiscoverySpi();

            spi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(spi);
        }
        else {
            GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

            spi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(spi);
        }

        cfg.setCacheConfiguration();

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        cfg.setIncludeProperties();

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000;
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testMultiThreaded() throws Exception {
        execute();
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testTopologyVersion() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        long prev = 0;

        for (Grid g : G.allGrids()) {
            GridKernal kernal = (GridKernal)g;

            long ver = kernal.context().discovery().topologyVersion();

            info("Top ver: " + ver);

            if (prev == 0)
                prev = ver;
        }

        info("Test finished.");
    }

    /**
     * @throws Exception If failed.
     */
    private void execute() throws Exception {
        info("Test timeout: " + (getTestTimeout() / (60 * 1000)) + " min.");

        startGridsMultiThreaded(GRID_CNT);

        clientFlagGlobal = true;

        startGridsMultiThreaded(GRID_CNT, CLIENT_GRID_CNT);

        final AtomicBoolean done = new AtomicBoolean();

        final AtomicInteger clientIdx = new AtomicInteger(GRID_CNT);

        GridFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    clientFlagPerThread.set(true);

                    int idx = clientIdx.getAndIncrement();

                    while (!done.get()) {
                        stopGrid(idx);
                        startGrid(idx);
                    }

                    return null;
                }
            },
            CLIENT_GRID_CNT
        );

//        final BlockingQueue<Integer> srvIdx = new LinkedBlockingQueue<>();
//
//        for (int i = 0; i < GRID_CNT; i++)
//            srvIdx.add(i);
//
//        GridFuture<?> fut2 = multithreadedAsync(
//            new Callable<Object>() {
//                @Override public Object call() throws Exception {
//                    clientFlagPerThread.set(false);
//
//                    while (!done.get()) {
//                        int idx = srvIdx.take();
//
//                        stopGrid(idx);
//                        startGrid(idx);
//
//                        srvIdx.add(idx);
//                    }
//
//                    return null;
//                }
//            },
//            GRID_CNT - 1
//        );

        Thread.sleep(getTestTimeout() - 60 * 1000);

        done.set(true);

        fut1.get();
//        fut2.get();
    }
}
