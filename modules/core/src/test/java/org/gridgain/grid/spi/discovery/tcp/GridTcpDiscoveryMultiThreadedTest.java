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
import org.gridgain.grid.spi.discovery.tcp.metricsstore.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test for {@link GridTcpDiscoverySpi}.
 */
public class GridTcpDiscoveryMultiThreadedTest extends GridCommonAbstractTest {
    /** */
    public static final int GRID_CNT = 1;

    /** */
    public static final int CLIENT_GRID_CNT = 15;

    /** */
    public static final int THREAD_CNT = 14;

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

    /** */
    private GridTcpDiscoveryVmMetricsStore metricsStore = new GridTcpDiscoveryVmMetricsStore();

    /** */
    private boolean useMetricsStore;

    /**
     * @throws Exception If fails.
     */
    public GridTcpDiscoveryMultiThreadedTest() throws Exception {
        super(false);

        metricsStore.setMetricsExpireTime(2000);
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

            if (useMetricsStore)
                spi.setMetricsStore(metricsStore);

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
    public void testMetricsStoreMultiThreaded() throws Exception {
        useMetricsStore = true;

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
    @SuppressWarnings({"RedundantCast"})
    private void execute() throws Exception {
        info("Test timeout: " + (getTestTimeout() / (60 * 1000)) + " min.");

        startGridsMultiThreaded(GRID_CNT);

        clientFlagGlobal = true;

//        startGridsMultiThreaded(GRID_CNT, CLIENT_GRID_CNT);
        for (int i = GRID_CNT; i < GRID_CNT + CLIENT_GRID_CNT; i++)
            startGrid(i);

        final AtomicBoolean done = new AtomicBoolean();
        final AtomicBoolean done0 = new AtomicBoolean();

        final AtomicInteger idx = new AtomicInteger();

        final List<Integer> idxs = new ArrayList<>();

        for (int i = GRID_CNT; i < GRID_CNT + CLIENT_GRID_CNT; i++)
            idxs.add(i);

        final CyclicBarrier barrier = new CyclicBarrier(THREAD_CNT, new Runnable() {
            @Override public void run() {
                if (done0.get())
                    done.set(true);

                Collections.shuffle(idxs);

                idx.set(GRID_CNT);
            }
        });

        GridFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (true) {
                        barrier.await();

                        if (done.get())
                            break;

                        int i = idxs.get(idx.getAndIncrement());

                        stopGrid(i);

                        clientFlagPerThread.set(i >= GRID_CNT);

                        startGrid(i);
                    }

                    info("Thread finished.");

                    return null;
                }
            },
            THREAD_CNT
        );

        // Duration = test timeout - 1 min.
        Thread.sleep(getTestTimeout() - 60 * 1000);

        done0.set(true);

        fut.get();
    }
}
