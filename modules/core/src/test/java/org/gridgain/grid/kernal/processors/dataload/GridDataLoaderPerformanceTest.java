/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dataload;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import java.util.concurrent.*;

import static org.apache.ignite.events.GridEventType.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Data loader performance test. Compares group lock data loader to traditional lock.
 * <p>
 * Disable assertions and give at least 2 GB heap to run this test.
 */
public class GridDataLoaderPerformanceTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final int ENTRY_CNT = 80000;

    /** */
    private boolean useCache;

    /** */
    private boolean useGrpLock;

    /** */
    private String[] vals = new String[2048];

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        cfg.setIncludeProperties();

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        cfg.setRestEnabled(false);

        cfg.setPeerClassLoadingEnabled(true);

        if (useCache) {
            GridCacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(PARTITIONED);

            cc.setDistributionMode(PARTITIONED_ONLY);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setStartSize(ENTRY_CNT / GRID_CNT);
            cc.setSwapEnabled(false);

            cc.setBackups(1);

            cc.setStoreValueBytes(true);

            cfg.setCacheSanityCheckEnabled(false);
            cfg.setCacheConfiguration(cc);
        }
        else
            cfg.setCacheConfiguration();

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < vals.length; i++) {
            int valLen = ThreadLocalRandom8.current().nextInt(128, 512);

            StringBuilder sb = new StringBuilder();

            for (int j = 0; j < valLen; j++)
                sb.append('a' + ThreadLocalRandom8.current().nextInt(20));

            vals[i] = sb.toString();

            info("Value: " + vals[i]);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPerformance() throws Exception {
        useGrpLock = false;

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPerformanceGroupLock() throws Exception {
        useGrpLock = true;

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        System.gc();
        System.gc();
        System.gc();

        try {
            useCache = true;

            startGridsMultiThreaded(GRID_CNT);

            useCache = false;

            Ignite ignite = startGrid();

            final IgniteDataLoader<Integer, String> ldr = ignite.dataLoader(null);

            ldr.perNodeBufferSize(8192);
            ldr.updater(useGrpLock ? GridDataLoadCacheUpdaters.<Integer, String>groupLocked() :
                GridDataLoadCacheUpdaters.<Integer, String>batchedSorted());
            ldr.autoFlushFrequency(0);

            final LongAdder cnt = new LongAdder();

            long start = U.currentTimeMillis();

            Thread t = new Thread(new Runnable() {
                @SuppressWarnings("BusyWait")
                @Override public void run() {
                    while (true) {
                        try {
                            Thread.sleep(10000);
                        }
                        catch (InterruptedException ignored) {
                            break;
                        }

                        info(">>> Adds/sec: " + cnt.sumThenReset() / 10);
                    }
                }
            });

            t.setDaemon(true);

            t.start();

            int threadNum = 2;//Runtime.getRuntime().availableProcessors();

            multithreaded(new Callable<Object>() {
                @SuppressWarnings("InfiniteLoopStatement")
                @Override public Object call() throws Exception {
                    ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                    while (true) {
                        int i = rnd.nextInt(ENTRY_CNT);

                        ldr.addData(i, vals[rnd.nextInt(vals.length)]);

                        cnt.increment();
                    }
                }
            }, threadNum, "loader");

            info("Closing loader...");

            ldr.close(false);

            long duration = U.currentTimeMillis() - start;

            info("Finished performance test. Duration: " + duration + "ms.");
        }
        finally {
            stopAllGrids();
        }
    }
}
