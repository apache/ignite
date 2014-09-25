/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.text.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Basic warm-up closure which warm-ups cache operations.
 */
public class GridBasicWarmupClosure implements GridInClosure<GridConfiguration> {
    /** Default grid count to warm up. */
    public static final int DFLT_GRID_CNT = 2;

    /** Default iteration count per thread. */
    public static final int DFLT_ITERATION_CNT = 30_000;

    /** Default key range. */
    public static final int DFLT_KEY_RANGE = 10_000;

    /** Grid count. */
    private int gridCnt = DFLT_GRID_CNT;

    /** Warmup date format. */
    private static final SimpleDateFormat WARMUP_DATE_FMT = new SimpleDateFormat("HH:mm:ss");

    /** Warmup thread count. */
    private int threadCnt = Runtime.getRuntime().availableProcessors() * 2;

    /** Per thread iteration count. */
    private int iterCnt = DFLT_ITERATION_CNT;

    /** Key range. */
    private int keyRange = DFLT_KEY_RANGE;

    /** Warmup discovery port. */
    private int discoveryPort = 27000;

    /** Methods to warmup. */
    private String[] warmupMethods = {"put", "putx", "get", "remove", "removex", "putIfAbsent", "replace"};

    /**
     * Gets number of grids to start and run warmup.
     *
     * @return Number of grids.
     */
    public int getGridCount() {
        return gridCnt;
    }

    /**
     * Sets number of grids to start and run the warmup.
     *
     * @param gridCnt Number of grids.
     */
    public void setGridCount(int gridCnt) {
        this.gridCnt = gridCnt;
    }

    /**
     * Gets warmup methods to use for cache warmup.
     *
     * @return Warmup methods.
     */
    public String[] getWarmupMethods() {
        return warmupMethods;
    }

    /**
     * Sets warmup methods to use for cache warmup.
     *
     * @param warmupMethods Array of warmup methods.
     */
    public void setWarmupMethods(String... warmupMethods) {
        this.warmupMethods = warmupMethods;
    }

    /**
     * Gets thread count for warmup.
     *
     * @return Thread count.
     */
    public int getThreadCount() {
        return threadCnt;
    }

    /**
     * Sets thread count for warmup.
     *
     * @param threadCnt Thread count.
     */
    public void setThreadCount(int threadCnt) {
        this.threadCnt = threadCnt;
    }

    /**
     * Gets iteration count for warmup.
     *
     * @return Iteration count.
     */
    public int getIterationCount() {
        return iterCnt;
    }

    /**
     * Sets iteration count for warmup.
     *
     * @param iterCnt Iteration count for warmup.
     */
    public void setIterationCount(int iterCnt) {
        this.iterCnt = iterCnt;
    }

    /**
     * Gets key range.
     *
     * @return Key range.
     */
    public int getKeyRange() {
        return keyRange;
    }

    /**
     * Sets key range.
     *
     * @param keyRange Key range.
     */
    public void setKeyRange(int keyRange) {
        this.keyRange = keyRange;
    }

    /**
     * Gets discovery port for warmup.
     *
     * @return Discovery port.
     */
    public int getDiscoveryPort() {
        return discoveryPort;
    }

    /**
     * Sets discovery port for warmup.
     *
     * @param discoveryPort Discovery port.
     */
    public void setDiscoveryPort(int discoveryPort) {
        this.discoveryPort = discoveryPort;
    }

    /** {@inheritDoc} */
    @Override public void apply(GridConfiguration gridCfg) {
        // Remove cache duplicates, clean up the rest, etc.
        GridConfiguration cfg = prepareConfiguration(gridCfg);

        // Do nothing if no caches found.
        if (cfg == null)
            return;

        out("Starting grids to warmup caches [gridCnt=" + gridCnt +
            ", caches=" + cfg.getCacheConfiguration().length + ']');

        Collection<Grid> grids = new LinkedList<>();

        String old = System.getProperty(GridSystemProperties.GG_UPDATE_NOTIFIER);

        try {
            System.setProperty(GridSystemProperties.GG_UPDATE_NOTIFIER, "false");

            GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

            for (int i = 0; i < gridCnt; i++) {
                GridConfiguration cfg0 = new GridConfiguration(cfg);

                GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

                discoSpi.setIpFinder(ipFinder);

                discoSpi.setLocalPort(discoveryPort);

                cfg0.setDiscoverySpi(discoSpi);

                cfg0.setGridLogger(new GridNullLogger());

                cfg0.setGridName("gridgain-warmup-grid-" + i);

                grids.add(GridGain.start(cfg0));
            }

            doWarmup(grids);
        }
        catch (Exception e) {
            throw new GridRuntimeException(e);
        }
        finally {
            for (Grid grid : grids)
                GridGain.stop(grid.name(), false);

            out("Stopped warmup grids.");

            if (old == null)
                old = "false";

            System.setProperty(GridSystemProperties.GG_UPDATE_NOTIFIER, old);
        }
    }

    /**
     * @param grids Grids to warmup.
     */
    private void doWarmup(Iterable<Grid> grids) throws Exception {
        Grid first = F.first(grids);

        ExecutorService svc = Executors.newFixedThreadPool(threadCnt);

        try {
            for (GridCache<?, ?> cache : first.caches()) {
                GridCache<Object, Object> cache0 = first.cache(cache.name());

                for (String warmupMethod : warmupMethods) {
                    Collection<Future> futs = new ArrayList<>(threadCnt);

                    for (int i = 0; i < threadCnt; i++) {
                        Callable call;

                        switch (warmupMethod) {
                            case "get": {
                                call = new GetCallable(cache0);

                                break;
                            }

                            case "put": {
                                call = new PutCallable(cache0);

                                break;
                            }

                            case "putx": {
                                call = new PutxCallable(cache0);

                                break;
                            }

                            case "remove": {
                                call = new RemoveCallable(cache0);

                                break;
                            }

                            case "removex": {
                                call = new RemovexCallable(cache0);

                                break;
                            }

                            case "putIfAbsent": {
                                call = new PutIfAbsentCallable(cache0);

                                break;
                            }

                            case "replace": {
                                call = new ReplaceCallable(cache0);

                                break;
                            }

                            default:
                                throw new GridException("Unsupported warmup method: " + warmupMethod);
                        }

                        futs.add(svc.submit(call));
                    }

                    out("Running warmup [cacheName=" + cache.name() + ", method=" + warmupMethod + ']');

                    for (Future fut : futs)
                        fut.get();

                    for (int key = 0; key < keyRange; key++)
                        cache0.remove(key);
                }
            }
        }
        finally {
            svc.shutdownNow();
        }
    }

    /**
     * Output for warmup messages.
     *
     * @param msg Format message.
     */
    private static void out(String msg) {
        System.out.println('[' + WARMUP_DATE_FMT.format(new Date(System.currentTimeMillis())) + "][WARMUP][" +
            Thread.currentThread().getName() + ']' + ' ' + msg);
    }

    /**
     * Prepares configuration for warmup.
     *
     * @param gridCfg Original grid configuration.
     * @return Prepared configuration or {@code null} if no caches found.
     */
    private GridConfiguration prepareConfiguration(GridConfiguration gridCfg) {
        if (F.isEmpty(gridCfg.getCacheConfiguration()))
            return null;

        GridConfiguration cp = new GridConfiguration();

        cp.setClientConnectionConfiguration(null);

        Collection<GridCacheConfiguration> reduced = new ArrayList<>();

        for (GridCacheConfiguration ccfg : gridCfg.getCacheConfiguration()) {
            if (CU.isSystemCache(ccfg.getName()))
                continue;

            if (!matches(reduced, ccfg)) {
                GridCacheConfiguration ccfgCp = new GridCacheConfiguration(ccfg);

                if (ccfgCp.getDistributionMode() == GridCacheDistributionMode.CLIENT_ONLY)
                    ccfgCp.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
                else if (ccfgCp.getDistributionMode() == GridCacheDistributionMode.NEAR_ONLY)
                    ccfgCp.setDistributionMode(GridCacheDistributionMode.NEAR_PARTITIONED);

                ccfgCp.setStore(null);
                ccfgCp.setWriteBehindEnabled(false);

                reduced.add(ccfgCp);
            }
        }

        if (F.isEmpty(reduced))
            return null;

        GridCacheConfiguration[] res = new GridCacheConfiguration[reduced.size()];

        reduced.toArray(res);

        cp.setCacheConfiguration(res);

        return cp;
    }

    /**
     * Checks if passed configuration matches one of the configurations in the list.
     *
     * @param reduced Reduced configurations.
     * @param ccfg Cache configuration to match.
     * @return {@code True} if matching configuration is found, {@code false} otherwise.
     */
    private boolean matches(Iterable<GridCacheConfiguration> reduced, GridCacheConfiguration ccfg) {
        for (GridCacheConfiguration ccfg0 : reduced) {
            if (matches(ccfg0, ccfg))
                return true;
        }

        return false;
    }

    /**
     * Checks if cache configurations are alike for warmup.
     *
     * @param ccfg0 First configuration.
     * @param ccfg1 Second configuration.
     * @return {@code True} if configurations match.
     */
    private boolean matches(GridCacheConfiguration ccfg0, GridCacheConfiguration ccfg1) {
        return
            F.eq(ccfg0.getCacheMode(), ccfg1.getCacheMode()) &&
            F.eq(ccfg0.getBackups(), ccfg1.getBackups()) &&
            F.eq(ccfg0.getAtomicityMode(), ccfg1.getAtomicityMode()) &&
            F.eq(ccfg0.getAtomicWriteOrderMode(), ccfg1.getAtomicWriteOrderMode()) &&
            F.eq(ccfg0.getMemoryMode(), ccfg1.getMemoryMode()) &&
            F.eq(ccfg0.getDistributionMode(), ccfg1.getDistributionMode());
    }

    /**
     * Base class for all warmup callables.
     */
    private abstract class BaseWarmupCallable implements Callable<Object> {
        /** Cache. */
        protected final GridCache<Object, Object> cache;

        /**
         * @param cache Cache.
         */
        protected BaseWarmupCallable(GridCache<Object, Object> cache) {
            this.cache = cache;
        }

        @Override public Object call() throws Exception {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < iterCnt; i++)
                operation(rnd.nextInt(keyRange));

            return null;
        }

        /**
         * Runs operation.
         *
         * @param key Key.
         */
        protected abstract void operation(int key) throws Exception;
    }

    /**
     *
     */
    private class GetCallable extends BaseWarmupCallable {
        /**
         * @param cache Cache.
         */
        private GetCallable(GridCache<Object, Object> cache) {
            super(cache);
        }

        /** {@inheritDoc} */
        @Override protected void operation(int key) throws Exception {
            cache.get(key);
        }
    }

    /**
     *
     */
    private class PutCallable extends BaseWarmupCallable {
        /**
         * @param cache Cache.
         */
        private PutCallable(GridCache<Object, Object> cache) {
            super(cache);
        }

        /** {@inheritDoc} */
        @Override protected void operation(int key) throws Exception {
            cache.put(key, key);
        }
    }

    /**
     *
     */
    private class PutxCallable extends BaseWarmupCallable {
        /**
         * @param cache Cache.
         */
        private PutxCallable(GridCache<Object, Object> cache) {
            super(cache);
        }

        /** {@inheritDoc} */
        @Override protected void operation(int key) throws Exception {
            cache.putx(key, key);
        }
    }

    /**
     *
     */
    private class RemoveCallable extends BaseWarmupCallable {
        /**
         * @param cache Cache.
         */
        private RemoveCallable(GridCache<Object, Object> cache) {
            super(cache);
        }

        /** {@inheritDoc} */
        @Override protected void operation(int key) throws Exception {
            cache.remove(key);
        }
    }

    /**
     *
     */
    private class RemovexCallable extends BaseWarmupCallable {
        /**
         * @param cache Cache.
         */
        private RemovexCallable(GridCache<Object, Object> cache) {
            super(cache);
        }

        /** {@inheritDoc} */
        @Override protected void operation(int key) throws Exception {
            cache.removex(key);
        }
    }

    /**
     *
     */
    private class PutIfAbsentCallable extends BaseWarmupCallable {
        /**
         * @param cache Cache.
         */
        private PutIfAbsentCallable(GridCache<Object, Object> cache) {
            super(cache);
        }

        /** {@inheritDoc} */
        @Override protected void operation(int key) throws Exception {
            cache.putIfAbsent(key, key);
        }
    }

    /**
     *
     */
    private class ReplaceCallable extends BaseWarmupCallable {
        /**
         * @param cache Cache.
         */
        private ReplaceCallable(GridCache<Object, Object> cache) {
            super(cache);
        }

        /** {@inheritDoc} */
        @Override protected void operation(int key) throws Exception {
            cache.replace(key, key, key);
        }
    }
}
