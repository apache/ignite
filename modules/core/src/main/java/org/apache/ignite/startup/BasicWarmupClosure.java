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

package org.apache.ignite.startup;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.logger.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.text.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Basic warm-up closure which warm-ups cache operations.
 */
public class BasicWarmupClosure implements IgniteInClosure<IgniteConfiguration> {
    /** */
    private static final long serialVersionUID = 9175346848249957458L;

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
    @Override public void apply(IgniteConfiguration gridCfg) {
        // Remove cache duplicates, clean up the rest, etc.
        IgniteConfiguration cfg = prepareConfiguration(gridCfg);

        // Do nothing if no caches found.
        if (cfg == null)
            return;

        out("Starting grids to warmup caches [gridCnt=" + gridCnt +
            ", caches=" + cfg.getCacheConfiguration().length + ']');

        Collection<Ignite> ignites = new LinkedList<>();

        String old = System.getProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER);

        try {
            System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");

            TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

            for (int i = 0; i < gridCnt; i++) {
                IgniteConfiguration cfg0 = new IgniteConfiguration(cfg);

                TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

                discoSpi.setIpFinder(ipFinder);

                discoSpi.setLocalPort(discoveryPort);

                cfg0.setDiscoverySpi(discoSpi);

                cfg0.setGridLogger(new NullLogger());

                cfg0.setGridName("ignite-warmup-grid-" + i);

                ignites.add(Ignition.start(cfg0));
            }

            doWarmup(ignites);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
        finally {
            for (Ignite ignite : ignites)
                Ignition.stop(ignite.name(), false);

            out("Stopped warmup grids.");

            if (old == null)
                old = "false";

            System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, old);
        }
    }

    /**
     * @param grids Grids to warmup.
     */
    private void doWarmup(Iterable<Ignite> grids) throws Exception {
        Ignite first = F.first(grids);

        ExecutorService svc = Executors.newFixedThreadPool(threadCnt);

        try {
            for (CacheConfiguration cc : first.configuration().getCacheConfiguration()) {
                GridCache<Object, Object> cache0 = ((IgniteKernal)first).getCache(cc.getName());

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
                                throw new IgniteCheckedException("Unsupported warmup method: " + warmupMethod);
                        }

                        futs.add(svc.submit(call));
                    }

                    out("Running warmup [cacheName=" + cc.getName() + ", method=" + warmupMethod + ']');

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
    private IgniteConfiguration prepareConfiguration(IgniteConfiguration gridCfg) {
        if (F.isEmpty(gridCfg.getCacheConfiguration()))
            return null;

        IgniteConfiguration cp = new IgniteConfiguration();

        cp.setConnectorConfiguration(null);

        Collection<CacheConfiguration> reduced = new ArrayList<>();

        for (CacheConfiguration ccfg : gridCfg.getCacheConfiguration()) {
            if (CU.isSystemCache(ccfg.getName()))
                continue;

            if (!matches(reduced, ccfg)) {
                CacheConfiguration ccfgCp = new CacheConfiguration(ccfg);

                ccfgCp.setCacheStoreFactory(null);
                ccfgCp.setWriteBehindEnabled(false);

                reduced.add(ccfgCp);
            }
        }

        if (F.isEmpty(reduced))
            return null;

        CacheConfiguration[] res = new CacheConfiguration[reduced.size()];

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
    private boolean matches(Iterable<CacheConfiguration> reduced, CacheConfiguration ccfg) {
        for (CacheConfiguration ccfg0 : reduced) {
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
    private boolean matches(CacheConfiguration ccfg0, CacheConfiguration ccfg1) {
        return
            F.eq(ccfg0.getCacheMode(), ccfg1.getCacheMode()) &&
            F.eq(ccfg0.getBackups(), ccfg1.getBackups()) &&
            F.eq(ccfg0.getAtomicityMode(), ccfg1.getAtomicityMode()) &&
            F.eq(ccfg0.getAtomicWriteOrderMode(), ccfg1.getAtomicWriteOrderMode()) &&
            F.eq(ccfg0.getMemoryMode(), ccfg1.getMemoryMode());
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

        /** {@inheritDoc} */
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
         * @throws Exception If failed.
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
