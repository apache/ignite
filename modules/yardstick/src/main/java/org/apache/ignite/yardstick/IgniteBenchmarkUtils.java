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

package org.apache.ignite.yardstick;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.yardstick.cache.IgnitePutBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriver;
import org.yardstickframework.BenchmarkDriverStartUp;
import org.yardstickframework.BenchmarkUtils;

/**
 * Utils.
 */
public class IgniteBenchmarkUtils {
    /** Preload logger. */
    private static PreloadLogger plgr;

    /**
     * Executor for printing logs.
     */
    private static ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();

    /**
     * Benchmark configuration.
     */
    private static BenchmarkConfiguration cfg;

    /**
     * Utility class constructor.
     */
    private IgniteBenchmarkUtils() {
        // No-op.
    }

    /**
     * @param igniteTx Ignite transaction.
     * @param txConcurrency Transaction concurrency.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws Exception If failed.
     */
    public static <T> T doInTransaction(IgniteTransactions igniteTx, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation,  Callable<T> clo) throws Exception {
        while (true) {
            try (Transaction tx = igniteTx.txStart(txConcurrency, txIsolation)) {
                T res = clo.call();

                tx.commit();

                return res;
            }
            catch (CacheException e) {
                if (e.getCause() instanceof ClusterTopologyException) {
                    ClusterTopologyException topEx = (ClusterTopologyException)e.getCause();

                    topEx.retryReadyFuture().get();
                }
                else
                    throw e;
            }
            catch (ClusterTopologyException e) {
                e.retryReadyFuture().get();
            }
            catch (TransactionRollbackException | TransactionOptimisticException ignore) {
                // Safe to retry right away.
            }
        }
    }

    /**
     * Starts nodes/driver in single JVM for quick benchmarks testing.
     *
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final String cfg = "modules/yardstick/config/ignite-localhost-config.xml";

        final Class<? extends BenchmarkDriver> benchmark = IgnitePutBenchmark.class;

        final int threads = 1;

        final boolean clientDriverNode = true;

        final int extraNodes = 4;

        final int warmUp = 5;
        final int duration = 5;

        final int range = 100_000;

        final boolean throughputLatencyProbe = false;

        for (int i = 0; i < extraNodes; i++) {
            IgniteConfiguration nodeCfg = Ignition.loadSpringBean(cfg, "grid.cfg");

            nodeCfg.setGridName("node-" + i);
            nodeCfg.setMetricsLogFrequency(0);

            Ignition.start(nodeCfg);
        }

        ArrayList<String> args0 = new ArrayList<>();

        addArg(args0, "-t", threads);
        addArg(args0, "-w", warmUp);
        addArg(args0, "-d", duration);
        addArg(args0, "-r", range);
        addArg(args0, "-dn", benchmark.getSimpleName());
        addArg(args0, "-sn", "IgniteNode");
        addArg(args0, "-cfg", cfg);

        if (throughputLatencyProbe)
            addArg(args0, "-pr", "ThroughputLatencyProbe");

        if (clientDriverNode)
            args0.add("-cl");

        BenchmarkDriverStartUp.main(args0.toArray(new String[args0.size()]));
    }

    /**
     * @param args Arguments.
     * @param arg Argument name.
     * @param val Argument value.
     */
    private static void addArg(List<String> args, String arg, Object val) {
        args.add(arg);
        args.add(val.toString());
    }

    /**
     * Prints non-system cache sizes during preloading.
     * @param node Ignite node.
     * @param cnfg Benchmark configuration.
     * @param logsInterval Time interval in milliseconds between printing logs.
     */
    public static void printLogs(IgniteNode node, BenchmarkConfiguration cnfg, long logsInterval){
        plgr = new PreloadLogger(node);
        cfg = cnfg;
        exec.scheduleWithFixedDelay(plgr, 0L, logsInterval, TimeUnit.MILLISECONDS);
        BenchmarkUtils.println(cfg, "Preloading log started.");
    }

    /**
     * Terminates printing log.
     * @throws Exception if failed.
     */
    public static void stopPrint() throws Exception {
        plgr.run();
        exec.awaitTermination(1, TimeUnit.SECONDS);
        exec.shutdownNow();
        BenchmarkUtils.println(cfg, "Preloading log finished.");
    }

    /**
     * Helper inner class.
     */
    private static class PreloadLogger implements Runnable{

        /**
         * List of caches whose size to be printed during preload.
         */
        private Collection<IgniteCache<Object, Object>> caches;

        /**
         * Map for keeping previous values to make sure all the caches are working correctly.
         */
        private Map<String, Long> cntrs;

        /**
         * String template used in String.format() to make output readable.
         */
        private String strFmt;

        /**
         * String template used in String.format() to make output readable.
         */

        PreloadLogger(IgniteNode node){
            this.caches = new ArrayList<>();
            this.cntrs = new HashMap<>();
            init(node);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            for (IgniteCache<Object, Object> cache : caches) {
                String cacheName = cache.getName();

                long cacheSize = cache.sizeLong();

                long recentlyLoaded = cacheSize - cntrs.get(cacheName);
                String recLoaded = recentlyLoaded == 0 ?  "" + recentlyLoaded : "+" + recentlyLoaded;

                BenchmarkUtils.println(cfg, String.format(strFmt, cacheName, cacheSize, recLoaded));

                cntrs.put(cacheName, cacheSize);
            }
        }

        /**
         * Helper method for initializing the cache list and the counters map.
         * @param node Ignite node.
         */
        private void init(IgniteNode node) {
            int longestName = 0;
            for (String cacheName : node.ignite().cacheNames()) {
                IgniteCache<Object, Object> cache = node.ignite().cache(cacheName);
                caches.add(cache);

                // Set up an initial values to the map
                cntrs.put(cache.getName(), 0L);

                //Find out the length of the longest cache name
                longestName = Math.max(cache.getName().length(), longestName);
            }

            // Should look like "Preloading:%-20s%-8d\t(%s)"
            strFmt = "Preloading:%-" + (longestName + 4) + "s%-8d\t(%s)";
        }
    }
}
