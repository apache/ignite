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

package org.apache.ignite.yardstick.cache.failover;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;
import org.yardstickframework.BenchmarkUtils.ProcessExecutionResult;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs long running failover tasks.
 */
public abstract class IgniteFailoverAbstractBenchmark<K, V> extends IgniteCacheAbstractBenchmark<K, V> {
    /** Async Cache. */
    protected IgniteCache<K, V> asyncCache;

    /** */
    private final AtomicBoolean firtsExProcessed = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        asyncCache = cache.withAsync();

        if (cfg.memberId() == 0) {
            Thread restarterThread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        println("Servers restarter started. Will start restarting servers after "
                            + cfg.warmup() + " sec. warmup.");

                        Thread.sleep(cfg.warmup() * 1000);

                        // Read servers configs from cache to local map.
                        IgniteCache<Integer, BenchmarkConfiguration> srvsCfgsCache = ignite().
                            getOrCreateCache(new CacheConfiguration<Integer, BenchmarkConfiguration>().
                                setName("serversConfigs"));

                        final Map<Integer, BenchmarkConfiguration> srvsCfgs = new HashMap<>();

                        for (Cache.Entry<Integer, BenchmarkConfiguration> e : srvsCfgsCache) {
                            println("Read entry from 'serversConfigs' cache : " + e);

                            srvsCfgs.put(e.getKey(), e.getValue());
                        }

                        assert ignite().cluster().forServers().nodes().size() == srvsCfgs.size();

                        final int backupsCnt = args.backups();

                        assert backupsCnt >= 1 : "Backups: " + backupsCnt;

                        final boolean isDebug = ignite().log().isDebugEnabled();

                        // Main logic.
                        while (!Thread.currentThread().isInterrupted()) {
                            Thread.sleep(args.restartDelay() * 1000);

                            int numNodesToRestart = nextRandom(1, backupsCnt + 1);

                            List<Integer> ids = new ArrayList<>();

                            ids.addAll(srvsCfgs.keySet());

                            Collections.shuffle(ids);

                            println("Start servers restarting [numNodesToRestart=" + numNodesToRestart
                                + ", shuffledIds = " + ids + "]");

                            for (int i = 0; i < numNodesToRestart; i++) {
                                Integer id = ids.get(i);

                                BenchmarkConfiguration bc = srvsCfgs.get(id);

                                ProcessExecutionResult res = BenchmarkUtils.kill9Server(bc, isDebug);

                                println("Server with id " + id + " has been killed."
                                    + (isDebug ? " Process execution result:\n" + res : ""));
                            }

                            Thread.sleep(args.restartSleep() * 1000);

                            for (int i = 0; i < numNodesToRestart; i++) {
                                Integer id = ids.get(i);

                                BenchmarkConfiguration bc = srvsCfgs.get(id);

                                ProcessExecutionResult res = BenchmarkUtils.startServer(bc, isDebug);

                                println("Server with id " + id + " has been started."
                                    + (isDebug ? " Process execution result:\n" + res : ""));
                            }
                        }
                    }
                    catch (Throwable e) {
                        println("Got exception: " + e);
                        e.printStackTrace();

                        U.dumpThreads(null);

                        if (e instanceof Error)
                            throw (Error)e;
                    }
                }
            }, "servers-restarter");

            restarterThread.setDaemon(true);
            restarterThread.start();
        }
    }

    /** {@inheritDoc} */
    @Override public void onException(Throwable e) {
        // Proceess only the first exception to prevent a multiple printing of a full thread dump.
        if (firtsExProcessed.compareAndSet(false, true)) {
            // Debug info on servers.
            Ignite ignite = ignite();

            ClusterGroup srvs = ignite.cluster().forServers();

            IgniteCompute asyncCompute = ignite.compute(srvs).withAsync();

            asyncCompute.broadcast(new ThreadDumpPrinterTask(ignite.cluster().localNode().id(), e));
            asyncCompute.future().get(10_000);

            // Debug info on current client.
            println("Full thread dump of the current node below.");

            U.dumpThreads(null);

            println("");

            ((IgniteMXBean)ignite()).dumpDebugInfo();
        }
    }

    /**
     */
    private static class ThreadDumpPrinterTask implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final UUID id;

        /** */
        private final Throwable e;

        /**
         * @param id Benchmark node id.
         * @param e Exception.
         */
        ThreadDumpPrinterTask(UUID id, Throwable e) {
            this.id = id;
            this.e = e;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            println("Driver finished with exception [driverNodeId=" + id + ", e=" +  e + "]");
            println("Full thread dump of the current server node below.");

            U.dumpThreads(null);

            println("");

            ((IgniteMXBean)ignite).dumpDebugInfo();
        }
    }
}
