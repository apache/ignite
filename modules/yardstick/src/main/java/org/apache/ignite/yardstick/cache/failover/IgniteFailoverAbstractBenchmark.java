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
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.Utils;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs long running failover tasks.
 */
public abstract class IgniteFailoverAbstractBenchmark<K, V> extends IgniteCacheAbstractBenchmark<K, V> {
    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (cfg.memberId() == 0) {
            Thread restarterThread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        println("[RESTARTER] Servers restarter started. Will start restarting servers after "
                            + cfg.warmup() + " sec. warmup.");

                        Thread.sleep(cfg.warmup() * 1000);

                        // Read servers configs from cache to local map.
                        IgniteCache<Integer, BenchmarkConfiguration> srvsCfgsCache = ignite().
                            getOrCreateCache(new CacheConfiguration<Integer, BenchmarkConfiguration>().
                                setName("serversConfigs"));

                        final Map<Integer, BenchmarkConfiguration> srvsCfgs = new HashMap<>();

                        for (Cache.Entry<Integer, BenchmarkConfiguration> e : srvsCfgsCache) {
                            println("[RESTARTER] Read entry from 'serversConfigs' cache = " + e);

                            srvsCfgs.put(e.getKey(), e.getValue());
                        }

                        assert ignite().cluster().forServers().nodes().size() == srvsCfgs.size();

                        final int backupsCnt = args.backups();

                        assert backupsCnt >= 1 : "Backups=" + backupsCnt;

                        final boolean isDebug = ignite().log().isDebugEnabled();

                        // Main logic.
                        while (!Thread.currentThread().isInterrupted()) {
                            Thread.sleep(args.restartDelay() * 1000);

                            int numNodesToRestart = nextRandom(1, backupsCnt + 1);

                            List<Integer> ids = new ArrayList<>();

                            ids.addAll(srvsCfgs.keySet());

                            Collections.shuffle(ids);

                            println("[RESTARTER] Number nodes to restart = " + numNodesToRestart + ", shuffled ids = " + ids);

                            for (int i = 0; i < numNodesToRestart; i++) {
                                Integer id = ids.get(i);

                                BenchmarkConfiguration bc = srvsCfgs.get(id);

                                Utils.ProcessExecutionResult res = Utils.kill9Server(bc, isDebug);

                                println("[RESTARTER] Server with id " + id + " has been killed."
                                    + (isDebug ? " Result:\n" + res : ""));
                            }

                            Thread.sleep(args.restartSleep() * 1000);

                            for (int i = 0; i < numNodesToRestart; i++) {
                                Integer id = ids.get(i);

                                BenchmarkConfiguration bc = srvsCfgs.get(id);

                                Utils.ProcessExecutionResult res = Utils.startServer(bc, isDebug);

                                println("[RESTARTER] Server with id " + id + " has been started."
                                    + (isDebug ? " Result:\n" + res : ""));
                            }
                        }
                    }
                    catch (Throwable e) {
                        println("[RESTARTER] Got exception: " + e);
                        e.printStackTrace();

                        println(Utils.threadDump());

                        if (e instanceof Error)
                            throw (Error)e;
                    }
                }
            }, "restarter");

            Thread threadDumpPrinterThread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            Thread.sleep(30 * 60 * 1000);

                            println(Utils.threadDump());
                        }
                    }
                    catch (Throwable e) {
                        println("[Thread dump printer] Got exception: " + e);
                        e.printStackTrace();

                        println(Utils.threadDump());

                        if (e instanceof Error)
                            throw (Error)e;
                    }
                }
            }, "thread-dump-printer");

            restarterThread.setDaemon(true);
            threadDumpPrinterThread.setDaemon(true);

            restarterThread.start();
            threadDumpPrinterThread.start();
        }
    }
}
