/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
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
    /** */
    private static final AtomicBoolean restarterStarted = new AtomicBoolean();

    /** */
    private final AtomicBoolean firtsExProcessed = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onWarmupFinished() {
        if (cfg.memberId() == 0 && restarterStarted.compareAndSet(false, true)) {
            Thread restarterThread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        println("Servers restarter started on driver: "
                            + IgniteFailoverAbstractBenchmark.this.getClass().getSimpleName());

                        Ignite ignite = ignite();

                        // Read servers configs from cache to local map.
                        IgniteCache<Integer, BenchmarkConfiguration> srvsCfgsCache = ignite.
                            getOrCreateCache(new CacheConfiguration<Integer, BenchmarkConfiguration>().
                                setName("serversConfigs"));

                        final Map<Integer, BenchmarkConfiguration> srvsCfgs = new HashMap<>();

                        for (Cache.Entry<Integer, BenchmarkConfiguration> e : srvsCfgsCache) {
                            println("Read entry from 'serversConfigs' cache : " + e);

                            srvsCfgs.put(e.getKey(), e.getValue());
                        }

                        assert ignite.cluster().forServers().nodes().size() == srvsCfgs.size();

                        final int backupsCnt = args.backups();

                        assert backupsCnt >= 1 : "Backups: " + backupsCnt;

                        final boolean isDebug = ignite.log().isDebugEnabled();

                        // Main logic.
                        while (!Thread.currentThread().isInterrupted()) {
                            Thread.sleep(args.restartDelay() * 1000);

                            int numNodesToRestart = nextRandom(1, backupsCnt + 1);

                            List<Integer> ids = new ArrayList<>();

                            ids.addAll(srvsCfgs.keySet());

                            Collections.shuffle(ids);

                            println("Waiting for partitioned map exchage of all nodes");

                            ignite.compute().broadcastAsync(new AwaitPartitionMapExchangeTask())
                                .get(args.cacheOperationTimeoutMillis());

                            println("Start servers restarting [numNodesToRestart=" + numNodesToRestart
                                + ", shuffledIds=" + ids + "]");

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

    /**
     * Awaits for partitiona map exchage.
     *
     * @param ignite Ignite.
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    protected static void awaitPartitionMapExchange(Ignite ignite) throws Exception {
        IgniteLogger log = ignite.log();

        log.info("Waiting for finishing of a partition exchange on node: " + ignite);

        IgniteKernal kernal = (IgniteKernal)ignite;

        while (true) {
            boolean partitionsExchangeFinished = true;

            for (IgniteInternalCache<?, ?> cache : kernal.cachesx(null)) {
                log.info("Checking cache: " + cache.name());

                GridCacheAdapter<?, ?> c = kernal.internalCache(cache.name());

                if (!(c instanceof GridDhtCacheAdapter))
                    break;

                GridDhtCacheAdapter<?, ?> dht = (GridDhtCacheAdapter<?, ?>)c;

                GridDhtPartitionFullMap partMap = dht.topology().partitionMap(true);

                for (Map.Entry<UUID, GridDhtPartitionMap> e : partMap.entrySet()) {
                    log.info("Checking node: " + e.getKey());

                    for (Map.Entry<Integer, GridDhtPartitionState> e1 : e.getValue().entrySet()) {
                        if (e1.getValue() != GridDhtPartitionState.OWNING) {
                            log.info("Undesired state [id=" + e1.getKey() + ", state=" + e1.getValue() + ']');

                            partitionsExchangeFinished = false;

                            break;
                        }
                    }

                    if (!partitionsExchangeFinished)
                        break;
                }

                if (!partitionsExchangeFinished)
                    break;
            }

            if (partitionsExchangeFinished)
                return;

            Thread.sleep(100);
        }
    }

    /** {@inheritDoc} */
    @Override public void onException(Throwable e) {
        // Proceess only the first exception to prevent a multiple printing of a full thread dump.
        if (firtsExProcessed.compareAndSet(false, true)) {
            // Debug info on current client.
            println("Full thread dump of the current node below.");

            U.dumpThreads(null);

            println("");

            ((IgniteMXBean)ignite()).dumpDebugInfo();

            // Debug info on servers.
            Ignite ignite = ignite();

            ClusterGroup srvs = ignite.cluster().forServers();

            ignite.compute(srvs).broadcastAsync(new ThreadDumpPrinterTask(ignite.cluster().localNode().id(), e))
                .get(10_000);
        }
    }

    /**
     * @return Cache name.
     */
    protected abstract String cacheName();

    /** {@inheritDoc} */
    @Override protected IgniteCache<K, V> cache() {
        return ignite().cache(cacheName());
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
            println("Driver finished with exception [driverNodeId=" + id + ", e=" + e + "]");
            println("Full thread dump of the current server node below.");

            U.dumpThreads(null);

            println("");

            ((IgniteMXBean)ignite).dumpDebugInfo();
        }
    }

    /**
     */
    private static class AwaitPartitionMapExchangeTask implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                awaitPartitionMapExchange(ignite);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }
}
