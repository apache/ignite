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

package org.apache.ignite.internal.benchmarks.jmh.preloader;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.GCProfiler;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

@State(Scope.Benchmark)
public class JmhCachePreloaderBenchmark extends JmhAbstractBenchmark {
    /** Property: entries per message. */
    protected static final String PROP_ENTRIES_PER_MESSAGE = "ignite.jmh.preloader.entriesPerMessage";

    /** IP finder shared across nodes. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Default cache name. */
    private static final String DEFAULT_CACHE_NAME = "default";

    private int entriesPerMessage;

    private IgniteEx supplierNode;
    private IgniteEx demanderNode;

    private GridCachePreloader supplierPreloader;
    private CacheObjectContext supplierCacheObjCtx;
    private GridCacheSharedContext<Object, Object> supplierSharedCtx;
    private IgniteCacheObjectProcessor supplierCacheObjProc;
    private GridCacheContext supplierCacheCtx;

    private GridCachePreloader demanderPreloader;
    private CacheObjectContext demanderCacheObjCtx;
    private GridCacheSharedContext<Object, Object> demanderSharedCtx;
    private IgniteCacheObjectProcessor demanderCacheObjProc;
    private GridCacheContext demanderCacheCtx;

    /**
     * Setup routine. Child classes must invoke this method first.
     *
     * @throws Exception If failed.
     */
    @Setup
    public void setup() throws Exception {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));

        System.out.println();
        System.out.println("--------------------");

        System.out.println("IGNITE BENCHMARK INFO: ");

        System.out.println("--------------------");
        System.out.println();

        supplierNode = (IgniteEx) Ignition.start(configuration("supplier"));

        supplierNode.cluster().active(true);

        supplierCacheCtx = supplierNode.cachex(DEFAULT_CACHE_NAME).context();
        supplierPreloader = supplierCacheCtx.preloader();
        supplierCacheObjCtx = supplierCacheCtx.cacheObjectContext();
        supplierCacheObjProc = supplierCacheCtx.cacheObjects();
        supplierSharedCtx = supplierCacheCtx.shared();

        entriesPerMessage = intProperty(PROP_ENTRIES_PER_MESSAGE);

        IgniteCache<Integer, Integer> cache = supplierNode.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < entriesPerMessage; k++)
            cache.put(k, k);

        demanderNode = (IgniteEx) Ignition.start(configuration("demander"));

        demanderCacheCtx = demanderNode.cachex(DEFAULT_CACHE_NAME).context();
        demanderPreloader = demanderCacheCtx.preloader();
        demanderCacheObjCtx = demanderCacheCtx.cacheObjectContext();
        demanderCacheObjProc = demanderCacheCtx.cacheObjects();
        demanderSharedCtx = demanderCacheCtx.shared();

        supplierNode.cluster().setBaselineTopology(2);

        U.sleep(2_000);

        demanderNode.cache(DEFAULT_CACHE_NAME).rebalance();

        U.sleep(2_000);
    }

    /**
     * Create Ignite configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Configuration.
     */
    protected IgniteConfiguration configuration(String igniteInstanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(discoSpi);

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                    int grpId = ((GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message()).groupId();

                    if (grpId == CU.cacheId(DEFAULT_CACHE_NAME))
                        return; // block supply messages
                }

                super.sendMessage(node, msg);
            }

            @Override public void sendMessage(ClusterNode node, Message msg,
                IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                    int grpId = ((GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message()).groupId();

                    if (grpId == CU.cacheId(DEFAULT_CACHE_NAME))
                        return; // block supply messages
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(2L * 1024 * 1024 * 1024)
                    .setMaxSize(2L * 1024 * 1024 * 1024)
                )
        );

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    protected CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(1)
            .setRebalanceDelay(-1);
    }

    /**
     * Tear down routine.
     *
     * @throws Exception If failed.
     */
    @TearDown
    public void tearDown() throws Exception {
        Ignition.stopAll(true);
    }

    @Benchmark
    public void handleSupplyMessage() throws Exception {
        try {
            demanderPreloader.handleSupplyMessage(0, supplierNode.localNode().id(), generateSupplyMessage());
        }
        catch (Throwable ex) {
            ex.printStackTrace();

            throw ex;
        }
    }

    private GridDhtPartitionSupplyMessage generateSupplyMessage() throws Exception {
        GridDhtPartitionSupplyMessage msg = new GridDhtPartitionSupplyMessage(
            4,
            CU.cacheId(DEFAULT_CACHE_NAME),
            new AffinityTopologyVersion(2, 2),
            true
        );

        for (int i = 0; i < entriesPerMessage; i++) {
            GridCacheEntryInfo entry = new GridCacheEntryInfo();

            entry.cacheId(CU.cacheId(DEFAULT_CACHE_NAME));
            entry.key(supplierCacheObjProc.toCacheKeyObject(supplierCacheObjCtx, supplierCacheCtx, ThreadLocalRandom.current().nextInt(), true));
            entry.value(supplierCacheObjProc.toCacheObject(supplierCacheObjCtx, ThreadLocalRandom.current().nextInt(), true));
            entry.version(new GridCacheVersion(2, 1, 1));

            msg.addEntry0(0, entry, supplierSharedCtx, supplierCacheObjCtx);
        }

        return msg;
    }

    @Benchmark
    public void handleDemandMessage() throws Exception {
        try {
            supplierPreloader.handleDemandMessage(0, demanderNode.localNode().id(), generateDemandMessage());
        }
        catch (Throwable ex) {
            ex.printStackTrace();

            throw ex;
        }
    }

    private GridDhtPartitionDemandMessage generateDemandMessage() throws Exception {
        GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(
            4,
            new AffinityTopologyVersion(2, 2),
            CU.cacheId(DEFAULT_CACHE_NAME)
        );

        msg.partitions().addFull(0);

        return msg;
    }

    public static void main(String[] args)throws Exception {
        run("handleSupplyMessage", 4, 10000);
        run("handleDemandMessage", 4, 10000);

//        JmhCachePreloaderBenchmark benchmark = new JmhCachePreloaderBenchmark();
//
//        benchmark.setup();
//
//        benchmark.handleSupplyMessage();
//
//        benchmark.tearDown();
    }

    /**
     * Run benchmark.
     *
     * @param benchmark Benchmark to run.
     * @param threads Amount of threads.
     * @throws Exception If failed.
     */
    private static void run(String benchmark, int threads, int entriesPerMessage) throws Exception {
        String simpleClsName = JmhCachePreloaderBenchmark.class.getSimpleName();

        String output = simpleClsName + "-" + benchmark +
            "-" + threads + "-threads" +
            "-" + entriesPerMessage + "-epm";

        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(threads)
            .warmupIterations(10)
            .measurementIterations(60)
            .benchmarks(simpleClsName + "." + benchmark)
            .output(output + ".jmh.log")
            .profilers(GCProfiler.class)
            .jvmArguments(
                "-Xms4g",
                "-Xmx4g",
                "-XX:+UnlockCommercialFeatures",
                "-XX:+FlightRecorder",
                "-XX:StartFlightRecording=delay=30s,dumponexit=true,filename=" + output + ".jfr",
                JmhIdeBenchmarkRunner.createProperty(PROP_ENTRIES_PER_MESSAGE, entriesPerMessage)
            )
            .run();
    }
}
