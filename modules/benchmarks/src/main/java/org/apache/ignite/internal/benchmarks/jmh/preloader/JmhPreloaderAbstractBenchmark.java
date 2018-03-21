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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
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
public class JmhPreloaderAbstractBenchmark extends JmhAbstractBenchmark {
    /** Property: entries per message. */
    protected static final String PROP_ENTRIES_PER_MESSAGE = "ignite.jmh.preloader.entriesPerMessage";

    /** Property: entries per message. */
    protected static final String PROP_REBALANCE_TYPE = "ignite.jmh.preloader.rebalanceType";

    /** IP finder shared across nodes. */
    protected static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Default cache name. */
    protected static final String DEFAULT_CACHE_NAME = "default";

    protected int entriesPerMessage;

    protected RebalanceType rebalanceType;

    protected IgniteEx supplierNode;
    protected IgniteEx demanderNode;

    protected GridCachePreloader supplierPreloader;
    protected CacheObjectContext supplierCacheObjCtx;
    protected GridCacheSharedContext<Object, Object> supplierSharedCtx;
    protected IgniteCacheObjectProcessor supplierCacheObjProc;
    protected GridCacheContext supplierCacheCtx;
    protected IgniteCache supplierCache;

    protected GridCachePreloader demanderPreloader;
    protected CacheObjectContext demanderCacheObjCtx;
    protected GridCacheSharedContext<Object, Object> demanderSharedCtx;
    protected IgniteCacheObjectProcessor demanderCacheObjProc;
    protected GridCacheContext demanderCacheCtx;
    protected IgniteCache demanderCache;

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

        rebalanceType = enumProperty(PROP_REBALANCE_TYPE, RebalanceType.class, RebalanceType.ENDLESS);

        if (rebalanceType == RebalanceType.ENDLESS)
            System.setProperty(IgniteSystemProperties.IGNITE_DEBUG_ENDLESS_REBALANCE, "true");
        else
            System.setProperty(IgniteSystemProperties.IGNITE_DEBUG_ENDLESS_REBALANCE, "false");

        supplierNode = (IgniteEx) Ignition.start(configuration("supplier"));

        supplierNode.cluster().active(true);

        supplierCacheCtx = supplierNode.cachex(DEFAULT_CACHE_NAME).context();
        supplierPreloader = supplierCacheCtx.preloader();
        supplierCacheObjCtx = supplierCacheCtx.cacheObjectContext();
        supplierCacheObjProc = supplierCacheCtx.cacheObjects();
        supplierSharedCtx = supplierCacheCtx.shared();
        supplierCache = supplierNode.cache(DEFAULT_CACHE_NAME);

        entriesPerMessage = intProperty(PROP_ENTRIES_PER_MESSAGE, 10_000);

        IgniteCache<Integer, Integer> cache = supplierNode.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < entriesPerMessage; k++)
            cache.put(k, k);

        demanderNode = (IgniteEx) Ignition.start(configuration("demander"));

        demanderCacheCtx = demanderNode.cachex(DEFAULT_CACHE_NAME).context();
        demanderPreloader = demanderCacheCtx.preloader();
        demanderCacheObjCtx = demanderCacheCtx.cacheObjectContext();
        demanderCacheObjProc = demanderCacheCtx.cacheObjects();
        demanderSharedCtx = demanderCacheCtx.shared();
        demanderCache = demanderNode.cache(DEFAULT_CACHE_NAME);

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

        if (rebalanceType == RebalanceType.SYNTHETIC || rebalanceType == RebalanceType.NONE) {
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
        }

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(8L * 1024 * 1024 * 1024)
                    .setMaxSize(8L * 1024 * 1024 * 1024)
                )
        );

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    protected CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(1)
            .setRebalanceDelay(-1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);
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

    protected GridDhtPartitionSupplyMessage generateSupplyMessage() throws Exception {
        GridDhtPartitionSupplyMessage msg = new GridDhtPartitionSupplyMessage(
            4,
            CU.cacheId(DEFAULT_CACHE_NAME),
            supplierCacheCtx.affinity().affinityTopologyVersion(),
            true
        );

        for (int i = 0; i < entriesPerMessage; i++) {
            GridCacheEntryInfo entry = new GridCacheEntryInfo();

            entry.cacheId(CU.cacheId(DEFAULT_CACHE_NAME));
            entry.key(supplierCacheObjProc.toCacheKeyObject(supplierCacheObjCtx, supplierCacheCtx, ThreadLocalRandom.current().nextInt(100_000), true));
            entry.value(supplierCacheObjProc.toCacheObject(supplierCacheObjCtx, ThreadLocalRandom.current().nextInt(), true));
            entry.version(new GridCacheVersion(2, 1, 1));

            msg.addEntry0(0, entry, supplierSharedCtx, supplierCacheObjCtx);
        }

        return msg;
    }

    protected GridDhtPartitionDemandMessage generateDemandMessage() throws Exception {
        GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(
            4,
            demanderCacheCtx.affinity().affinityTopologyVersion(),
            CU.cacheId(DEFAULT_CACHE_NAME)
        );

        msg.partitions().addFull(0);

        return msg;
    }

    public enum RebalanceType {
        NONE,
        SYNTHETIC,
        ENDLESS
    }
}
