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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.CallbackExecutorLogListener;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * For testing of rebalance statistics.
 */
public class RebalanceStatisticsTest extends GridCommonAbstractTest {
    /** Caches configurations. */
    private CacheConfiguration[] cacheCfgs;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(cacheCfgs)
            .setRebalanceThreadPoolSize(5)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /**
     * Test statistics of a rebalance.
     *
     * Steps:
     * 1)Creating and filling a cluster;
     * 2)Starting a new node with listening for logs and supply messages;
     * 3)Check that number of supply messages is equal to number of logs received +1;
     * 4)Find corresponding message in log for each supply message;
     * 5)Find log message after all of groups and to check its correctness.
     *
     * @throws Exception if any error occurs.
     */
    @Test
    public void testRebalanceStatistics() throws Exception {
        createCluster(3);

        ListeningTestLogger listeningTestLog = new ListeningTestLogger(log);
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(3)).setGridLogger(listeningTestLog);

        // Collect log messages with rebalance statistics.
        Collection<String> logMsgs = new ConcurrentLinkedQueue<>();
        listeningTestLog.registerListener(
            new CallbackExecutorLogListener("Completed( \\(final\\))? rebalanc(ing|e chain).*", logMsgs::add)
        );

        Map<Ignite, Collection<T2<ClusterNode, Message>>> recordMsgs = new ConcurrentHashMap<>();

        G.allGrids().forEach(n -> TestRecordingCommunicationSpi.spi(n).record((node, msg) -> {
            if (GridDhtPartitionSupplyMessage.class.isInstance(msg))
                recordMsgs.computeIfAbsent(n, n1 -> new ConcurrentLinkedQueue<>()).add(new T2<>(node, msg));

            return false;
        }));

        IgniteEx node = startGrid(cfg);
        awaitPartitionMapExchange();

        // Collect supply messages only for new node.
        Map<Ignite, List<GridDhtPartitionSupplyMessage>> supplyMsgs = G.allGrids().stream()
            .filter(n -> !n.equals(node))
            .collect(
                toMap(
                    identity(),
                    n -> recordMsgs.get(n).stream()
                        .filter(t2 -> t2.get1().id().equals(node.localNode().id()))
                        .map(IgniteBiTuple::get2)
                        .map(GridDhtPartitionSupplyMessage.class::cast)
                        .collect(toList())
                )
            );

        // +1 because one message about end of rebalance for all groups.
        assertEquals(supplyMsgs.values().stream().mapToInt(List::size).sum() + 1, logMsgs.size());

        IgniteClosure2X<GridCacheEntryInfo, CacheObjectContext, Long> getSize =
            new IgniteClosure2X<GridCacheEntryInfo, CacheObjectContext, Long>() {
                /** {@inheritDoc} */
                @Override public Long applyx(
                    GridCacheEntryInfo info,
                    CacheObjectContext ctx
                ) throws IgniteCheckedException {
                    return (long)info.marshalledSize(ctx);
                }
            };

        for (Map.Entry<Ignite, List<GridDhtPartitionSupplyMessage>> supplyMsg : supplyMsgs.entrySet()) {
            List<String> supplierMsgs = logMsgs.stream()
                .filter(s -> s.contains("supplier=" + supplyMsg.getKey().cluster().localNode().id()))
                .collect(toList());

            List<GridDhtPartitionSupplyMessage> msgs = supplyMsg.getValue();
            assertEquals(msgs.size(), supplierMsgs.size());

            for (GridDhtPartitionSupplyMessage msg : msgs) {
                Map<Integer, CacheEntryInfoCollection> infos = U.field(msg, "infos");

                CacheGroupContext grpCtx = node.context().cache().cacheGroup(msg.groupId());

                long bytes = 0;

                for (CacheEntryInfoCollection c : infos.values()) {
                    for (GridCacheEntryInfo i : c.infos())
                        bytes += getSize.apply(i, grpCtx.cacheObjectContext());
                }

                String[] checVals = {
                    "grp=" + grpCtx.cacheOrGroupName(),
                    "partitions=" + infos.size(),
                    "entries=" + infos.values().stream().mapToInt(i -> i.infos().size()).sum(),
                    "topVer=" + msg.topologyVersion(),
                    "rebalanceId=" + U.field(msg, "rebalanceId"),
                    "bytesRcvd=" + U.humanReadableByteCount(bytes),
                    "fullPartitions=" + infos.size(),
                    "fullEntries=" + infos.values().stream().mapToInt(i -> i.infos().size()).sum(),
                    "fullBytesRcvd=" + U.humanReadableByteCount(bytes),
                    "histPartitions=0",
                    "histEntries=0",
                    "histBytesRcvd=0",
                };

                assertTrue(
                    supplierMsgs.toString(),
                    supplierMsgs.stream().anyMatch(s -> Stream.of(checVals).allMatch(s::contains))
                );
            }
        }

        String rebChainMsg = logMsgs.stream().filter(s -> s.startsWith("Completed rebalance chain")).findAny().get();

        long rebId = -1;
        int parts = 0;
        int entries = 0;
        long bytes = 0;

        for (List<GridDhtPartitionSupplyMessage> msgs : supplyMsgs.values()) {
            for (GridDhtPartitionSupplyMessage msg : msgs) {
                Map<Integer, CacheEntryInfoCollection> infos = U.field(msg, "infos");

                rebId = U.field(msg, "rebalanceId");
                parts += infos.size();
                entries += infos.values().stream().mapToInt(i -> i.infos().size()).sum();

                CacheObjectContext cacheObjCtx = node.context().cache().cacheGroup(msg.groupId()).cacheObjectContext();

                for (CacheEntryInfoCollection c : infos.values()) {
                    for (GridCacheEntryInfo i : c.infos())
                        bytes += getSize.apply(i, cacheObjCtx);
                }
            }
        }

        String[] checVals = {
            "partitions=" + parts,
            "entries=" + entries,
            "rebalanceId=" + rebId,
            "bytesRcvd=" + U.humanReadableByteCount(bytes),
        };

        assertTrue(rebChainMsg, Stream.of(checVals).allMatch(rebChainMsg::contains));
    }

    /**
     * Create and populate cluster.
     *
     * @param nodeCnt Node count.
     * @return Coordinator.
     * @throws Exception if any error occurs.
     */
    private IgniteEx createCluster(int nodeCnt) throws Exception {
        String grpName0 = "grp0";
        String grpName1 = "grp1";

        cacheCfgs = new CacheConfiguration[] {
            cacheConfiguration("ch_0_0", grpName0, 10, 2),
            cacheConfiguration("ch_0_1", grpName0, 10, 2),
            cacheConfiguration("ch_0_2", grpName0, 10, 2),
            cacheConfiguration("ch_1_0", grpName1, 10, 2),
            cacheConfiguration("ch_1_1", grpName1, 10, 2),
        };

        IgniteEx crd = startGrids(nodeCnt);
        crd.cluster().active(true);

        populateCluster(crd, 10, "");

        return crd;
    }

    /**
     * Сontent of node data on all partitions for all caches.
     *
     * @param node Node.
     * @param cnt  Count values.
     * @param add  Additional value postfix.
     */
    private void populateCluster(IgniteEx node, int cnt, String add) {
        requireNonNull(node);
        requireNonNull(add);

        for (CacheConfiguration cacheCfg : cacheCfgs) {
            String cacheName = cacheCfg.getName();
            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (int i = 0; i < cacheCfg.getAffinity().partitions(); i++) {
                partitionKeys(cache, i, cnt, i * cnt)
                    .forEach(k -> cache.put(k, cacheName + "_val_" + k + add));
            }
        }
    }

    /**
     * Create cache configuration.
     *
     * @param cacheName Cache name.
     * @param grpName Cache group name.
     * @param parts Count of partitions.
     * @param backups Count backup.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName, @Nullable String grpName, int parts, int backups) {
        requireNonNull(cacheName);

        return new CacheConfiguration<>(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(backups)
            .setGroupName(grpName);
    }
}
