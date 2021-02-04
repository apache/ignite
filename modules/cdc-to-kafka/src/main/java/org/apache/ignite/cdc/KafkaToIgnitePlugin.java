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

package org.apache.ignite.cdc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Plugin to apply DataEntry from Kafka to Ignite.
 *
 * Expectations:
 *
 * 1. Same cache created on writer and reader side.
 *
 */
public class KafkaToIgnitePlugin implements IgnitePlugin, PartitionsExchangeAware {
    /** */
    private final PluginContext ctx;

    /** */
    private final IgniteEx ign;

    /** */
    private final IgniteLogger log;

    /** Executor service. */
    private ExecutorService execSvc;

    /** */
    private final int thCnt = 10;

    /** */
    private final int kafkaPartitionCnt = 100;

    /** Replicated groups. */
    private Set<Integer> grps;

    /** Map to cache ids. */
    private ConcurrentMap<Integer, IgniteCache<BinaryObject, BinaryObject>> cc = new ConcurrentHashMap<>();

    /**
     * @param ctx Plugin context.
     */
    public KafkaToIgnitePlugin(PluginContext ctx, Properties props, Set<Integer> groups) {
        this.ctx = ctx;
        this.ign = (IgniteEx)ctx.grid();
        this.log = ign.log().getLogger(KafkaToIgnitePlugin.class);
        this.grps = groups;

        execSvc = Executors.newFixedThreadPool(thCnt);
    }

    /** Start the plugin. */
    public void start() throws Exception {
        if (log.isInfoEnabled())
            log.warning("Starting KafkaToIgnitePlugin[grps=" + grps + ']');

        ign.context().cache().context().exchange().registerExchangeAwareComponent(this);
    }

    /** Start the plugin. */
    public void stop() {
        if (log.isInfoEnabled())
            log.warning("KafkaToIgnitePlugin.stop");
    }

    /** {@inheritDoc} */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        boolean destroyWorkers = needToDestroyWorkers(fut);

        if (log.isInfoEnabled())
            log.warning("KafkaToIgnitePlugin.onInitBeforeTopologyLock[destroyWorkers=" + destroyWorkers + ']');
    }

    /** {@inheritDoc} */
    @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        try {
            AffinityTopologyVersion topVer = fut.get();

            boolean workersDestroyed = needToDestroyWorkers(fut);

            if (log.isInfoEnabled()) {
                log.warning("KafkaToIgnitePlugin.onDoneAfterTopologyUnlock" +
                    "[workersDestroyed=" + workersDestroyed + ", topVer=" + topVer + ']');
            }

            if (workersDestroyed)
                switchGroupsPartitions(topVer);
        }
        catch (IgniteCheckedException e) {
            log.error("Fail to get topology version.", e);
        }
    }

    /** */
    private void switchGroupsPartitions(AffinityTopologyVersion topVer) {
        log.warning("KafkaToIgnitePlugin.switchGroupsPartitions " + topVer);

        List<Applier> appliers = new ArrayList<>();
        Map<Integer, Applier> kafkaPartApplier = new HashMap<>();
        AtomicInteger cntr = new AtomicInteger();

        final boolean[] replicationEnabled = {false};

        grps.forEach(grpId -> {
            try {
                boolean grpExists = switchGroupPartitions(grpId, topVer, kafkaPartApplier, appliers, cntr);

                replicationEnabled[0] |= grpExists;
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        });

        if (!replicationEnabled[0])
            return;

        for (int i=0; i<appliers.size(); i++)
            log.warning(i + " = " + appliers.get(i));
    }

    /** */
    public boolean switchGroupPartitions(
        int grpId,
        AffinityTopologyVersion topVer,
        Map<Integer, Applier> applier4partition,
        List<Applier> appliers,
        AtomicInteger cntr
    ) throws IgniteCheckedException {
        GridAffinityAssignmentCache aff = ign.context().cache().context().affinity().groupAffinity(grpId);

        if (aff == null) {
            log.warning("Group not started or removed[grpId=" + grpId + ']');

            return false;
        }

        Map<Integer, List<Integer>> kafkaPartsToIgnite = new TreeMap<>();

        List<List<ClusterNode>> assignments = aff.assignments(topVer);

        Set<Integer> primaryParts = IntStream.range(0, assignments.size())
            .filter(i -> assignments.get(i).get(0).id().equals(ign.localNode().id()))
            .peek(ignPart -> kafkaPartsToIgnite
                .computeIfAbsent(ignPart % kafkaPartitionCnt, key -> new ArrayList<>())
                .add(ignPart))
            .boxed()
            .collect(Collectors.toSet());

        for (Map.Entry<Integer, List<Integer>> kafka2ignite : kafkaPartsToIgnite.entrySet()) {
            Integer kafkaPart = kafka2ignite.getKey();

            if (applier4partition.containsKey(kafkaPart))
                applier4partition.get(kafkaPart).addGroupPartition(kafkaPart, grpId, kafka2ignite.getValue());

            Applier applier;

            if (appliers.size() < thCnt) {
                applier = new Applier();

                appliers.add(applier);
            }
            else
                applier = appliers.get(cntr.getAndIncrement() % thCnt);

            applier.addGroupPartition(kafkaPart, grpId, kafka2ignite.getValue());

            applier4partition.put(kafkaPart, applier);
        }

        if (log.isInfoEnabled())
            log.warning("KafkaToIgnitePlugin.switchPartitions " + primaryParts);

        return true;
    }

    /** */
    private boolean needToDestroyWorkers(GridDhtPartitionsExchangeFuture fut) {
        boolean isCacheAffinityChangeMessage = fut.events().events().stream().anyMatch(evt ->
            evt.type() == EVT_DISCOVERY_CUSTOM_EVT &&
                ((DiscoveryCustomEvent)evt).customMessage() instanceof CacheAffinityChangeMessage);

        if (isCacheAffinityChangeMessage) {
            log.warning("Destroying workers because of CacheAffinityChangeMessage");

            return true;
        }

        boolean srvNodeFailed = !fut.firstEvent().eventNode().isClient() &&
            (fut.firstEvent().type() == EVT_NODE_LEFT ||
                fut.firstEvent().type() == EVT_NODE_FAILED);

        if (srvNodeFailed) {
            log.warning("Destroying workers becuase server node failed.");

            return true;
        }

        boolean assignmentsOrPrimaryChanged =
            fut.activateCluster() ||
            fut.affinityReassign();

        if (assignmentsOrPrimaryChanged) {
            log.warning("Destroying workers because assignmentsOrPrimaryChanged[" +
                "activateCluster=" + fut.activateCluster() +
                ",affinityReassign=" + fut.affinityReassign() + ']');

            return true;
        }

        boolean grpCreated = grps.stream().anyMatch(fut::dynamicCacheGroupStarted);

        if (grpCreated) {
            log.warning("Destroying workers because group created");

            return true;
        }

        boolean grpRemoved = fut.exchangeActions() != null && fut.exchangeActions()
            .cacheGroupsToStop()
            .stream()
            .anyMatch(grpData -> grps.contains(grpData.descriptor().groupId()));

        if (grpRemoved)
            log.warning("Destroying workers because group removed");

        return grpRemoved;
    }

    /** */
    private static class Applier implements Runnable {
        /** */
        private final Map<Integer, Set<Integer>> grpParts = new HashMap<>();

        /** */
        private final Set<Integer> kafkaParts = new HashSet<>();

        /** {@inheritDoc} */
        @Override public void run() {

        }

        /**
         * @param grpId
         * @param parts
         */
        public void addGroupPartition(int kafkaPart, int grpId, List<Integer> parts) {
            grpParts
                .computeIfAbsent(grpId, key -> new HashSet<>())
                .addAll(parts);

            kafkaParts.add(kafkaPart);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Applier{" +
                "kafkaParts=" + kafkaParts +
                ", grpParts=" + grpParts +
                '}';
        }
    }
}