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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.NotNull;

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

    /** Replicated caches. */
    private Set<Integer> caches;

    /** */
    private final List<Applier> appliers = new ArrayList<>();

    /** */
    private final Properties props;

    /**
     * @param ctx Plugin context.
     */
    public KafkaToIgnitePlugin(PluginContext ctx, Properties props, Set<Integer> caches) {
        this.ctx = ctx;
        this.ign = (IgniteEx)ctx.grid();
        this.log = ign.log().getLogger(KafkaToIgnitePlugin.class);
        this.caches = caches;
        this.props = props;

        execSvc = Executors.newFixedThreadPool(thCnt, new ThreadFactory() {
            AtomicInteger cntr = new AtomicInteger();

            @Override public Thread newThread(@NotNull Runnable r) {
                Thread th = new Thread(r);

                th.setName("applier-thread-" + cntr.getAndIncrement());

                return th;
            }
        });
    }

    /** Start the plugin. */
    public void start() throws Exception {
        if (log.isInfoEnabled())
            log.warning("Starting KafkaToIgnitePlugin[caches=" + caches + ']');

        ign.context().cache().context().exchange().registerExchangeAwareComponent(this);
    }

    /** Start the plugin. */
    public void stop() {
        if (log.isInfoEnabled())
            log.warning("KafkaToIgnitePlugin.stop");

        stopAppliers(true);
    }

    /** {@inheritDoc} */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        boolean needToDestroy = needToDestroyWorkers(fut);

        if (log.isInfoEnabled())
            log.warning("KafkaToIgnitePlugin.onInitBeforeTopologyLock[needToDestroy=" + needToDestroy + ']');

        if (needToDestroy) {
            stopAppliers(needToDestroy);

            log.warning("Appliers stoped!");
        }

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

            if (workersDestroyed) {
                switchCachePartitions(topVer);

                startAppliers();
            }
        }
        catch (IgniteCheckedException e) {
            log.error("Fail to get topology version.", e);
        }
    }

    /** */
    private void switchCachePartitions(AffinityTopologyVersion topVer) {
        log.warning("KafkaToIgnitePlugin.switchGroupsPartitions " + topVer);

        assert appliers.isEmpty();

        Map<Integer, Applier> kafkaPartApplier = new HashMap<>();
        AtomicInteger cntr = new AtomicInteger();

        final boolean[] replicationEnabled = {false};

        caches.forEach(cacheId -> {
            try {
                boolean cacheExists = switchCachePartitions(cacheId, topVer, kafkaPartApplier, appliers, cntr);

                replicationEnabled[0] |= cacheExists;
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        });

        if (!replicationEnabled[0])
            return;
    }

    /** */
    private void startAppliers() {
        appliers.forEach(execSvc::submit);

        for (int i=0; i< appliers.size(); i++) {
            log.warning(i + " = " + appliers.get(i));

            execSvc.submit(appliers.get(i));
        }
    }

    /** */
    private void stopAppliers(boolean destroy) {
        if (destroy) {
            appliers.forEach(Applier::close);
            appliers.clear();
        }
    }

    /** */
    public boolean switchCachePartitions(
        int cacheId,
        AffinityTopologyVersion topVer,
        Map<Integer, Applier> applier4partition,
        List<Applier> appliers,
        AtomicInteger cntr
    ) throws IgniteCheckedException {
        DynamicCacheDescriptor cacheDesc = ign.context().cache().cacheDescriptor(cacheId);

        if (cacheDesc == null) {
            log.warning("Cache not started or removed[cacheId=" + cacheId + ']');

            return false;
        }

        int grpId = cacheDesc.groupId();

        GridAffinityAssignmentCache aff = ign.context().cache().context().affinity().groupAffinity(grpId);

        if (aff == null) {
            log.warning("Cache not started or removed[cacheId=" + cacheId + ']');

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

        if (primaryParts.isEmpty())
            return false;

        for (Map.Entry<Integer, List<Integer>> kafka2ignite : kafkaPartsToIgnite.entrySet()) {
            Integer kafkaPart = kafka2ignite.getKey();

            if (applier4partition.containsKey(kafkaPart))
                applier4partition.get(kafkaPart).addCachePartition(cacheId, kafka2ignite.getValue(), kafkaPart);

            Applier applier;

            if (appliers.size() < thCnt) {
                applier = new Applier(ign, props);

                appliers.add(applier);
            }
            else
                applier = appliers.get(cntr.getAndIncrement() % thCnt);

            applier.addCachePartition(cacheId, kafka2ignite.getValue(), kafkaPart);

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
            (fut.firstEvent().type() == EVT_NODE_LEFT || fut.firstEvent().type() == EVT_NODE_FAILED);

        if (srvNodeFailed) {
            log.warning("Destroying workers becuase server node failed.");

            return true;
        }

        boolean assignmentsOrPrimaryChanged = fut.activateCluster() || fut.affinityReassign();

        if (assignmentsOrPrimaryChanged) {
            log.warning("Destroying workers because assignmentsOrPrimaryChanged[" +
                "activateCluster=" + fut.activateCluster() +
                ",affinityReassign=" + fut.affinityReassign() + ']');

            return true;
        }

        boolean cacheCreated = fut.exchangeActions() != null &&
            caches.stream().anyMatch(cacheId -> fut.exchangeActions().cacheStarted(cacheId));

        if (cacheCreated) {
            log.warning("Destroying workers because cache created");

            return true;
        }

        boolean cacheRemoved = fut.exchangeActions() != null &&
            caches.stream().anyMatch(cacheId -> fut.exchangeActions().cacheStopped(cacheId));

        if (cacheRemoved)
            log.warning("Destroying workers because cache removed");

        return cacheRemoved;
    }
}