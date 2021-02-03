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

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static org.apache.ignite.cdc.CDCIgniteToKafka.IGNITE_TO_KAFKA_TOPIC;
import static org.apache.ignite.cdc.KafkaUtils.TIMEOUT_MIN;
import static org.apache.ignite.cdc.Utils.fromSystemOrProperty;
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
    public static final String PARTITIONS = "kafka.to.ignite.consumer.partitions";

    /** */
    private final PluginContext ctx;

    /** */
    private final IgniteEx ign;

    /** Properties. */
    private final Properties props;

    /** */
    private final IgniteLogger log;

    /** Executor service. */
    private ExecutorService execSvc;

    /** Topic name. */
    private final String topic;

    /** Kafka partitions count. */
    private int kafkaPartitionsNum;

    private Set<Integer> grps;

    /** Map to cache ids. */
    private ConcurrentMap<Integer, IgniteCache<BinaryObject, BinaryObject>> cc = new ConcurrentHashMap<>();

    /**
     * @param ctx Plugin context.
     */
    public KafkaToIgnitePlugin(PluginContext ctx, Properties props, Set<Integer> groups) {
        this.ctx = ctx;
        this.ign = (IgniteEx)ctx.grid();
        this.props = props;
        this.topic = fromSystemOrProperty(IGNITE_TO_KAFKA_TOPIC, props);
        this.log = ign.log();
        this.grps = groups;
    }

    /** Start the plugin. */
    public void start() throws Exception {
        if (log.isInfoEnabled())
            log.warning("Starting KafkaToIgnitePlugin[" + ign.configuration().getIgniteInstanceName() + ']' + "[grps=" + grps + ']');

        ign.context().cache().context().exchange().registerExchangeAwareComponent(this);
    }

    /** Start the plugin. */
    public void stop() {
        if (log.isInfoEnabled())
            log.warning("KafkaToIgnitePlugin.stop[" + ign.configuration().getIgniteInstanceName() + ']');
    }

    /** {@inheritDoc} */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        boolean destroyWorkers = workersDestroyed(fut);

        if (log.isInfoEnabled()) {
            log.warning("KafkaToIgnitePlugin.onInitBeforeTopologyLock" +
                "[" + ign.configuration().getIgniteInstanceName() + "]" +
                "[destroyWorkers=" + destroyWorkers + ']');
        }

        // TODO: stop and destroy kafka readers(assignments will be changed).
    }

    /** {@inheritDoc} */
    @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        try {
            AffinityTopologyVersion topVer = fut.get();

            boolean workersDestroyed = workersDestroyed(fut);

            if (log.isInfoEnabled()) {
                log.warning("KafkaToIgnitePlugin.onDoneAfterTopologyUnlock" +
                    "[" + ign.configuration().getIgniteInstanceName() + "]" +
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

        grps.forEach(grpId -> {
            try {
                switchGroupPartitions(grpId, topVer);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        });
    }

    /** */
    public void switchGroupPartitions(int grpId, AffinityTopologyVersion topVer) throws IgniteCheckedException {
        GridAffinityAssignmentCache aff = ign.context().cache().context().affinity().groupAffinity(grpId);

        if (aff == null) {
            log.warning("Group not started or removed[grpId=" + grpId + ']');

            return;
        }

        List<List<ClusterNode>> assignments = aff.assignments(topVer);

        Set<Integer> primaryParts = IntStream.range(0, assignments.size())
            .filter(i -> assignments.get(i).get(0).id().equals(ign.localNode().id()))
            .boxed()
            .collect(Collectors.toSet());

        if (log.isInfoEnabled())
            log.warning("KafkaToIgnitePlugin.switchPartitions[" + ign.configuration().getIgniteInstanceName() + ']' + primaryParts);
    }

    /** */
    private boolean workersDestroyed(GridDhtPartitionsExchangeFuture fut) {
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

    /**
     * Single Kafka partition consumer.
     */
    private static class KafkaPartitionToIgnite implements Runnable {
        /** Kafka partition to read. */
        private final int kafkaPartition;

        /** Properties. */
        private final Properties props;

        /** Ignite. */
        private final IgniteEx ign;

        /**
         * @param kafkaPartition Kafka partition to read.
         */
        public KafkaPartitionToIgnite(int kafkaPartition, Properties props, IgniteEx ign) {
            this.kafkaPartition = kafkaPartition;
            this.props = props;
            this.ign = ign;
        }

        /** {@inheritDoc} */
        @Override public void run() {

        }
    }

    /** Kafka to Ignite worker. */
    private class KafkaToIgniteWorker implements Runnable {
        public static final int RETRY_CNT = 3;

        /** */
        private boolean stoped;

        /** */
        private int retryCnt = RETRY_CNT;

        /** {@inheritDoc} */
        @Override public void run() {
            KafkaConsumer<BinaryObject, EntryEvent<BinaryObject, BinaryObject>> consumer = new KafkaConsumer<>(props);

            while (retryCnt > 0 && !stoped) {
                ConsumerRecords<BinaryObject, EntryEvent<BinaryObject, BinaryObject>> recs;

                try {
                    recs = consumer.poll(Duration.ofMinutes(TIMEOUT_MIN));

                    retryCnt = RETRY_CNT;
                }
                catch (Exception e) {
                    retryCnt--;

                    e.printStackTrace();

                    try {
                        Thread.sleep(Duration.ofMillis(TIMEOUT_MIN).toMillis());
                    }
                    catch (InterruptedException err) {
                        throw new RuntimeException(err);
                    }

                    continue;
                }

                for (ConsumerRecord<BinaryObject, EntryEvent<BinaryObject, BinaryObject>> rec : recs)
                    apply(rec.value());
            }
        }

        /** Applies single event to Ignite. */
        private void apply(EntryEvent<BinaryObject, BinaryObject> evt) {
            IgniteCache<BinaryObject, BinaryObject> cache = cc.computeIfAbsent(evt.cacheId(), cacheId -> {
                for (String cacheName : ign.cacheNames()) {
                    if (CU.cacheId(cacheName) == cacheId)
                        return ign.cache(cacheName).withKeepBinary();
                }

                throw new IllegalStateException("Cache with id not found[cacheId=" + cacheId + ']');
            });

            CacheEntry<BinaryObject, BinaryObject> entry = cache.getEntry(evt.key());

            GridCacheVersion rmvVer = null;

            if (entry == null) {
                GridCacheContext<Object, Object> cctx = ign.context().cache().context().cacheContext(evt.cacheId());

                GridDhtLocalPartition part = cctx.topology().localPartition(evt.partition());

                rmvVer = part.findRemoveVersion(evt.key());
            }

            if (needToUpdate(evt.order(), entry == null ? null : (GridCacheVersion)entry.version(), rmvVer)) {
                switch (evt.operation()) {
                    case UPDATE:
                        cache.put(evt.key(), evt.value()); //TODO: add version from event to entry.

                    case DELETE:
                        cache.remove(evt.key()); //TODO: add version from event to entry.

                    default:
                        throw new IllegalArgumentException("Unknown operation type: " + evt.operation());
                }
            }
        }

        /**
         * TODO: add tombstone to Ignite.
         *
         * @param kafkaOrd Entry version from Kafka.
         * @param curVer Current entry version.
         * @param rmvVer Entry version from remove operation.
         * @return {@code True} if entry should be updated. {@code False} if this update should be skipped.
         */
        private boolean needToUpdate(EntryEventOrder kafkaOrd, GridCacheVersion curVer, GridCacheVersion rmvVer) {
            if (curVer == null && rmvVer == null)
                return true;

            GridCacheVersion kafkaVer =
                new GridCacheVersion(kafkaOrd.topVer(), kafkaOrd.nodeOrderDrId(), kafkaOrd.order());

            if (rmvVer != null)
                return kafkaVer.compareTo(rmvVer) > 0;

            return kafkaVer.compareTo(curVer) > 0;
        }
    }
}
/*
        kafkaPartitionsNum = KafkaUtils.initTopic(topic, props);

        int thNum = Integer.parseInt(fromSystemOrProperty(KAFKA_TO_IGNITE_THREAD_COUNT, props));

        ExecutorService execSvc = Executors.newFixedThreadPool(thNum);

        for (int i = 0; i < thNum; i++)
            execSvc.submit(new KafkaToIgniteWorker());

        execSvc.shutdown();

        execSvc.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
*/

