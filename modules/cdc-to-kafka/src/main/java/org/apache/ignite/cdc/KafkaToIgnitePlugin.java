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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
    private volatile boolean appliersStopped;

    /** */
    private final List<Future<?>> appliersFuts = new ArrayList<>();

    /** */
    private final List<Applier> appliers = new ArrayList<>();

    /** */
    private ConcurrentMap<Integer, IgniteCache<BinaryObject, BinaryObject>> ignCaches = new ConcurrentHashMap<>();

    /**
     * @param ctx Plugin context.
     */
    public KafkaToIgnitePlugin(PluginContext ctx, Properties props, Set<Integer> caches) {
        this.ctx = ctx;
        this.ign = (IgniteEx)ctx.grid();
        this.log = ign.log().getLogger(KafkaToIgnitePlugin.class);
        this.caches = caches;

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
        boolean destroyWorkers = needToDestroyWorkers(fut);

        if (log.isInfoEnabled())
            log.warning("KafkaToIgnitePlugin.onInitBeforeTopologyLock[destroyWorkers=" + destroyWorkers + ']');

        stopAppliers(destroyWorkers);
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
                switchCachePartitions(topVer);

            startAppliers();
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

        if (!replicationEnabled[0]) {
            return;
        }
    }

    /** */
    private void startAppliers() {
        appliersStopped = false;

        for (int i=0; i< appliers.size(); i++) {
            log.warning(i + " = " + appliers.get(i));

            appliersFuts.add(execSvc.submit(appliers.get(i)));
        }
    }

    /** */
    private void stopAppliers(boolean destroy) {
        appliersStopped = true;

        for (Future<?> afut : appliersFuts) {
            assert !afut.isCancelled();

            afut.cancel(true);
        }

        if (destroy) {
            appliers.forEach(applier -> {
                try {
                    applier.close();
                }
                catch (Throwable e) {
                    log.warning("Close error!", e);
                }
            });

            appliersFuts.clear();
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
        int grpId = ign.context().cache().cacheDescriptor(cacheId).groupId();

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
                applier = new Applier(log);

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
    private class Applier implements Runnable, AutoCloseable {
        /** cacheId -> kafkaPart -> List<ignitePart> */
        private final Map<Integer, Map<Integer, Set<Integer>>> cacheParts = new HashMap<>();

        /** */
        private final IgniteLogger log;

        /** */
        private Map<int[], KafkaConsumer<int[], byte[]>> consumers;

        /** */
        public Applier(IgniteLogger log) {
            this.log = log.getLogger(Applier.class);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                initConsumers();

                assert !consumers.isEmpty();

                Iterator<Map.Entry<int[], KafkaConsumer<int[], byte[]>>> consumerIter = Collections.emptyIterator();

                while (!appliersStopped) {
                    log.warning("Fetching data from " + Thread.currentThread().getName() + '!');

                    if (!consumerIter.hasNext())
                        consumerIter = consumers.entrySet().iterator();

                    Map.Entry<int[], KafkaConsumer<int[], byte[]>> entry = consumerIter.next();

                    poll(entry.getKey()[0], entry.getKey()[1], entry.getValue());
                }
            }
            catch (InterruptedException e) {
                Thread.interrupted();
            }

            log.warning(Thread.currentThread().getName() + " - stoped!");
        }

        /**
         * @param cacheId Cache id.
         * @param partitionId Partition id.
         * @param consumer Data consumer.
         */
        private void poll(int cacheId, int partitionId, KafkaConsumer<int[], byte[]> consumer) throws InterruptedException {
            Thread.sleep(100);

            ConsumerRecords<int[], byte[]> records = consumer.poll(Duration.ofSeconds(3));

            for (ConsumerRecord<int[], byte[]> rec : records) {
                int[] cacheAndPart = rec.key();

                if (cacheId != cacheAndPart[0] || partitionId != cacheAndPart[1])
                    continue;

                apply(toEvent(rec.value()));
            }
        }

        /**
         * @param evt Applies event to Ignite.
         */
        private void apply(EntryEvent<BinaryObject, BinaryObject> evt) {
            // Group id here!!!
            IgniteCache<BinaryObject, BinaryObject> cache = ignCaches.computeIfAbsent(evt.cacheId(), cacheId -> {
                for (String cacheName : ign.cacheNames()) {
                    if (CU.cacheId(cacheName) == cacheId)
                        return ign.cache(cacheName).withKeepBinary();
                }

                throw new IllegalStateException("Cache with id not found[cacheId=" + cacheId + ']');
            });

            CacheEntry<BinaryObject, BinaryObject> entry = cache.getEntry(evt.key());

            GridCacheVersion rmvVer = null;

            if (entry == null)
                //TODO: implement me.
                rmvVer = new GridCacheVersion(0, 0, 0);

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

        private boolean needToUpdate(EntryEventOrder kafkaOrd, GridCacheVersion curVer, GridCacheVersion rmvVer) {
            if (curVer == null && rmvVer == null)
                return true;

            GridCacheVersion kafkaVer =
                new GridCacheVersion(kafkaOrd.topVer(), kafkaOrd.nodeOrderDrId(), kafkaOrd.order());

            if (rmvVer != null)
                return kafkaVer.compareTo(rmvVer) > 0;

            return kafkaVer.compareTo(curVer) > 0;
        }

        /**
         * @param cacheId Group id.
         * @param parts Ignite partitions.
         * @param kafkaPart Kafka partition.
         */
        public void addCachePartition(int cacheId, List<Integer> parts, int kafkaPart) {
            cacheParts
                .computeIfAbsent(cacheId, key -> new HashMap<>())
                .computeIfAbsent(kafkaPart, key -> new HashSet<>())
                .addAll(parts);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Applier{, cacheParts=" + cacheParts + '}';
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            log.warning("Close applier!");

            for (KafkaConsumer<int[], byte[]> consumer : consumers.values())
                consumer.close(Duration.ofSeconds(3));

            consumers.clear();
        }

        /** */
        private void initConsumers() {
            for (Map.Entry<Integer, Map<Integer, Set<Integer>>> cacheEntry : cacheParts.entrySet()) {
                int cacheId = cacheEntry.getKey();
                Map<Integer, Set<Integer>> cacheParts = cacheEntry.getValue();

                for (Map.Entry<Integer, Set<Integer>> kafkaToIgniteParts : cacheParts.entrySet()) {
                    for (Integer ignitePart : kafkaToIgniteParts.getValue()) {
                        KafkaConsumer<int[], byte[]> consumer = new KafkaConsumer(new Properties());

                        consumers.put(new int[] {cacheId, ignitePart}, consumer);
                    }
                }

            }
        }
    }

    /**
     * @param value
     * @return
     */
    private EntryEvent<BinaryObject, BinaryObject> toEvent(byte[] value) {
        return null;
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