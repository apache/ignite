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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import static org.apache.ignite.cdc.EntryEventType.DELETE;
import static org.apache.ignite.cdc.EntryEventType.UPDATE;

/**
 * Thread that polls message from the Kafka topic partitions and applies those messages to the Ignite cahes.
 * It expected that messages was written to the Kafka by the {@link CDCIgniteToKafka} CDC consumer.
 * <p>
 * Each applier receive set of Kafka topic partitions to read and caches to process.
 * Applier creates consumer per partition because Kafka consumer reads not fair, consumer reads messages from specific partition while there is new messages in specific partition.
 * See <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-387%3A+Fair+Message+Consumption+Across+Partitions+in+KafkaConsumer">KIP-387</a> and <a href="https://issues.apache.org/jira/browse/KAFKA-3932">KAFKA-3932</a> for further information.
 * All consumers should belongs to the same consumer-group to ensure consistent reading.
 * Applier polls messages from each consumer in round-robin fashion.
 * <p>
 * Messages applied to Ignite using {@link IgniteInternalCache#putAllConflict(Map)}, {@link IgniteInternalCache#removeAllConflict(Map)}
 * these methods allows to provide {@link GridCacheVersion} of the entry to the Ignite so in case update conflicts they can be resolved by the {@link CacheVersionConflictResolver}.
 * <p>
 * In case of any error during read applier just fail. Fail of any applier will lead to the fail of {@link CDCKafkaToIgnite} application.
 * It expected that application will be configured for automatic restarts with the OS tool to failover temporary errors such as Kafka or Ignite unavailability.
 *
 * @see CDCKafkaToIgnite
 * @see CDCIgniteToKafka
 * @see IgniteInternalCache#putAllConflict(Map)
 * @see IgniteInternalCache#removeAllConflict(Map)
 * @see CacheVersionConflictResolver
 * @see GridCacheVersion
 * @see EntryEvent
 * @see EntryEventOrder
 */
class Applier implements Runnable, AutoCloseable {
    /** */
    public static final int MAX_BATCH_SZ = 256;

    /** Ignite instance. */
    private final IgniteEx ign;

    /** Log. */
    private final IgniteLogger log;

    /** Closed flag. Shared between all appliers. */
    private final AtomicBoolean closed;

    /** Kafka partitions to poll. */
    private final Set<Integer> kafkaParts = new HashSet<>();

    /** Caches. */
    private final Map<Integer, IgniteInternalCache<BinaryObject, BinaryObject>> ignCaches = new HashMap<>();

    /** Kafka properties. */
    private final Properties kafkaProps;

    /** Topic to read. */
    private final String topic;

    /** Caches ids to read. */
    private final Set<Integer> caches;

    /** Consumers. */
    private final List<KafkaConsumer<Integer, byte[]>> consumers = new ArrayList<>();

    /** */
    private static final AtomicLong rcvdEvts = new AtomicLong();

    /**
     * @param ign Ignite instance
     * @param kafkaProps Kafka properties.
     * @param topic Topic name.
     * @param caches Cache ids.
     * @param closed Closed flag.
     */
    public Applier(IgniteEx ign, Properties kafkaProps, String topic, Set<Integer> caches, AtomicBoolean closed) {
        assert !F.isEmpty(caches);

        this.ign = ign;
        this.kafkaProps = kafkaProps;
        this.topic = topic;
        this.caches = caches;
        this.closed = closed;

        log = ign.log().getLogger(Applier.class);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        assert !F.isEmpty(kafkaParts);

        U.setCurrentIgniteName(ign.name());

        try {
            for (int kafkaPart : kafkaParts) {
                KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(kafkaProps);

                consumer.assign(Collections.singleton(new TopicPartition(topic, kafkaPart)));

                consumers.add(consumer);
            }

            Iterator<KafkaConsumer<Integer, byte[]>> consumerIter = Collections.emptyIterator();

            while (!closed.get()) {
                if (!consumerIter.hasNext())
                    consumerIter = consumers.iterator();

                poll(consumerIter.next());
            }
        }
        catch (WakeupException e) {
            if (!closed.get())
                log.error("Applier wakeup error!", e);
        }
        catch (Throwable e) {
            log.error("Applier error!", e);

            closed.set(true);
        }
        finally {
            for (KafkaConsumer<Integer, byte[]> consumer : consumers) {
                try {
                    consumer.close(Duration.ofSeconds(3));
                }
                catch (Exception e) {
                    log.warning("Close error!", e);
                }
            }

            consumers.clear();
        }

        log.warning(Thread.currentThread().getName() + " - stoped!");
    }

    /**
     * Polls data from the specific consumer and applies it to the Ignite.
     * @param consumer Data consumer.
     */
    private void poll(KafkaConsumer<Integer, byte[]> consumer) throws IgniteCheckedException {
        ConsumerRecords<Integer, byte[]> records = consumer.poll(Duration.ofSeconds(3));

        log.warning("Polled from consumer[assignments=" + consumer.assignment() + ",rcvdEvts=" + rcvdEvts.addAndGet(records.count()) + ']');

        Map<KeyCacheObject, GridCacheDrInfo> updBatch = new HashMap<>();
        Map<KeyCacheObject, GridCacheVersion> rmvBatch = new HashMap<>();
        IgniteInternalCache<BinaryObject, BinaryObject> currCache = null;

        for (ConsumerRecord<Integer, byte[]> rec : records) {
            if (!caches.contains(rec.key()))
                continue;

            try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(rec.value()))) {
                EntryEvent<BinaryObject, BinaryObject> evt = (EntryEvent<BinaryObject, BinaryObject>)is.readObject();

                IgniteInternalCache<BinaryObject, BinaryObject> cache = ignCaches.computeIfAbsent(evt.cacheId(), cacheId -> {
                    for (String cacheName : ign.cacheNames()) {
                        if (CU.cacheId(cacheName) == cacheId)
                            return ign.cachex(cacheName);
                    }

                    throw new IllegalStateException("Cache with id not found[cacheId=" + cacheId + ']');
                });

                if (cache != currCache) {
                    if (!F.isEmpty(rmvBatch))
                        currCache.removeAllConflict(rmvBatch);

                    if (!F.isEmpty(updBatch))
                        currCache.putAllConflict(updBatch);

                    updBatch.clear();
                    rmvBatch.clear();

                    currCache = cache;
                }

                EntryEventOrder order = evt.order();

                KeyCacheObject key = new KeyCacheObjectImpl(evt.key(), null, evt.partition());

                switch (evt.operation()) {
                    case UPDATE:
                        applyIfRequired(updBatch, rmvBatch, currCache, key, UPDATE);

                        CacheObject val = new CacheObjectImpl(evt.value(), null);

                        updBatch.put(key, new GridCacheDrInfo(val,
                                new GridCacheVersion(order.topVer(), order.nodeOrderDrId(), order.order())));

                        break;

                    case DELETE:
                        applyIfRequired(updBatch, rmvBatch, currCache, key, DELETE);

                        rmvBatch.put(key, new GridCacheVersion(order.topVer(), order.nodeOrderDrId(), order.order()));

                        break;

                    default:
                        throw new IllegalArgumentException("Unknown operation type: " + evt.operation());
                }
            }
            catch (ClassNotFoundException | IOException e) {
                throw new IgniteCheckedException(e);
            }
        }

        if (currCache != null) {
            if (!F.isEmpty(rmvBatch))
                currCache.removeAllConflict(rmvBatch);

            if (!F.isEmpty(updBatch))
                currCache.putAllConflict(updBatch);
        }

        consumer.commitSync(Duration.ofSeconds(3));
    }

    /**
     * Applies data from {@code updMap} or {@code rmvBatch} to Ignite if required.
     *
     * @param updBatch Update map.
     * @param rmvBatch Remove map.
     * @param cache Current cache.
     * @param key Key.
     * @param op Operation.
     * @throws IgniteCheckedException
     */
    private void applyIfRequired(
        Map<KeyCacheObject, GridCacheDrInfo> updBatch,
        Map<KeyCacheObject, GridCacheVersion> rmvBatch,
        IgniteInternalCache<BinaryObject, BinaryObject> cache,
        KeyCacheObject key,
        EntryEventType op
    ) throws IgniteCheckedException {
        if (isApplyBatch(DELETE, rmvBatch, op, key)) {
            cache.removeAllConflict(rmvBatch);

            rmvBatch.clear();
        }

        if (isApplyBatch(UPDATE, updBatch, op, key)) {
            cache.putAllConflict(updBatch);

            updBatch.clear();
        }
    }

    /** @return {@code True} if update batch should be applied. */
    private boolean isApplyBatch(
        EntryEventType batchOp,
        Map<KeyCacheObject, ?> map,
        EntryEventType op,
        KeyCacheObject key
    ) {
        return (!F.isEmpty(map) && op != batchOp) ||
            map.size() >= MAX_BATCH_SZ ||
            map.containsKey(key);
    }

    /** @param kafkaPart Kafka partition to consumer by this applier. */
    public void addPartition(int kafkaPart) {
        kafkaParts.add(kafkaPart);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.warning("Close applier!");

        closed.set(true);

        consumers.forEach(KafkaConsumer::wakeup);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Applier.class, this);
    }
}
