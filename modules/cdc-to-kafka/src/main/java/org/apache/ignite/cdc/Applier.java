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
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/** */
class Applier implements Runnable, AutoCloseable {
    /** */
    private final IgniteEx ign;

    /** */
    private final IgniteLogger log;

    /** */
    private volatile boolean closed;

    /** */
    private final Set<Integer> kafkaParts = new HashSet<>();

    /** */
    private final Map<Integer, IgniteInternalCache<BinaryObject, BinaryObject>> ignCaches = new HashMap<>();

    /** */
    private final Properties commonProps;

    /** */
    private final String topic;

    /** */
    private final Set<Integer> caches;

    /** */
    private final List<KafkaConsumer<Integer, byte[]>> consumers = new ArrayList<>();

    /** */
    private static final AtomicLong rcvdEvts = new AtomicLong();

    /** */
    public Applier(IgniteEx ign, Properties commonProps, String topic, Set<Integer> caches) {
        this.ign = ign;
        this.log = ign.log().getLogger(Applier.class);
        this.commonProps = commonProps;
        this.topic = topic;
        this.caches = caches;
    }

    /** */
    private void initConsumers() {
        for (int kafkaPart : kafkaParts) {
            Properties props = (Properties)commonProps.clone();

            KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(props);

            consumer.assign(Collections.singleton(new TopicPartition(topic, kafkaPart)));

            consumers.add(consumer);
        }
    }

    /** {@inheritDoc} */
    @Override public void run() {
        U.setCurrentIgniteName(ign.name());

        try {
            initConsumers();

            assert !consumers.isEmpty();

            Iterator<KafkaConsumer<Integer, byte[]>> consumerIter = Collections.emptyIterator();

            while (!closed) {
                log.warning("Fetching data from " + Thread.currentThread().getName() + '!');

                if (!consumerIter.hasNext())
                    consumerIter = consumers.iterator();

                poll(consumerIter.next());
            }
        }
        catch (WakeupException e) {
            if (!closed)
                log.error("Applier wakeup error!", e);
        }
        catch (Throwable e) {
            log.error("Applier error!", e);
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
     * @param consumer Data consumer.
     */
    private void poll(KafkaConsumer<Integer, byte[]> consumer) throws IgniteCheckedException {
        log.warning("Polling from consumer[assignments=" + consumer.assignment() + ",rcvdEvts=" + rcvdEvts.get() + ']');

        //TODO: try to reconnect on fail. Exit if no success.
        ConsumerRecords<Integer, byte[]> records = consumer.poll(Duration.ofSeconds(3));

        for (ConsumerRecord<Integer, byte[]> rec : records) {
            if (!caches.contains(rec.key()))
                continue;

            try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(rec.value()))) {
                apply((EntryEvent<BinaryObject, BinaryObject>)is.readObject());
            }
            catch (ClassNotFoundException | IOException e) {
                throw new IgniteCheckedException(e);
            }
        }

        consumer.commitSync(Duration.ofSeconds(3));
    }

    /**
     * @param evt Applies event to Ignite.
     * @param drId Data center replication id.
     */
    private void apply(EntryEvent<BinaryObject, BinaryObject> evt) throws IgniteCheckedException {
        rcvdEvts.incrementAndGet();

        IgniteInternalCache<BinaryObject, BinaryObject> cache = ignCaches.computeIfAbsent(evt.cacheId(), cacheId -> {
            for (String cacheName : ign.cacheNames()) {
                if (CU.cacheId(cacheName) == cacheId)
                    return ign.cachex(cacheName);
            }

            throw new IllegalStateException("Cache with id not found[cacheId=" + cacheId + ']');
        });

        EntryEventOrder kafkaOrd = evt.order();

        KeyCacheObject keyCacheObj = new KeyCacheObjectImpl(evt.key(), null, evt.partition());

        // TODO: try batch here.
        switch (evt.operation()) {
            case UPDATE:
                CacheObject cacheObj = new CacheObjectImpl(evt.value(), null);

                cache.putAllConflict(Collections.singletonMap(keyCacheObj,
                    new GridCacheDrInfo(cacheObj,
                        new GridCacheVersion(kafkaOrd.topVer(), kafkaOrd.nodeOrderDrId(), kafkaOrd.order()))));

                break;

            case DELETE:
                cache.removeAllConflict(Collections.singletonMap(keyCacheObj,
                        new GridCacheVersion(kafkaOrd.topVer(), kafkaOrd.nodeOrderDrId(), kafkaOrd.order())));

                break;

            default:
                throw new IllegalArgumentException("Unknown operation type: " + evt.operation());
        }
    }

    /**
     * @param kafkaPart Kafka partition.
     */
    public void addPartition(int kafkaPart) {
        kafkaParts.add(kafkaPart);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Applier{kafakParts=" + kafkaParts + '}';
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.warning("Close applier!");

        closed = true;

        consumers.forEach(KafkaConsumer::wakeup);
    }
}
