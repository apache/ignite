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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/** */
class Applier implements Runnable, AutoCloseable {
    /** cacheId -> kafkaPart -> List<ignitePart> */
    private final Map<Integer, Map<Integer, Set<Integer>>> cacheParts = new HashMap<>();

    /** */
    private final IgniteEx ign;

    /** */
    private final IgniteLogger log;

    /** */
    private volatile boolean closed;

    /** */
    private final Map<int[], KafkaConsumer<int[], byte[]>> consumers = new TreeMap<>(new Comparator<int[]>() {
        @Override public int compare(int[] o1, int[] o2) {
            Objects.requireNonNull(o1);
            Objects.requireNonNull(o2);

            int len = Math.min(o1.length, o2.length);

            for (int i=0; i<len; i++) {
                int res = Integer.compare(o1[i], o2[i]);

                if (res != 0)
                    return res;
            }

            return Integer.compare(o1.length, o2.length);
        }
    });

    /** */
    private final Map<Integer, IgniteCache<BinaryObject, BinaryObject>> ignCaches = new HashMap<>();

    /** */
    private final Properties commonProps;

    /** */
    public Applier(IgniteEx ign, Properties commonProps) {
        this.ign = ign;
        this.log = ign.log().getLogger(Applier.class);
        this.commonProps = commonProps;
    }

    /** */
    private void initConsumers() {
        int cnt = 0;

        for (Map.Entry<Integer, Map<Integer, Set<Integer>>> cacheEntry : cacheParts.entrySet()) {
            int cacheId = cacheEntry.getKey();
            Map<Integer, Set<Integer>> cacheParts = cacheEntry.getValue();

            for (Map.Entry<Integer, Set<Integer>> kafkaToIgniteParts : cacheParts.entrySet()) {
                int kafkaPart = kafkaToIgniteParts.getKey();

                for (Integer ignitePart : kafkaToIgniteParts.getValue()) {
                    Properties props = (Properties)commonProps.clone();

                    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                        "kafka-to-ignite-applier." + cacheId + '.' + ignitePart);

                    KafkaConsumer<int[], byte[]> consumer = new KafkaConsumer<>(props);

                    consumer.assign(Collections.singleton(new TopicPartition("replication-topic-" + cacheId, kafkaPart)));

                    consumers.put(new int[] {cacheId, ignitePart}, consumer);

                    cnt++;

                    if (cnt == 1)
                        return;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try {
            initConsumers();

            assert !consumers.isEmpty();

            Iterator<Map.Entry<int[], KafkaConsumer<int[], byte[]>>> consumerIter = Collections.emptyIterator();

            while (!closed) {
                log.warning("Fetching data from " + Thread.currentThread().getName() + '!');

                if (!consumerIter.hasNext())
                    consumerIter = consumers.entrySet().iterator();

                Map.Entry<int[], KafkaConsumer<int[], byte[]>> entry = consumerIter.next();

                poll(entry.getKey()[0], entry.getKey()[1], entry.getValue());
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
            for (KafkaConsumer<int[], byte[]> consumer : consumers.values()) {
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
     * @param cacheId Cache id.
     * @param partitionId Partition id.
     * @param consumer Data consumer.
     */
    private void poll(int cacheId, int partitionId, KafkaConsumer<int[], byte[]> consumer) throws InterruptedException, IgniteCheckedException {
        ConsumerRecords<int[], byte[]> records = consumer.poll(Duration.ofSeconds(3));

        for (ConsumerRecord<int[], byte[]> rec : records) {
            int[] cacheAndPart = rec.key();

            if (cacheId != cacheAndPart[0] || partitionId != cacheAndPart[1])
                continue;

            try(ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(rec.value()))) {
                apply((EntryEvent<BinaryObject, BinaryObject>)is.readObject());
            }
            catch (ClassNotFoundException | IOException e) {
                throw new IgniteCheckedException(e);
            }
        }
    }

    /**
     * @param evt Applies event to Ignite.
     */
    private void apply(EntryEvent<BinaryObject, BinaryObject> evt) {
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
    @Override public void close() {
        log.warning("Close applier!");

        closed = true;

        consumers.values().forEach(KafkaConsumer::wakeup);
    }
}