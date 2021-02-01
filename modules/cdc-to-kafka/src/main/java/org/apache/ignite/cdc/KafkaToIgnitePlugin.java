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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static org.apache.ignite.cdc.CDCIgniteToKafka.IGNITE_TO_KAFKA_TOPIC;
import static org.apache.ignite.cdc.KafkaToIgnite.KAFKA_TO_IGNITE_THREAD_COUNT;
import static org.apache.ignite.cdc.KafkaUtils.TIMEOUT_MIN;
import static org.apache.ignite.cdc.Utils.fromSystemOrProperty;

/**
 * Plugin to apply DataEntry from Kafka to Ignite.
 *
 * Expectations:
 *
 * 1. Same cache created on writer and reader side.
 *
 */
public class KafkaToIgnitePlugin implements IgnitePlugin {
    /** */
    public static final String PARTITIONS = "kafka.to.ignite.consumer.partitions";

    /** */
    private final PluginContext ctx;

    /** */
    private final IgniteEx ign;

    /** Properties. */
    private final Properties props;

    /** Executor service. */
    private ExecutorService execSvc;

    /** Topic name. */
    private final String topic;

    /** Kafka partitions count. */
    private int kafkaPartitionsNum;

    /** Map to cache ids. */
    private ConcurrentMap<Long, IgniteCache<BinaryObject, BinaryObject>> caches = new ConcurrentHashMap<>();

    /**
     * @param ctx Plugin context.
     */
    public KafkaToIgnitePlugin(PluginContext ctx, Properties props) {
        this.ctx = ctx;
        this.ign = (IgniteEx)ctx.grid();
        this.props = props;
        this.topic = fromSystemOrProperty(IGNITE_TO_KAFKA_TOPIC, props);
    }

    /** Start the plugin. */
    public void start() throws Exception {
        kafkaPartitionsNum = KafkaUtils.initTopic(topic, props);

        int thNum = Integer.parseInt(fromSystemOrProperty(KAFKA_TO_IGNITE_THREAD_COUNT, props));

        ExecutorService execSvc = Executors.newFixedThreadPool(thNum);

        for (int i = 0; i < thNum; i++)
            execSvc.submit(new KafkaToIgniteWorker());

        execSvc.shutdown();

        execSvc.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    }

    /** Start the plugin. */
    public void stop() {

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
            IgniteCache<BinaryObject, BinaryObject> cache = caches.computeIfAbsent(evt.cacheId(), cacheId -> {
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
