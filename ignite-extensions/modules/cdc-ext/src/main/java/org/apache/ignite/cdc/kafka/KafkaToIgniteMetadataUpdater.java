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

package org.apache.ignite.cdc.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

import static org.apache.ignite.cdc.AbstractIgniteCdcStreamer.registerBinaryMeta;
import static org.apache.ignite.cdc.AbstractIgniteCdcStreamer.registerMapping;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/** */
public class KafkaToIgniteMetadataUpdater implements AutoCloseable, OffsetCommitCallback {
    /** Binary context. */
    private final BinaryContext ctx;

    /** Log. */
    private final IgniteLogger log;

    /** The maximum time to complete Kafka related requests, in milliseconds. */
    private final long kafkaReqTimeout;

    /** */
    private final KafkaConsumer<Void, byte[]> cnsmr;

    /** */
    private final AtomicLong rcvdEvts = new AtomicLong();

    /** Offsets from the last successful metadata update. */
    private volatile Map<TopicPartition, Long> offsets;

    /** Possible commit error. */
    private volatile Exception err;

    /** Metadata topic partitions. */
    private final Set<TopicPartition> parts;

    /**
     * @param ctx Binary context.
     * @param log Logger.
     * @param initProps Kafka properties.
     * @param streamerCfg Streamer configuration.
     */
    public KafkaToIgniteMetadataUpdater(
        BinaryContext ctx,
        IgniteLogger log,
        Properties initProps,
        KafkaToIgniteCdcStreamerConfiguration streamerCfg
    ) {
        this.ctx = ctx;
        this.kafkaReqTimeout = streamerCfg.getKafkaRequestTimeout();
        this.log = log.getLogger(KafkaToIgniteMetadataUpdater.class);

        Properties kafkaProps = new Properties();

        kafkaProps.putAll(initProps);
        kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class.getName());
        kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaProps.put(GROUP_ID_CONFIG, streamerCfg.getMetadataConsumerGroup() != null
            ? streamerCfg.getMetadataConsumerGroup()
            : ("ignite-metadata-update-" + streamerCfg.getKafkaPartsFrom() + "-" + streamerCfg.getKafkaPartsTo()));

        cnsmr = new KafkaConsumer<>(kafkaProps);

        String metaTopic = streamerCfg.getMetadataTopic();

        List<PartitionInfo> topicMeta = cnsmr.partitionsFor(metaTopic, Duration.ofMillis(kafkaReqTimeout));

        if (F.isEmpty(topicMeta))
            throw new IgniteException("Unknown topic: " + metaTopic);

        parts = topicMeta
            .stream()
            .map(pInfo -> new TopicPartition(metaTopic, pInfo.partition()))
            .collect(Collectors.toSet());

        if (parts.size() != 1) {
            this.log.warning("Metadata topic '" + metaTopic + "' has " + parts.size() + " partitions. " +
                "In order to read data with guaranteed order set number of partitions to 1");
        }

        cnsmr.subscribe(Collections.singletonList(metaTopic));
    }

    /** Polls all available records from metadata topic and applies it to Ignite. */
    public synchronized void updateMetadata() {
        if (err != null)
            throw new IgniteException(err);

        // If there are no new records in topic, method KafkaConsumer#poll blocks up to the specified timeout.
        // In order to eliminate this, we compare current offsets with the offsets from the last metadata update
        // (stored in 'offsets' field). If there are no offsets changes, polling cycle is skipped.
        Map<TopicPartition, Long> offsets0 = cnsmr.endOffsets(parts, Duration.ofMillis(kafkaReqTimeout));

        if (!F.isEmpty(offsets0) && F.eqNotOrdered(offsets, offsets0)) {
            if (log.isDebugEnabled())
                log.debug("Offsets unchanged, poll skipped");

            return;
        }

        while (true) {
            ConsumerRecords<Void, byte[]> recs = cnsmr.poll(Duration.ofMillis(kafkaReqTimeout));

            if (recs.count() == 0) {
                if (log.isDebugEnabled())
                    log.debug("Empty poll from meta topic");

                return;
            }

            if (log.isInfoEnabled())
                log.info("Polled from meta topic [rcvdEvts=" + rcvdEvts.addAndGet(recs.count()) + ']');

            for (ConsumerRecord<Void, byte[]> rec : recs) {
                Object data = IgniteUtils.fromBytes(rec.value());

                if (data instanceof BinaryMetadata)
                    registerBinaryMeta(ctx, log, (BinaryMetadata)data);
                else if (data instanceof TypeMapping)
                    registerMapping(ctx, log, (TypeMapping)data);
                else
                    throw new IllegalArgumentException("Unknown meta type[type=" + data + ']');
            }

            // Offsets updated only after commit.
            cnsmr.commitAsync(this);
        }
    }

    /** {@inheritDoc} */
    @Override public void onComplete(Map<TopicPartition, OffsetAndMetadata> committed, Exception err) {
        if (err != null) {
            log.warning("Commit error:", err);

            this.err = err;

            return;
        }

        Map<TopicPartition, Long> offsets0 = new HashMap<>();

        committed.forEach((tp, offAndMeta) -> offsets0.put(tp, offAndMeta.offset()));

        if (log.isDebugEnabled())
            log.debug("Offset committed: " + offsets0);

        offsets = offsets0;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        cnsmr.wakeup();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(KafkaToIgniteMetadataUpdater.class, this);
    }
}
