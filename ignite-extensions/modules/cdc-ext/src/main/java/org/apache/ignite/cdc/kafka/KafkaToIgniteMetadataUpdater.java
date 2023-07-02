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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

import static org.apache.ignite.cdc.AbstractIgniteCdcStreamer.registerBinaryMeta;
import static org.apache.ignite.cdc.AbstractIgniteCdcStreamer.registerMapping;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/** */
public class KafkaToIgniteMetadataUpdater implements AutoCloseable {
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

        cnsmr.subscribe(Collections.singletonList(streamerCfg.getMetadataTopic()));
    }

    /** Polls all available records from metadata topic and applies it to Ignite. */
    public synchronized void updateMetadata() {
        while (true) {
            ConsumerRecords<Void, byte[]> recs = cnsmr.poll(Duration.ofMillis(kafkaReqTimeout));

            if (recs.count() == 0)
                return;

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

            cnsmr.commitSync(Duration.ofMillis(kafkaReqTimeout));
        }
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
