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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cdc.AbstractCdcEventsApplier;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import static org.apache.ignite.cdc.kafka.IgniteToKafkaCdcStreamer.META_UPDATE_MARKER;

/**
 * Thread that polls message from the Kafka topic partitions and applies those messages to the Ignite caches.
 * It expected that messages was written to the Kafka by the {@link IgniteToKafkaCdcStreamer} Change Data Capture consumer.
 * <p>
 * Each {@code Applier} receive set of Kafka topic partitions to read and caches to process.
 * Applier creates consumer per partition because Kafka consumer reads not fair,
 * consumer reads messages from specific partition while there is new messages in specific partition.
 * See <a href=
 * "https://cwiki.apache.org/confluence/display/KAFKA/KIP-387%3A+Fair+Message+Consumption+Across+Partitions+in+KafkaConsumer">KIP-387</a>
 * and <a href="https://issues.apache.org/jira/browse/KAFKA-3932">KAFKA-3932</a> for further information.
 * All consumers should belongs to the same consumer-group to ensure consistent reading.
 * Applier polls messages from each consumer in round-robin fashion.
 * <p>
 * Messages applied to Ignite using {@link IgniteInternalCache#putAllConflict(Map)}, {@link IgniteInternalCache#removeAllConflict(Map)}
 * these methods allows to provide {@link GridCacheVersion} of the entry to the Ignite so in case update conflicts they can be resolved
 * by the {@link CacheVersionConflictResolver}.
 * <p>
 * In case of any error during read applier just fail.
 * Fail of any applier will lead to the fail of {@link KafkaToIgniteCdcStreamer} application.
 * It expected that application will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka or Ignite unavailability.
 *
 * @see KafkaToIgniteCdcStreamer
 * @see IgniteToKafkaCdcStreamer
 * @see IgniteInternalCache#putAllConflict(Map)
 * @see IgniteInternalCache#removeAllConflict(Map)
 * @see CacheVersionConflictResolver
 * @see GridCacheVersion
 * @see CdcEvent
 * @see CacheEntryVersion
 */
class KafkaToIgniteCdcStreamerApplier implements Runnable, AutoCloseable {
    /** Log. */
    private final IgniteLogger log;

    /** Closed flag. Shared between all appliers. */
    private final AtomicBoolean stopped;

    /** Kafka properties. */
    private final Properties kafkaProps;

    /** Topic to read. */
    private final String topic;

    /** Lower kafka partition (inclusive). */
    private final int kafkaPartFrom;

    /** Higher kafka partition (exclusive). */
    private final int kafkaPartTo;

    /** Caches ids to read. */
    private final Set<Integer> caches;

    /** The maximum time to complete Kafka related requests, in milliseconds. */
    private final long kafkaReqTimeout;

    /** Metadata updater. */
    private final KafkaToIgniteMetadataUpdater metaUpdr;

    /** Consumers. */
    private final List<KafkaConsumer<Integer, byte[]>> cnsmrs = new ArrayList<>();

    /** */
    private final AtomicLong rcvdEvts = new AtomicLong();

    /** Cdc events applier supplier. */
    private final Supplier<AbstractCdcEventsApplier> applierSupplier;

    /** Cdc events applier. */
    private AbstractCdcEventsApplier applier;

    /**
     * @param applierSupplier Cdc events applier supplier.
     * @param log Logger.
     * @param kafkaProps Kafka properties.
     * @param topic Topic name.
     * @param kafkaPartFrom Read from partition.
     * @param kafkaPartTo Read to partition.
     * @param caches Cache ids.
     * @param maxBatchSize Maximum batch size.
     * @param kafkaReqTimeout The maximum time to complete Kafka related requests, in milliseconds.
     * @param metaUpdr Metadata updater.
     * @param stopped Stopped flag.
     */
    public KafkaToIgniteCdcStreamerApplier(
        Supplier<AbstractCdcEventsApplier> applierSupplier,
        IgniteLogger log,
        Properties kafkaProps,
        String topic,
        int kafkaPartFrom,
        int kafkaPartTo,
        Set<Integer> caches,
        int maxBatchSize,
        long kafkaReqTimeout,
        KafkaToIgniteMetadataUpdater metaUpdr,
        AtomicBoolean stopped
    ) {
        this.applierSupplier = applierSupplier;
        this.kafkaProps = kafkaProps;
        this.topic = topic;
        this.kafkaPartFrom = kafkaPartFrom;
        this.kafkaPartTo = kafkaPartTo;
        this.caches = caches;
        this.kafkaReqTimeout = kafkaReqTimeout;
        this.metaUpdr = metaUpdr;
        this.stopped = stopped;
        this.log = log.getLogger(KafkaToIgniteCdcStreamerApplier.class);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        applier = applierSupplier.get();

        try {
            for (int kafkaPart = kafkaPartFrom; kafkaPart < kafkaPartTo; kafkaPart++) {
                KafkaConsumer<Integer, byte[]> cnsmr = new KafkaConsumer<>(kafkaProps);

                cnsmr.assign(Collections.singleton(new TopicPartition(topic, kafkaPart)));

                cnsmrs.add(cnsmr);
            }

            Iterator<KafkaConsumer<Integer, byte[]>> cnsmrIter = Collections.emptyIterator();

            while (!stopped.get()) {
                if (!cnsmrIter.hasNext())
                    cnsmrIter = cnsmrs.iterator();

                poll(cnsmrIter.next());
            }
        }
        catch (WakeupException e) {
            if (!stopped.get())
                log.error("Applier wakeup error!", e);
        }
        catch (Throwable e) {
            log.error("Applier error!", e);

            stopped.set(true);
        }
        finally {
            for (KafkaConsumer<Integer, byte[]> consumer : cnsmrs) {
                try {
                    consumer.close(Duration.ofMillis(kafkaReqTimeout));
                }
                catch (Exception e) {
                    log.warning("Close error!", e);
                }
            }

            cnsmrs.clear();
        }

        if (log.isInfoEnabled())
            log.info(Thread.currentThread().getName() + " - stopped!");
    }

    /**
     * Polls data from the specific consumer and applies it to the Ignite.
     * @param cnsmr Data consumer.
     */
    private void poll(KafkaConsumer<Integer, byte[]> cnsmr) throws IgniteCheckedException {
        ConsumerRecords<Integer, byte[]> recs = cnsmr.poll(Duration.ofMillis(kafkaReqTimeout));

        if (log.isInfoEnabled()) {
            log.info(
                "Polled from consumer [assignments=" + cnsmr.assignment() +
                    ", cnt=" + recs.count() +
                    ", rcvdEvts=" + rcvdEvts.addAndGet(recs.count()) + ']'
            );
        }

        applier.apply(F.iterator(recs, this::deserialize, true, this::filterAndPossiblyUpdateMetadata));

        cnsmr.commitSync(Duration.ofMillis(kafkaReqTimeout));
    }

    /**
     * Filter out {@link CdcEvent} records.
     * Updates metadata in case update metadata marker found.
     * @param rec Record to filter.
     * @return {@code True} if record should be pushed down.
     */
    private boolean filterAndPossiblyUpdateMetadata(ConsumerRecord<Integer, byte[]> rec) {
        byte[] val = rec.value();

        if (rec.key() == null && Arrays.equals(val, META_UPDATE_MARKER)) {
            metaUpdr.updateMetadata();

            return false;
        }

        return F.isEmpty(caches) || caches.contains(rec.key());
    }

    /**
     * @param rec Kafka record.
     * @return CDC event.
     */
    private CdcEvent deserialize(ConsumerRecord<Integer, byte[]> rec) {
        try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(rec.value()))) {
            return (CdcEvent)is.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.warning("Close applier!");

        metaUpdr.close();

        cnsmrs.forEach(KafkaConsumer::wakeup);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(KafkaToIgniteCdcStreamerApplier.class, this);
    }
}
