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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.resources.LoggerResource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.EVTS_CNT;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.EVTS_CNT_DESC;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.LAST_EVT_TIME;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.LAST_EVT_TIME_DESC;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.MAPPINGS_CNT;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.MAPPINGS_CNT_DESC;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.TYPES_CNT;
import static org.apache.ignite.cdc.IgniteToIgniteCdcStreamer.TYPES_CNT_DESC;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_KAFKA_REQ_TIMEOUT;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_MAX_BATCH_SIZE;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * Change Data Consumer that streams all data changes to Kafka topic.
 * {@link CdcEvent} spread across Kafka topic partitions with {@code {ignite_partition} % {kafka_topic_count}} formula.
 * Consumer will just fail in case of any error during write. Fail of consumer will lead to the fail of {@code ignite-cdc} application.
 * It expected that {@code ignite-cdc} will be configured for automatic restarts with the OS tool to failover temporary errors
 * such as Kafka unavailability or network issues.
 *
 * If you have plans to apply written messages to the other Ignite cluster in active-active manner,
 * e.g. concurrent updates of the same entry in other cluster is possible,
 * please, be aware of {@link CacheVersionConflictResolverImpl} conflict resolved.
 * Configuration of {@link CacheVersionConflictResolverImpl} can be found in {@link KafkaToIgniteCdcStreamer} documentation.
 *
 * @see CdcMain
 * @see KafkaToIgniteCdcStreamer
 * @see KafkaToIgniteClientCdcStreamer
 * @see CacheVersionConflictResolverImpl
 */
@IgniteExperimental
public class IgniteToKafkaCdcStreamer implements CdcConsumer {
    /** Default value for the flag that indicates whether entries only from primary nodes should be handled. */
    public static final boolean DFLT_IS_ONLY_PRIMARY = false;

    /** Bytes sent metric name. */
    public static final String BYTES_SENT = "BytesSent";

    /** Bytes sent metric description. */
    public static final String BYTES_SENT_DESCRIPTION = "Count of bytes sent.";

    /** Metadata updater marker. */
    public static final byte[] META_UPDATE_MARKER = U.longToBytes(0xC0FF1E);

    /** Log. */
    @LoggerResource
    private IgniteLogger log;

    /** Kafka producer. */
    private KafkaProducer<Integer, byte[]> producer;

    /** Handle only primary entry flag. */
    private boolean onlyPrimary = DFLT_IS_ONLY_PRIMARY;

    /** Topic to send data. */
    private String evtTopic;

    /** Topic to send metadata. */
    private String metadataTopic;

    /** Kafka topic partitions count. */
    private int kafkaParts;

    /** Kafka properties. */
    private Properties kafkaProps;

    /** Cache IDs. */
    private Set<Integer> cachesIds;

    /** Cache names. */
    private Collection<String> caches;

    /** Max batch size. */
    private int maxBatchSz = DFLT_MAX_BATCH_SIZE;

    /** The maximum time to complete Kafka related requests, in milliseconds. */
    private long kafkaReqTimeout = DFLT_KAFKA_REQ_TIMEOUT;

    /** Timestamp of last sent message. */
    private AtomicLongMetric lastMsgTs;

    /** Count of bytes sent to the Kafka. */
    private AtomicLongMetric bytesSnt;

    /** Count of sent events. */
    private AtomicLongMetric evtsCnt;

    /** Count of sent binary types. */
    protected AtomicLongMetric typesCnt;

    /** Count of sent mappings. */
    protected AtomicLongMetric mappingsCnt;

    /** Count of metadata updates. */
    protected byte metaUpdCnt = 0;

    /** */
    private List<Future<RecordMetadata>> futs;


    /** {@inheritDoc} */
    @Override public boolean onEvents(Iterator<CdcEvent> evts) {
        Iterator<CdcEvent> filtered = F.iterator(evts, e -> e, true, evt -> {
            if (log.isDebugEnabled())
                log.debug("Event received [evt=" + evt + ']');

            if (onlyPrimary && !evt.primary()) {
                if (log.isDebugEnabled())
                    log.debug("Event skipped because of primary flag [evt=" + evt + ']');

                return false;
            }

            if (evt.version().otherClusterVersion() != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Event skipped because of version [evt=" + evt +
                        ", otherClusterVersion=" + evt.version().otherClusterVersion() + ']');
                }

                return false;
            }

            if (!cachesIds.contains(evt.cacheId())) {
                if (log.isDebugEnabled())
                    log.debug("Event skipped because of cacheId [evt=" + evt + ']');

                return false;
            }

            return true;
        });

        sendAll(
            filtered,
            evt -> new ProducerRecord<>(
                evtTopic,
                evt.partition() % kafkaParts,
                evt.cacheId(),
                IgniteUtils.toBytes(evt)
            ),
            evtsCnt
        );

        return true;
    }

    /** {@inheritDoc} */
    @Override public void onTypes(Iterator<BinaryType> types) {
        sendAll(
            types,
            t -> new ProducerRecord<>(metadataTopic, IgniteUtils.toBytes(((BinaryTypeImpl)t).metadata())),
            typesCnt
        );

        sendMetaUpdatedMarkers();
    }

    /** {@inheritDoc} */
    @Override public void onMappings(Iterator<TypeMapping> mappings) {
        sendAll(
            mappings,
            m -> new ProducerRecord<>(metadataTopic, IgniteUtils.toBytes(m)),
            mappingsCnt
        );

        sendMetaUpdatedMarkers();
    }

    /** {@inheritDoc} */
    @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
        cacheEvents.forEachRemaining(e -> {
            // Just skip. Handle of cache events not supported.
        });
    }

    /** {@inheritDoc} */
    @Override public void onCacheDestroy(Iterator<Integer> caches) {
        caches.forEachRemaining(e -> {
            // Just skip. Handle of cache events not supported.
        });
    }

    /** Send marker(meta need to be updated) record to each partition of events topic. */
    private void sendMetaUpdatedMarkers() {
        sendAll(
            IntStream.range(0, kafkaParts).iterator(),
            p -> new ProducerRecord<>(evtTopic, p, null, META_UPDATE_MARKER),
            evtsCnt
        );

        if (log.isDebugEnabled())
            log.debug("Meta update markers sent.");
    }

    /** Send all data to Kafka. */
    private <T> void sendAll(
        Iterator<T> data,
        Function<T, ProducerRecord<Integer, byte[]>> toRec,
        AtomicLongMetric cntr
    ) {
        while (data.hasNext())
            sendOneBatch(data, toRec, cntr);
    }

    /** Send one batch. */
    private <T> void sendOneBatch(
        Iterator<T> data,
        Function<T, ProducerRecord<Integer, byte[]>> toRec,
        AtomicLongMetric cntr
    ) {
        while (data.hasNext() && futs.size() < maxBatchSz) {
            T item = data.next();

            if (log.isDebugEnabled())
                log.debug("Sent asynchronously [item=" + item + ']');

            ProducerRecord<Integer, byte[]> rec = toRec.apply(item);

            bytesSnt.add(rec.value().length);

            futs.add(producer.send(rec));
        }

        if (!futs.isEmpty()) {
            try {
                for (Future<RecordMetadata> fut : futs)
                    fut.get(kafkaReqTimeout, TimeUnit.MILLISECONDS);

                cntr.add(futs.size());
                lastMsgTs.value(System.currentTimeMillis());

                futs.clear();
            }
            catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }

            if (log.isInfoEnabled())
                log.info("Items processed [count=" + cntr.value() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void start(MetricRegistry mreg) {
        A.notNull(kafkaProps, "Kafka properties");
        A.notNull(evtTopic, "Kafka topic");
        A.notNull(metadataTopic, "Kafka metadata topic");
        A.notEmpty(caches, "caches");
        A.ensure(kafkaParts > 0, "The number of Kafka partitions must be explicitly set to a value greater than zero.");
        A.ensure(kafkaReqTimeout >= 0, "The Kafka request timeout cannot be negative.");

        kafkaProps.setProperty(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaProps.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        cachesIds = caches.stream()
            .map(CU::cacheId)
            .collect(Collectors.toSet());

        try {
            producer = new KafkaProducer<>(kafkaProps);

            if (log.isInfoEnabled()) {
                log.info("CDC Ignite To Kafka started [" +
                    "topic=" + evtTopic +
                    ", metadataTopic = " + metadataTopic +
                    ", onlyPrimary=" + onlyPrimary +
                    ", cacheIds=" + cachesIds + ']'
                );
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.evtsCnt = mreg.longMetric(EVTS_CNT, EVTS_CNT_DESC);
        this.lastMsgTs = mreg.longMetric(LAST_EVT_TIME, LAST_EVT_TIME_DESC);
        this.bytesSnt = mreg.longMetric(BYTES_SENT, BYTES_SENT_DESCRIPTION);
        this.typesCnt = mreg.longMetric(TYPES_CNT, TYPES_CNT_DESC);
        this.mappingsCnt = mreg.longMetric(MAPPINGS_CNT, MAPPINGS_CNT_DESC);

        futs = new ArrayList<>(maxBatchSz);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        producer.close();
    }

    /**
     * Sets whether entries only from primary nodes should be handled.
     *
     * @param onlyPrimary Whether entries only from primary nodes should be handled.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setOnlyPrimary(boolean onlyPrimary) {
        this.onlyPrimary = onlyPrimary;

        return this;
    }

    /**
     * Sets topic that is used to send data to Kafka.
     *
     * @param evtTopic Kafka topic.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setTopic(String evtTopic) {
        this.evtTopic = evtTopic;

        return this;
    }

    /**
     * Sets topic that is used to send metadata to Kafka.
     *
     * @param metadataTopic Metadata topic.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setMetadataTopic(String metadataTopic) {
        this.metadataTopic = metadataTopic;

        return this;
    }

    /**
     * Sets number of Kafka partitions for data topic.
     *
     * @param kafkaParts Number of Kafka partitions for data topic.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setKafkaPartitions(int kafkaParts) {
        this.kafkaParts = kafkaParts;

        return this;
    }

    /**
     * Sets cache names that participate in CDC.
     *
     * @param caches Cache names.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setCaches(Collection<String> caches) {
        this.caches = caches;

        return this;
    }

    /**
     * Sets maximum batch size.
     *
     * @param maxBatchSz Maximum batch size.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setMaxBatchSize(int maxBatchSz) {
        this.maxBatchSz = maxBatchSz;

        return this;
    }

    /**
     * Sets properties that are used to initiate connection to Kafka.
     *
     * @param kafkaProps Properties that are used to initiate connection to Kafka.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setKafkaProperties(Properties kafkaProps) {
        this.kafkaProps = kafkaProps;

        return this;
    }

    /**
     * Sets the maximum time to complete Kafka related requests, in milliseconds.
     * 
     * @param kafkaReqTimeout Timeout value.
     * @return {@code this} for chaining.
     */
    public IgniteToKafkaCdcStreamer setKafkaRequestTimeout(long kafkaReqTimeout) {
        this.kafkaReqTimeout = kafkaReqTimeout;

        return this;
    }
}
