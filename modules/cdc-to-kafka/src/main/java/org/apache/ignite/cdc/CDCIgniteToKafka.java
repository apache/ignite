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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cdc.conflictplugin.DrIdCacheVersionConflictResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import static org.apache.ignite.cdc.Utils.properties;
import static org.apache.ignite.cdc.Utils.property;

/**
 * CDC consumer that streams all data changes to Kafka topic.
 * {@link EntryEvent} spread across Kafka topic partitions with {@code {ignite_partition} % {kafka_topic_count}} formula.
 * In case of any error during write consumer just fail. Fail of consumer will lead to the fail of whole application.
 * It expected that CDC application will be configured for automatic restarts with the OS tool to failover temporary errors such as Kafka unavailability.
 *
 * If you have plans to apply written messages to the other Ignite cluster in active-active manner,
 * e.g. concurrent updates of the same entry in other cluster is possible, please, be aware of {@link DrIdCacheVersionConflictResolver} conflict resolved.
 * Configuration of {@link DrIdCacheVersionConflictResolver} can be found in {@link CDCKafkaToIgnite} documentation.
 *
 * @see IgniteCDC
 * @see CDCKafkaToIgnite
 * @see DrIdCacheVersionConflictResolver
 */
public class CDCIgniteToKafka implements CDCConsumer<BinaryObject, BinaryObject> {
    /** Default kafka topic name. */
    private static final String DFLT_TOPIC_NAME = "cdc-ignite";

    /** Path to the kafka properties file. */
    private static final String CDC_CONSUMER_IGNITE_TO_KAFKA_PROPS = "CDC_CONSUMER_IGNITE_TO_KAFKA_PROPS";

    /** Ignite to Kafka topic name. */
    public static final String IGNITE_TO_KAFKA_TOPIC = "ignite.to.kafka.topic";

    /** Ignite to Kafka only primary flag. */
    public static final String IGNITE_TO_KAFKA_ONLY_PRIMARY = "ignite.to.kafka.only.primary";

    /** Ignite to Kafka only primary flag. */
    public static final String IGNITE_TO_KAFKA_CACHES = "ignite.to.kafka.caches";

    /** Error message. */
    private static final String ERR_MSG = CDC_CONSUMER_IGNITE_TO_KAFKA_PROPS +
        " should point to the Kafka properties file.";

    /** Log. */
    private IgniteLogger log;

    /** Kafka producer to stream events. */
    private KafkaProducer<Integer, EntryEvent<BinaryObject, BinaryObject>> producer;

    /** Handle only primary entry flag. */
    private boolean onlyPrimary;

    /** Topic name. */
    private String topic;

    /** Number Kafka topic partitions. */
    private int kafkaPartitionsNum;

    /** Cache IDs. */
    private Set<Integer> cachesIds;

    /** Kafka properties. */
    private Properties kafkaProps;

    /** Count of sent messages.  */
    private long cntSntMsgs;

    /** */
    private boolean startFromProps;

    /** Empty constructor. */
    public CDCIgniteToKafka() {
        startFromProps = true;
    }

    /**
     * @param topic Topic name.
     * @param caches Cache names.
     * @param onlyPrimary If {@code true} then stream only events from primaries.
     * @param kafkaProps Kafpa properties.
     */
    public CDCIgniteToKafka(String topic, Set<String> caches, boolean onlyPrimary, Properties kafkaProps) {
        assert caches != null && !caches.isEmpty();

        this.topic = topic;
        this.onlyPrimary = onlyPrimary;
        this.kafkaProps = kafkaProps;

        cachesIds = caches.stream()
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override public boolean onChange(Iterator<EntryEvent<BinaryObject, BinaryObject>> evts) {
        List<Future<RecordMetadata>> futs = new ArrayList<>();

        evts.forEachRemaining(evt -> {
            if (onlyPrimary && !evt.primary())
                return;

            if (evt.order().otherDcOrder() != null)
                return;

            if (!cachesIds.isEmpty() && !cachesIds.contains(evt.cacheId()))
                return;

            cntSntMsgs++;

            futs.add(producer.send(new ProducerRecord<>(
                topic,
                evt.partition() % kafkaPartitionsNum,
                evt.cacheId(),
                evt
            )));
        });

        try {
            for (Future<RecordMetadata> fut : futs)
                fut.get(KafkaUtils.TIMEOUT_MIN, TimeUnit.MINUTES);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        log.info("cntSntMsgs=" + cntSntMsgs);

        return true;
    }

    /** Starts event consumer. */
    public void startFromProperties() throws Exception {
        kafkaProps = properties(System.getProperty(CDC_CONSUMER_IGNITE_TO_KAFKA_PROPS), ERR_MSG);

        topic = property(IGNITE_TO_KAFKA_TOPIC, kafkaProps, DFLT_TOPIC_NAME);

        cachesIds = cachesIds(kafkaProps);

        onlyPrimary = Boolean.parseBoolean(property(IGNITE_TO_KAFKA_ONLY_PRIMARY, kafkaProps, "false"));
    }

    /** {@inheritDoc} */
    @Override public void start(IgniteConfiguration configuration, IgniteLogger log) {
        this.log = log;

        try {
            if (startFromProps)
                startFromProperties();

            kafkaPartitionsNum = KafkaUtils.initTopic(topic, kafkaProps);

            producer = new KafkaProducer<>(kafkaProps);

            log.info("Ignite To Kafka started[topic=" + topic + ",onlyPrimary=" + onlyPrimary + ",cacheIds=" + cachesIds + ']');
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Reads cache ids from properties. */
    private Set<Integer> cachesIds(Properties props) {
        String cacheNames = property(IGNITE_TO_KAFKA_CACHES, props);

        if (cacheNames == null || cacheNames.isEmpty())
            return Collections.emptySet();

        return Arrays.stream(cacheNames.split(","))
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toSet());
    }

    /**
     * Sets kafka properties.
     *
     * @param kafkaProps Kafka properties.
     */
    public void setKafkaProps(Properties kafkaProps) {
        this.kafkaProps = kafkaProps;
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        producer.close();
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String id() {
        return "ignite-to-kafka";
    }
}
