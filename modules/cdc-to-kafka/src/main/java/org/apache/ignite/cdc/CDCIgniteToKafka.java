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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import static org.apache.ignite.cdc.Utils.property;
import static org.apache.ignite.cdc.Utils.properties;

/**
 * CDC consumer that streams all data changes to Kafka.
 */
public class CDCIgniteToKafka implements DataChangeListener<BinaryObject, BinaryObject> {
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

    /** Timeout minutes. */
    public static final int TIMEOUT_MIN = 1;

    /** Kafka producer to stream events. */
    private KafkaProducer<int[], EntryEvent<BinaryObject, BinaryObject>> producer;

    /** Handle only primary entry flag. */
    private boolean onlyPrimary;

    /** Topic name. */
    private String topic;

    /** Number Kafka topic partitions. */
    private int kafkaPartitionsNum;

    /** */
    private Set<Integer> cachesIds;

    /** {@inheritDoc} */
    @Override public boolean onChange(Iterable<EntryEvent<BinaryObject, BinaryObject>> evts) {
        List<Future<RecordMetadata>> futs = new ArrayList<>();

        for (EntryEvent<BinaryObject, BinaryObject> evt : evts) {
            if (onlyPrimary && !evt.primary())
                continue;

            if (!cachesIds.isEmpty() && !cachesIds.contains(evt.cacheId()))
                continue;

            futs.add(producer.send(new ProducerRecord<>(
                topic,
                evt.partition() % kafkaPartitionsNum,
                new int[] { evt.cacheId(), evt.partition() },
                evt
            )));
        }

        try {
            for (Future<RecordMetadata> fut : futs)
                fut.get(TIMEOUT_MIN, TimeUnit.MINUTES);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    /** Start event consumer with possible error. */
    public void startx() throws Exception {
        Properties props = properties(System.getProperty(CDC_CONSUMER_IGNITE_TO_KAFKA_PROPS), ERR_MSG);

        topic = property(IGNITE_TO_KAFKA_TOPIC, props);

        kafkaPartitionsNum = KafkaUtils.initTopic(topic, props);

        onlyPrimary = Boolean.parseBoolean(property(IGNITE_TO_KAFKA_ONLY_PRIMARY, props));

        producer = new KafkaProducer<>(props);

        cachesIds = cachesIds(props);
    }

    /** {@inheritDoc} */
    @Override public void start(IgniteConfiguration configuration, IgniteLogger log) {
        try {
            startx();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private Set<Integer> cachesIds(Properties props) {
        String cacheNames = property(IGNITE_TO_KAFKA_CACHES, props);

        if (cacheNames == null || cacheNames.isEmpty())
            return Collections.emptySet();

        return Arrays.stream(cacheNames.split(","))
            .mapToInt(CU::cacheId)
            .boxed()
            .collect(Collectors.toSet());
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
