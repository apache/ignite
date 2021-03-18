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
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.cdc.conflictplugin.CDCCacheConflictResolutionManager;
import org.apache.ignite.cdc.conflictplugin.DrIdCacheVersionConflictResolver;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cdc.CDCIgniteToKafka.IGNITE_TO_KAFKA_TOPIC;
import static org.apache.ignite.cdc.Utils.property;

/**
 * Main class of CDC(Capture Data Change) Kafka to Ignite application.
 * This application is counterpart of {@link CDCIgniteToKafka} CDC consumer.
 * Application runs several {@link Applier} thread to read Kafka topic and apply {@link EntryEvent} to Ignite.
 * <p>
 * Each applier receive even number of kafka topic partition to read.
 * <p>
 * In case of any error during read applier just fail. Fail of any applier will lead to the fail of whole application.
 * It expected that application will be configured for automatic restarts with the OS tool to failover temporary errors such as Kafka or Ignite unavailability.
 * <p>
 * To resolve possible update conflicts(in case of concurrent update in source and destination Ignite clusters) real-world deployments should use
 * some conflict resolver, for example {@link DrIdCacheVersionConflictResolver}.
 * Example of Ignite configuration with the conflict resolver:
 * <pre>
 * {@code
 * CDCReplicationConfigurationPluginProvider cfgPlugin = new CDCReplicationConfigurationPluginProvider();
 *
 * cfgPlugin.setDrId(drId); // Data center replication ID.
 * cfgPlugin.setCaches(new HashSet<>(Arrays.asList("my-cache", "some-other-cache"))); // Caches to replicate.
 *
 * IgniteConfiguration cfg = ...;
 *
 * cfg.setPluginProviders(cfgPlugin);
 * }
 * </pre>
 * Please, see {@link CDCCacheConflictResolutionManager} for additional information.
 *
 * Properties list:
 * <ul>
 *  <li>{@link #KAFKA_TO_IGNITE_THREAD_COUNT} - count of {@link Applier} threads.</li>
 *  <li>{@link CDCIgniteToKafka#IGNITE_TO_KAFKA_TOPIC} - Kafka topic name if not provided in constructor.</li>
 * </ul>
 *
 * @see IgniteCDC
 * @see CDCIgniteToKafka
 * @see EntryEvent
 * @see Applier
 */
public class CDCKafkaToIgnite implements Runnable {
    /** Property to define number of {@link Applier} threads. */
    private static final String KAFKA_TO_IGNITE_THREAD_COUNT = "kafka.to.ignite.thread.count";

    /** Ignite instance shared between all {@link Applier}. */
    private final IgniteEx ign;

    /** Kafka consumer properties. */
    private final Properties kafkaProps;

    /** Replicated caches. */
    private final Set<Integer> caches;

    /** Executor service to run {@link Applier} instances. */
    private final ExecutorService execSvc;

    /** Appliers. */
    private final List<Applier> appliers = new ArrayList<>();

    /** Threads count. */
    private final int thCnt;

    /** Kafka topic to read. */
    private String topic;

    /**
     * @param ign Ignite instance
     * @param kafkaProps Kafka properties.
     * @param topic Topic name.
     * @param cacheNames Cache names.
     */
    public CDCKafkaToIgnite(IgniteEx ign, Properties kafkaProps, String topic, String... cacheNames) {
        this.ign = ign;
        this.kafkaProps = kafkaProps;
        this.topic = topic;
        this.caches = Arrays.stream(cacheNames)
            .peek(cache -> Objects.requireNonNull(ign.cache(cache), cache + " not exists!"))
            .map(CU::cacheId).collect(Collectors.toSet());

        this.thCnt = Integer.parseInt(property(KAFKA_TO_IGNITE_THREAD_COUNT, kafkaProps, "3"));

        execSvc = Executors.newFixedThreadPool(thCnt, new ThreadFactory() {
            private final AtomicInteger cntr = new AtomicInteger();

            @Override public Thread newThread(@NotNull Runnable r) {
                Thread th = new Thread(r);

                th.setName("applier-thread-" + cntr.getAndIncrement());

                return th;
            }
        });

        if (!kafkaProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG))
            throw new IllegalArgumentException("Kafka properties don't contains " + ConsumerConfig.GROUP_ID_CONFIG);

        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }

    /** Runs CDC. */
    @Override public void run() {
        try {
            runX();
        }
        catch (Exception e) {
            e.printStackTrace();

            throw new RuntimeException(e);
        }
    }

    /** Runs CDC application with possible exception. */
    public void runX() throws Exception {
        if (topic == null)
            topic = property(IGNITE_TO_KAFKA_TOPIC, kafkaProps);

        AtomicBoolean closed = new AtomicBoolean();

        for (int i = 0; i < thCnt; i++)
            appliers.add(new Applier(ign, kafkaProps, topic, caches, closed));

        int kafkaPartitionsNum = KafkaUtils.initTopic(topic, kafkaProps);

        for (int i = 0; i < kafkaPartitionsNum; i++)
            appliers.get(i % thCnt).addPartition(i);

        try {
            for (int i = 0; i < thCnt; i++)
                execSvc.submit(appliers.get(i));

            execSvc.shutdown();

            execSvc.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            closed.set(true);

            appliers.forEach(U::closeQuiet);
        }
    }
}
