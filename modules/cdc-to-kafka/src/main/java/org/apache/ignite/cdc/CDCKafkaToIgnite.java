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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cdc.CDCIgniteToKafka.IGNITE_TO_KAFKA_TOPIC;
import static org.apache.ignite.cdc.Utils.property;

/**
 * CDC(Capture Data Change) Kafka to Ignite application.
 */
public class CDCKafkaToIgnite implements Runnable {
    private static final String KAFKA_TO_IGNITE_THREAD_COUNT = "kafka.to.ignite.thread.count";

    /** Ignite. */
    private final IgniteEx ign;

    /** Properties. */
    private final Properties kafkaProps;

    /** Replicated caches. */
    private final Set<Integer> caches;

    /** Executor service. */
    private final ExecutorService execSvc;

    /** */
    private final List<Applier> appliers = new ArrayList<>();

    /** */
    private final int thCnt;

    /** */
    public CDCKafkaToIgnite(IgniteEx ign, Properties kafkaProps, String[] cacheNames) {
        this.ign = ign;
        this.kafkaProps = kafkaProps;
        this.caches = Arrays.stream(cacheNames)
            .peek(cache -> Objects.requireNonNull(ign.cache(cache), cache + " not exists!"))
            .map(CU::cacheId).collect(Collectors.toSet());

        this.thCnt = Integer.parseInt(property(KAFKA_TO_IGNITE_THREAD_COUNT, kafkaProps, "10"));

        execSvc = Executors.newFixedThreadPool(thCnt, new ThreadFactory() {
            private final AtomicInteger cntr = new AtomicInteger();

            @Override public Thread newThread(@NotNull Runnable r) {
                Thread th = new Thread(r);

                th.setName("applier-thread-" + cntr.getAndIncrement());

                return th;
            }
        });
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
        String topic = property(IGNITE_TO_KAFKA_TOPIC, kafkaProps);

        for (int i = 0; i < thCnt; i++)
            appliers.add(new Applier(ign, kafkaProps, topic, caches));

        int kafkaPartitionsNum = KafkaUtils.initTopic(topic, kafkaProps);

        for (int i = 0; i < kafkaPartitionsNum; i++)
            appliers.get(i % thCnt).addPartition(i);

        try {
            for (int i = 0; i < thCnt; i++)
                execSvc.submit(appliers.get(i));

            execSvc.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            appliers.forEach(U::closeQuiet);
        }
    }
}