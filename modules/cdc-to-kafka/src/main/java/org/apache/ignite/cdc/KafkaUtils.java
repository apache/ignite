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

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import static org.apache.ignite.cdc.Utils.property;

/** */
public class KafkaUtils {
    /** */
    public static final int TIMEOUT_MIN = 1;

    /** Ignite to Kafka topic partitions number. */
    public static final String IGNITE_TO_KAFKA_NUM_PARTITIONS = "ignite.to.kafka.numpartitions";

    /** Ignite to Kafka topic partitions number. */
    public static final String IGNITE_TO_KAFKA_REPLICATION_FACTOR = "ignite.to.kafka.replication.factor";

    /**
     * Initialize Kafka topics.
     *
     * @param props Properties.
     */
    public static int initTopic(String topic, Properties props)
        throws InterruptedException, ExecutionException, TimeoutException {
        try (AdminClient adminCli = AdminClient.create(props)) {
            return initTopic0(topic, props, adminCli, 3);
        }
    }

    /**
     * @param topic
     * @param props
     * @param adminCli
     * @param guard
     * @return
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws ExecutionException
     */
    private static int initTopic0(String topic, Properties props, AdminClient adminCli, int guard)
        throws InterruptedException, TimeoutException, ExecutionException {
        if (guard == 0)
            throw new IllegalStateException("guard == 0");

        try {
            DescribeTopicsResult res = adminCli.describeTopics(Collections.singleton(topic));

            Map<String, TopicDescription> map = res.all().get(TIMEOUT_MIN, TimeUnit.MINUTES);

            if (!map.containsKey(topic))
                throw new IllegalStateException("Topic info not returned by describe topic request.");

            return map.get(topic).partitions().size();
        }
        catch (ExecutionException e) {
            e.printStackTrace();

            if (!(e.getCause() instanceof UnknownTopicOrPartitionException))
                throw e;

            // Waits some time for concurrent topic creation.
            Thread.sleep(ThreadLocalRandom.current().nextInt(1_500));

            return createTopic(topic, props, adminCli, guard);
        }
    }

    /**
     *
     * @param topic
     * @param props
     * @param adminCli
     * @param guard
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    private static int createTopic(String topic, Properties props, AdminClient adminCli, int guard)
        throws InterruptedException, ExecutionException, TimeoutException {
        int kafkaPartitionsNum = Integer.parseInt(property(IGNITE_TO_KAFKA_NUM_PARTITIONS, props, "32"));

        String replicationFactorStr = property(IGNITE_TO_KAFKA_REPLICATION_FACTOR, props, "1");

        try {
            adminCli.createTopics(Collections.singleton(new NewTopic(
                topic,
                kafkaPartitionsNum,
                replicationFactorStr == null ? 1 : Short.parseShort(replicationFactorStr))
            )).all().get(TIMEOUT_MIN, TimeUnit.MINUTES);

            return kafkaPartitionsNum;
        }
        catch (ExecutionException e) {
            e.printStackTrace();

            if (!(e.getCause() instanceof TopicExistsException))
                throw e;

            // Waits some time for concurrent topic creation.
            Thread.sleep(ThreadLocalRandom.current().nextInt(1_500));

            return initTopic0(topic, props, adminCli, guard - 1);
        }
    }
}
