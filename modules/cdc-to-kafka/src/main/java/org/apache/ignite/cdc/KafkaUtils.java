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

/** Kafka Utils. */
public class KafkaUtils {
    /** Default kafka request timeout. */
    public static final int TIMEOUT_MIN = 1;

    /** Ignite to Kafka topic partitions number. */
    public static final String IGNITE_TO_KAFKA_NUM_PARTITIONS = "ignite.to.kafka.numpartitions";

    /** Ignite to Kafka topic partitions number. */
    public static final String IGNITE_TO_KAFKA_REPLICATION_FACTOR = "ignite.to.kafka.replication.factor";

    /**
     * Initialize Kafka topic.
     *
     * @param props Properties.
     */
    public static int initTopic(String topic, Properties props)
        throws InterruptedException, ExecutionException, TimeoutException {
        try (AdminClient adminCli = AdminClient.create(props)) {
            return createTopic(
                topic,
                Integer.parseInt(property(IGNITE_TO_KAFKA_NUM_PARTITIONS, props, "32")),
                property(IGNITE_TO_KAFKA_REPLICATION_FACTOR, props, "1"),
                adminCli
            );
        }
    }

    /**
     * Creates Kafka topic, returns partitions count if topic already exists.
     *
     * @param topic Topic name.
     * @param kafkaPartsCnt Kafka topic partitions count.
     * @param adminCli Admin client.
     * @return Kafka topic partitions count.
     */
    private static int createTopic(String topic, int kafkaPartsCnt, String replicationFactorStr, AdminClient adminCli)
        throws InterruptedException, ExecutionException, TimeoutException {
        try {
            adminCli.createTopics(Collections.singleton(new NewTopic(
                topic,
                kafkaPartsCnt,
                replicationFactorStr == null ? 1 : Short.parseShort(replicationFactorStr))
            )).all().get(TIMEOUT_MIN, TimeUnit.MINUTES);

            return kafkaPartsCnt;
        }
        catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException))
                throw e;

            // Waits some time for concurrent topic creation.
            Thread.sleep(ThreadLocalRandom.current().nextInt(1_500));

            return topicPartitionsCount(topic, adminCli, 10);
        }
    }

    /**
     * Query topic partitions count from Kafka cluster.
     *
     * @param topic Topic name.
     * @param adminCli Admin client.
     * @param guard Guard number.
     * @return Kafka topic partitions count.
     */
    private static int topicPartitionsCount(String topic, AdminClient adminCli, int guard)
        throws InterruptedException, TimeoutException, ExecutionException {
        if (guard == 0)
            throw new IllegalStateException("guard == 0, topic = " + topic);

        try {
            DescribeTopicsResult res = adminCli.describeTopics(Collections.singleton(topic));

            Map<String, TopicDescription> map = res.all().get(TIMEOUT_MIN, TimeUnit.MINUTES);

            if (!map.containsKey(topic))
                throw new IllegalStateException("Topic info not returned by describe topic request.");

            return map.get(topic).partitions().size();
        }
        catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException))
                throw e;

            // Waits some time for concurrent topic creation.
            Thread.sleep(1000 + ThreadLocalRandom.current().nextInt(500));

            return topicPartitionsCount(topic, adminCli, guard - 1);
        }
    }
}
