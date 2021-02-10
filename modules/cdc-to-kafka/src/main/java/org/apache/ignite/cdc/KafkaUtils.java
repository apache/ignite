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
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

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
     * @param props Properties.
     */
    public static int initTopic(String topic, Properties props) throws InterruptedException, ExecutionException, TimeoutException {
        try (AdminClient adminCli = AdminClient.create(props)) {
            DescribeTopicsResult res = adminCli.describeTopics(Collections.singleton(topic));

            Map<String, TopicDescription> map = res.all().get(TIMEOUT_MIN, TimeUnit.MINUTES);

            if (map.containsKey(topic))
                return map.get(topic).partitions().size();

            int kafkaPartitionsNum = Integer.parseInt(
                Objects.requireNonNull(
                    property(IGNITE_TO_KAFKA_NUM_PARTITIONS, props)));

            String replicationFactorStr = property(IGNITE_TO_KAFKA_REPLICATION_FACTOR, props);

            adminCli.createTopics(Collections.singleton(new NewTopic(
                topic,
                kafkaPartitionsNum,
                replicationFactorStr == null ? 1 : Short.parseShort(replicationFactorStr))
            )).all().get(TIMEOUT_MIN, TimeUnit.MINUTES);

            return kafkaPartitionsNum;
        }
    }
}
