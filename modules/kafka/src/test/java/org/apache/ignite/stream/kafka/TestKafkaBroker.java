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

package org.apache.ignite.stream.kafka;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.test.TestingServer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Tuple2;

/**
 * Kafka Test Broker.
 */
public class TestKafkaBroker {
    /** ZooKeeper connection timeout. */
    private static final int ZK_CONNECTION_TIMEOUT = 6000;

    /** ZooKeeper session timeout. */
    private static final int ZK_SESSION_TIMEOUT = 6000;

    /** ZooKeeper port. */
    private static final int ZK_PORT = 21811;

    /** Broker host. */
    private static final String BROKER_HOST = "localhost";

    /** Broker port. */
    private static final int BROKER_PORT = 9092;

    /** Kafka config. */
    private KafkaConfig kafkaCfg;

    /** Kafka server. */
    private KafkaServer kafkaSrv;

    /** ZooKeeper. */
    private TestingServer zkServer;

    /** Kafka Zookeeper utils. */
    private ZkUtils zkUtils;

    /**
     * Kafka broker constructor.
     */
    public TestKafkaBroker() {
        try {
            setupZooKeeper();

            setupKafkaServer();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start Kafka: " + e);
        }
    }

    /**
     * Creates a topic.
     *
     * @param topic Topic name.
     * @param partitions Number of partitions for the topic.
     * @param replicationFactor Replication factor.
     * @throws TimeoutException If operation is timed out.
     * @throws InterruptedException If interrupted.
     */
    public void createTopic(String topic, int partitions, int replicationFactor)
        throws TimeoutException, InterruptedException {
        List<KafkaServer> servers = new ArrayList<>();

        servers.add(kafkaSrv);

        TestUtils.createTopic(zkUtils, topic, partitions, replicationFactor,
            scala.collection.JavaConversions.asScalaBuffer(servers), new Properties());
    }

    /**
     * Sends a message to Kafka broker.
     *
     * @param records List of records.
     * @return Producer used to send the message.
     */
    public void sendMessages(List<ProducerRecord<String, String>> records) {
        Producer<String, String> producer = new KafkaProducer<>(getProducerConfig());

        for (ProducerRecord<String, String> rec : records)
            producer.send(rec);

        producer.flush();
        producer.close();
    }

    /**
     * Shuts down test Kafka broker.
     */
    public void shutdown() {
        if (zkUtils != null)
            zkUtils.close();

        if (kafkaSrv != null)
            kafkaSrv.shutdown();

        if (zkServer != null) {
            try {
                zkServer.stop();
            }
            catch (IOException ignored) {
                // No-op.
            }
        }

        List<String> logDirs = scala.collection.JavaConversions.seqAsJavaList(kafkaCfg.logDirs());

        for (String logDir : logDirs)
            U.delete(new File(logDir));
    }

    /**
     * Sets up test Kafka broker.
     *
     * @throws IOException If failed.
     */
    private void setupKafkaServer() throws IOException {
        kafkaCfg = new KafkaConfig(getKafkaConfig());

        kafkaSrv = TestUtils.createServer(kafkaCfg, SystemTime$.MODULE$);

        kafkaSrv.startup();
    }

    /**
     * Sets up ZooKeeper test server.
     *
     * @throws Exception If failed.
     */
    private void setupZooKeeper() throws Exception {
        zkServer = new TestingServer(ZK_PORT, true);

        Tuple2<ZkClient, ZkConnection> zkTuple = ZkUtils.createZkClientAndConnection(zkServer.getConnectString(),
            ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT);

        zkUtils = new ZkUtils(zkTuple._1(), zkTuple._2(), false);
    }

    /**
     * Obtains Kafka config.
     *
     * @return Kafka config.
     * @throws IOException If failed.
     */
    private Properties getKafkaConfig() throws IOException {
        Properties props = new Properties();

        props.put("broker.id", "0");
        props.put("zookeeper.connect", zkServer.getConnectString());
        props.put("host.name", BROKER_HOST);
        props.put("port", BROKER_PORT);
        props.put("offsets.topic.replication.factor", "1");
        props.put("log.dir", createTmpDir("_cfg").getAbsolutePath());
        props.put("log.flush.interval.messages", "1");
        props.put("log.flush.interval.ms", "10");

        return props;
    }

    /**
     * Obtains broker address.
     *
     * @return Kafka broker address.
     */
    public String getBrokerAddress() {
        return BROKER_HOST + ":" + BROKER_PORT;
    }

    /**
     * Obtains Zookeeper address.
     *
     * @return Zookeeper address.
     */
    public String getZookeeperAddress() {
        return BROKER_HOST + ":" + ZK_PORT;
    }

    /**
     * Obtains producer config.
     *
     * @return Kafka Producer config.
     */
    private Properties getProducerConfig() {
        Properties props = new Properties();

        props.put("bootstrap.servers", getBrokerAddress());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    /**
     * Creates temporary directory.
     *
     * @param prefix Prefix.
     * @return Created file.
     * @throws IOException If failed.
     */
    private static File createTmpDir(String prefix) throws IOException {
        Path path = Files.createTempDirectory(prefix);

        return path.toFile();
    }
}
