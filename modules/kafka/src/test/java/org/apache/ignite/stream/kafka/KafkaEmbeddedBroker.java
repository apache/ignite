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

import org.apache.commons.io.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.zookeeper.server.*;

import kafka.admin.*;
import kafka.api.*;
import kafka.api.Request;
import kafka.producer.*;
import kafka.server.*;
import kafka.utils.*;
import org.I0Itec.zkclient.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Kafka Embedded Broker.
 */
public class KafkaEmbeddedBroker {

    /** Default ZooKeeper Host. */
    private static final String ZK_HOST = "localhost";

    /** Broker Port. */
    private static final int BROKER_PORT = 9092;

    /** ZooKeeper Connection Timeout. */
    private static final int ZK_CONNECTION_TIMEOUT = 6000;

    /** ZooKeeper Session Timeout. */
    private static final int ZK_SESSION_TIMEOUT = 6000;

    /** ZooKeeper port. */
    private static int zkPort = 0;

    /** Is ZooKeeper Ready. */
    private boolean zkReady;

    /** Kafka Config. */
    private KafkaConfig brokerConfig;

    /** Kafka Server. */
    private KafkaServer kafkaServer;

    /** ZooKeeper Client. */
    private ZkClient zkClient;

    /** Embedded ZooKeeper. */
    private EmbeddedZooKeeper zooKeeper;

    /**
     * Creates an embedded Kafka Broker.
     */
    public KafkaEmbeddedBroker() {
        try {
            setupEmbeddedZooKeeper();
            setupEmbeddedKafkaServer();
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("failed to start Kafka Broker " + e);
        }

    }

    /**
     * @return ZooKeeper Address.
     */
    public static String getZKAddress() {
        return ZK_HOST + ":" + zkPort;
    }

    /**
     * Creates a Topic.
     *
     * @param topic topic name
     * @param partitions number of paritions for the topic
     * @param replicationFactor replication factor
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void createTopic(String topic, final int partitions, final int replicationFactor)
        throws TimeoutException, InterruptedException {
        AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, new Properties());
        waitUntilMetadataIsPropagated(topic, 0, 10000, 100);
    }

    /**
     * Sends message to Kafka Broker.
     *
     * @param keyedMessages List of Keyed Messages.
     * @return Producer used to send the message.
     */
    public Producer sendMessages(List<KeyedMessage<String, String>> keyedMessages) {
        Producer<String, String> producer = new Producer<>(getProducerConfig());
        producer.send(scala.collection.JavaConversions.asScalaBuffer(keyedMessages));
        return producer;
    }

    /**
     * Shuts down Kafka Broker.
     *
     * @throws IOException
     */
    public void shutdown()
        throws IOException {

        zkReady = false;

        if (kafkaServer != null)
            kafkaServer.shutdown();

        List<String> logDirs = scala.collection.JavaConversions.asJavaList(brokerConfig.logDirs());

        for (String logDir : logDirs) {
            FileUtils.deleteDirectory(new File(logDir));
        }

        if (zkClient != null) {
            zkClient.close();
            zkClient = null;
        }

        if (zooKeeper != null) {

            try {
                zooKeeper.shutdown();
            }
            catch (IOException e) {
                // ignore
            }

            zooKeeper = null;
        }

    }

    /**
     * @return the Zookeeper Client
     */
    private ZkClient getZkClient() {
        A.ensure(zkReady, "Zookeeper not setup yet");
        A.notNull(zkClient, "Zookeeper client is not yet initialized");

        return zkClient;
    }

    /**
     * Checks if topic metadata is propagated.
     *
     * @param topic topic name
     * @param partition partition
     * @return true if propagated otherwise false
     */
    private boolean isMetadataPropagated(final String topic, final int partition) {
        final scala.Option<PartitionStateInfo> partitionStateOption = kafkaServer.apis().metadataCache().getPartitionInfo(
            topic, partition);
        if (partitionStateOption.isDefined()) {
            final PartitionStateInfo partitionState = partitionStateOption.get();
            final LeaderAndIsr leaderAndInSyncReplicas = partitionState.leaderIsrAndControllerEpoch().leaderAndIsr();

            if (ZkUtils.getLeaderForPartition(getZkClient(), topic, partition) != null
                && Request.isValidBrokerId(leaderAndInSyncReplicas.leader())
                && leaderAndInSyncReplicas.isr().size() >= 1)
                return true;

        }
        return false;
    }

    /**
     * Waits until metadata is propagated.
     *
     * @param topic topic name
     * @param partition partition
     * @param timeout timeout value in millis
     * @param interval interval in millis to sleep
     * @throws TimeoutException
     * @throws InterruptedException
     */
    private void waitUntilMetadataIsPropagated(final String topic, final int partition, final long timeout,
        final long interval) throws TimeoutException, InterruptedException {
        int attempt = 1;
        final long startTime = System.currentTimeMillis();

        while (true) {
            if (isMetadataPropagated(topic, partition))
                return;

            final long duration = System.currentTimeMillis() - startTime;

            if (duration < timeout)
                Thread.sleep(interval);
            else
                throw new TimeoutException("metadata propagate timed out, attempt=" + attempt);

            attempt++;
        }

    }

    /**
     * Sets up embedded Kafka Server
     *
     * @throws IOException
     */
    private void setupEmbeddedKafkaServer()
        throws IOException {
        A.ensure(zkReady, "Zookeeper should be setup before hand");

        brokerConfig = new KafkaConfig(getBrokerConfig());
        kafkaServer = new KafkaServer(brokerConfig, SystemTime$.MODULE$);
        kafkaServer.startup();
    }

    /**
     * Sets up embedded zooKeeper
     *
     * @throws IOException
     * @throws InterruptedException
     */
    private void setupEmbeddedZooKeeper()
        throws IOException, InterruptedException {
        EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper(ZK_HOST, zkPort);
        zooKeeper.startup();
        zkPort = zooKeeper.getActualPort();
        zkClient = new ZkClient(getZKAddress(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$);
        zkReady = true;
    }

    /**
     * @return Kafka Broker Address.
     */
    private static String getBrokerAddress() {
        return ZK_HOST + ":" + BROKER_PORT;
    }

    /**
     * Gets KafKa Brofer Config
     *
     * @return Kafka Broker Config
     * @throws IOException
     */
    private static Properties getBrokerConfig()
        throws IOException {
        Properties props = new Properties();
        props.put("broker.id", "0");
        props.put("host.name", ZK_HOST);
        props.put("port", "" + BROKER_PORT);
        props.put("log.dir", createTempDir("_cfg").getAbsolutePath());
        props.put("zookeeper.connect", getZKAddress());
        props.put("log.flush.interval.messages", "1");
        props.put("replica.socket.timeout.ms", "1500");
        return props;
    }

    /**
     * @return Kafka Producer Config
     */
    private static ProducerConfig getProducerConfig() {
        Properties props = new Properties();
        props.put("metadata.broker.list", getBrokerAddress());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "org.apache.ignite.kafka.SimplePartitioner");
        return new ProducerConfig(props);
    }

    /**
     * Creates Temp Directory
     *
     * @param prefix prefix
     * @return Created File.
     * @throws IOException
     */
    private static File createTempDir(final String prefix)
        throws IOException {
        final Path path = Files.createTempDirectory(prefix);
        return path.toFile();

    }

    /**
     * Creates Embedded ZooKeeper.
     */
    private static class EmbeddedZooKeeper {
        /** Default ZooKeeper Host. */
        private final String zkHost;

        /** Default ZooKeeper Port. */
        private final int zkPort;

        /** NIO Context Factory. */
        private NIOServerCnxnFactory factory;

        /** Snapshot Directory. */
        private File snapshotDir;

        /** Log Directory. */
        private File logDir;

        /**
         * Creates an embedded Zookeeper
         * @param zkHost zookeeper host
         * @param zkPort zookeeper port
         */
        EmbeddedZooKeeper(final String zkHost, final int zkPort) {
            this.zkHost = zkHost;
            this.zkPort = zkPort;
        }

        /**
         * Starts up ZooKeeper.
         *
         * @throws IOException
         * @throws InterruptedException
         */
        void startup()
            throws IOException, InterruptedException {
            snapshotDir = createTempDir("_ss");
            logDir = createTempDir("_log");
            ZooKeeperServer zooServer = new ZooKeeperServer(snapshotDir, logDir, 500);
            factory = new NIOServerCnxnFactory();
            factory.configure(new InetSocketAddress(zkHost, zkPort), 16);
            factory.startup(zooServer);
        }

        /**
         *
         * @return actual port zookeeper is started
         */
        int getActualPort() {
            return factory.getLocalPort();
        }

        /**
         * Shuts down ZooKeeper.
         *
         * @throws IOException
         */
        void shutdown()
            throws IOException {
            if (factory != null) {
                factory.shutdown();

                U.delete(snapshotDir);
                U.delete(logDir);
            }
        }
    }

}
