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
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import kafka.admin.AdminUtils;
import kafka.api.LeaderAndIsr;
import kafka.api.PartitionStateInfo;
import kafka.api.Request;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Kafka Embedded Broker.
 */
public class KafkaEmbeddedBroker {
    /** Default ZooKeeper host. */
    private static final String ZK_HOST = "localhost";

    /** Broker port. */
    private static final int BROKER_PORT = 9092;

    /** ZooKeeper connection timeout. */
    private static final int ZK_CONNECTION_TIMEOUT = 6000;

    /** ZooKeeper session timeout. */
    private static final int ZK_SESSION_TIMEOUT = 6000;

    /** ZooKeeper port. */
    private static int zkPort = 0;

    /** Is ZooKeeper ready. */
    private boolean zkReady;

    /** Kafka config. */
    private KafkaConfig brokerCfg;

    /** Kafka server. */
    private KafkaServer kafkaSrv;

    /** ZooKeeper client. */
    private ZkClient zkClient;

    /** Embedded ZooKeeper. */
    private EmbeddedZooKeeper zooKeeper;

    /**
     * Creates an embedded Kafka broker.
     */
    public KafkaEmbeddedBroker() {
        try {
            setupEmbeddedZooKeeper();

            setupEmbeddedKafkaServer();
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to start Kafka broker " + e);
        }
    }

    /**
     * @return ZooKeeper address.
     */
    public static String getZKAddress() {
        return ZK_HOST + ':' + zkPort;
    }

    /**
     * Creates a Topic.
     *
     * @param topic Topic name.
     * @param partitions Number of partitions for the topic.
     * @param replicationFactor Replication factor.
     * @throws TimeoutException If operation is timed out.
     * @throws InterruptedException If interrupted.
     */
    public void createTopic(String topic, int partitions, int replicationFactor)
        throws TimeoutException, InterruptedException {
        AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, new Properties());

        waitUntilMetadataIsPropagated(topic, 0, 10000, 100);
    }

    /**
     * Sends message to Kafka broker.
     *
     * @param keyedMessages List of keyed messages.
     * @return Producer used to send the message.
     */
    public Producer<String, String> sendMessages(List<KeyedMessage<String, String>> keyedMessages) {
        Producer<String, String> producer = new Producer<>(getProducerConfig());

        producer.send(scala.collection.JavaConversions.asScalaBuffer(keyedMessages));

        return producer;
    }

    /**
     * Shuts down Kafka broker.
     */
    public void shutdown() {
        zkReady = false;

        if (kafkaSrv != null)
            kafkaSrv.shutdown();

        List<String> logDirs = scala.collection.JavaConversions.asJavaList(brokerCfg.logDirs());

        for (String logDir : logDirs)
            U.delete(new File(logDir));

        if (zkClient != null) {
            zkClient.close();

            zkClient = null;
        }

        if (zooKeeper != null) {

            try {
                zooKeeper.shutdown();
            }
            catch (IOException e) {
                // No-op.
            }

            zooKeeper = null;
        }
    }

    /**
     * @return ZooKeeper client.
     */
    private ZkClient getZkClient() {
        A.ensure(zkReady, "Zookeeper not setup yet");
        A.notNull(zkClient, "Zookeeper client is not yet initialized");

        return zkClient;
    }

    /**
     * Checks if topic metadata is propagated.
     *
     * @param topic Topic name.
     * @param part Partition.
     * @return {@code True} if propagated, otherwise {@code false}.
     */
    private boolean isMetadataPropagated(String topic, int part) {
        scala.Option<PartitionStateInfo> partStateOption =
            kafkaSrv.apis().metadataCache().getPartitionInfo(topic, part);

        if (!partStateOption.isDefined())
            return false;

        PartitionStateInfo partState = partStateOption.get();

        LeaderAndIsr LeaderAndIsr = partState.leaderIsrAndControllerEpoch().leaderAndIsr();

        return ZkUtils.getLeaderForPartition(getZkClient(), topic, part) != null &&
            Request.isValidBrokerId(LeaderAndIsr.leader()) && LeaderAndIsr.isr().size() >= 1;
    }

    /**
     * Waits until metadata is propagated.
     *
     * @param topic Topic name.
     * @param part Partition.
     * @param timeout Timeout value in millis.
     * @param interval Interval in millis to sleep.
     * @throws TimeoutException If operation is timed out.
     * @throws InterruptedException If interrupted.
     */
    private void waitUntilMetadataIsPropagated(String topic, int part, long timeout, long interval)
        throws TimeoutException, InterruptedException {
        int attempt = 1;

        long startTime = System.currentTimeMillis();

        while (true) {
            if (isMetadataPropagated(topic, part))
                return;

            long duration = System.currentTimeMillis() - startTime;

            if (duration < timeout)
                Thread.sleep(interval);
            else
                throw new TimeoutException("Metadata propagation is timed out, attempt " + attempt);

            attempt++;
        }
    }

    /**
     * Sets up embedded Kafka server.
     *
     * @throws IOException If failed.
     */
    private void setupEmbeddedKafkaServer() throws IOException {
        A.ensure(zkReady, "Zookeeper should be setup before hand");

        brokerCfg = new KafkaConfig(getBrokerConfig());

        kafkaSrv = new KafkaServer(brokerCfg, SystemTime$.MODULE$);

        kafkaSrv.startup();
    }

    /**
     * Sets up embedded ZooKeeper.
     *
     * @throws IOException If failed.
     * @throws InterruptedException If interrupted.
     */
    private void setupEmbeddedZooKeeper() throws IOException, InterruptedException {
        EmbeddedZooKeeper zooKeeper = new EmbeddedZooKeeper(ZK_HOST, zkPort);

        zooKeeper.startup();

        zkPort = zooKeeper.getActualPort();

        zkClient = new ZkClient(getZKAddress(), ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$);

        zkReady = true;
    }

    /**
     * @return Kafka broker address.
     */
    private static String getBrokerAddress() {
        return ZK_HOST + ':' + BROKER_PORT;
    }

    /**
     * Gets Kafka broker config.
     *
     * @return Kafka broker config.
     * @throws IOException If failed.
     */
    private static Properties getBrokerConfig() throws IOException {
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
     * @return Kafka Producer config.
     */
    private static ProducerConfig getProducerConfig() {
        Properties props = new Properties();

        props.put("metadata.broker.list", getBrokerAddress());
        props.put("serializer.class", StringEncoder.class.getName());
        props.put("key.serializer.class", StringEncoder.class.getName());
        props.put("partitioner.class", SimplePartitioner.class.getName());

        return new ProducerConfig(props);
    }

    /**
     * Creates temp directory.
     *
     * @param prefix Prefix.
     * @return Created file.
     * @throws IOException If failed.
     */
    private static File createTempDir( String prefix) throws IOException {
        Path path = Files.createTempDirectory(prefix);

        return path.toFile();
    }

    /**
     * Creates embedded ZooKeeper.
     */
    private static class EmbeddedZooKeeper {
        /** Default ZooKeeper host. */
        private final String zkHost;

        /** Default ZooKeeper port. */
        private final int zkPort;

        /** NIO context factory. */
        private NIOServerCnxnFactory factory;

        /** Snapshot directory. */
        private File snapshotDir;

        /** Log directory. */
        private File logDir;

        /**
         * Creates an embedded ZooKeeper.
         *
         * @param zkHost ZooKeeper host.
         * @param zkPort ZooKeeper port.
         */
        EmbeddedZooKeeper(String zkHost, int zkPort) {
            this.zkHost = zkHost;
            this.zkPort = zkPort;
        }

        /**
         * Starts up ZooKeeper.
         *
         * @throws IOException If failed.
         * @throws InterruptedException If interrupted.
         */
        void startup() throws IOException, InterruptedException {
            snapshotDir = createTempDir("_ss");

            logDir = createTempDir("_log");

            ZooKeeperServer zkSrv = new ZooKeeperServer(snapshotDir, logDir, 500);

            factory = new NIOServerCnxnFactory();

            factory.configure(new InetSocketAddress(zkHost, zkPort), 16);

            factory.startup(zkSrv);
        }

        /**
         * @return Actual port ZooKeeper is started.
         */
        int getActualPort() {
            return factory.getLocalPort();
        }

        /**
         * Shuts down ZooKeeper.
         *
         * @throws IOException If failed.
         */
        void shutdown() throws IOException {
            if (factory != null) {
                factory.shutdown();

                U.delete(snapshotDir);

                U.delete(logDir);
            }
        }
    }
}