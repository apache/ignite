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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cdc.serde.JavaObjectSerializer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.apache.ignite.cdc.CDCIgniteToKafka.IGNITE_TO_KAFKA_CACHES;
import static org.apache.ignite.cdc.CDCIgniteToKafka.IGNITE_TO_KAFKA_TOPIC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test for cache with custom groupId created.
 * Test for one cache of groupId.
 */
public class KafkaToIgnitePluginTest extends GridCommonAbstractTest {
    /** */
    public static final String AP_TOPIC_NAME = "active-passive-topic";

    /** */
    public static final String AP_CACHE = "active-passive-cache";

    /** */
    public static final String ACTIVE_ACTIVE_CACHE = "active-active-cache";

    /** */
    private static Properties props;

    /** */
    private static IgniteEx[] source;

    /** */
    private static IgniteEx[] dest;

    /** */
    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    /** */
    private int commPort = TcpCommunicationSpi.DFLT_PORT;

    /** */
    private int discoPort = TcpDiscoverySpi.DFLT_PORT;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setLocalPort(discoPort)
                .setLocalPortRange(DFLT_PORT_RANGE)
                .setIpFinder(new TcpDiscoveryVmIpFinder() {{
                    setAddresses(Collections.singleton("127.0.0.1:" + discoPort + ".." + (discoPort + DFLT_PORT_RANGE)));
                }}))
            .setCommunicationSpi(new TcpCommunicationSpi()
                .setLocalPort(commPort)
                .setLocalPortRange(DFLT_PORT_RANGE));

        if (!cfg.isClientMode()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

            cfg.getDataStorageConfiguration()
                .setWalForceArchiveTimeout(5_000)
                .setCdcEnabled(true)
                .setWalMode(WALMode.FSYNC);

            cfg.setConsistentId(igniteInstanceName);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        kafka.start();

        if (props == null) {
            props = new Properties();

            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JavaObjectSerializer.class.getName());

            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-ignite-applier");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        }

        source = new IgniteEx[] {
            startGrid(1),
            startGrid(2),
            startClientGrid(3)
        };

        source[0].cluster().state(ACTIVE);
        source[0].cluster().tag("source");

        discoPort += DFLT_PORT_RANGE + 1;
        commPort += DFLT_PORT_RANGE + 1;

        dest = new IgniteEx[] {
            startGrid(4),
            startGrid(5)
        };

        assertFalse("source".equals(dest[0].cluster().tag()));

        dest[0].cluster().state(ACTIVE);
        dest[0].cluster().tag("destination");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        props = null;

        super.afterTestsStopped();
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_TO_KAFKA_TOPIC, value = AP_TOPIC_NAME)
    @WithSystemProperty(key = IGNITE_TO_KAFKA_CACHES, value = AP_CACHE)
    public void testActivePassiveReplication() throws Exception {
        streamToKafka(source[0], AP_TOPIC_NAME, AP_CACHE);
        streamToKafka(source[1], AP_TOPIC_NAME, AP_CACHE);

        Function<String, Runnable> genData = cacheName -> () -> {
            IgniteCache<Integer, Data> cache = source[source.length - 1].createCache(cacheName);

            FastCrc crc = new FastCrc();

            for (int i = 0; i < 50; i++) {
                byte[] payload = new byte[1024];

                ThreadLocalRandom.current().nextBytes(payload);

                crc.update(ByteBuffer.wrap(payload), 1024);

                cache.put(i, new Data(payload, crc.getValue()));

                crc.reset();
            }
        };

        IgniteCache<Integer, Data> destCache1 = dest[0].createCache(AP_CACHE);

        destCache1.put(1, new Data(null, 0));
        destCache1.remove(1);

        runAsync(genData.apply("cache-1"));
        runAsync(genData.apply(AP_CACHE));

        IgniteInternalFuture<?> kafkaToIgniteFut = runAsync(new CDCKafkaToIgnite(dest[0], props, AP_CACHE));

        waitForCondition(() -> {
            FastCrc crc = new FastCrc();

            for (int i = 0; i < 50; i++) {
                if (!destCache1.containsKey(i))
                    return false;

                Data data = destCache1.get(i);

                crc.reset();
                crc.update(ByteBuffer.wrap(data.payload), data.payload.length);

                assertEquals(crc.getValue(), data.crc);
            }

            return true;
        }, getTestTimeout());

        kafkaToIgniteFut.cancel();
    }

    /** */
    @Test
    public void testActiveActiveReplication() throws Exception {
        streamToKafka(source[0], "source-dest", ACTIVE_ACTIVE_CACHE);
        streamToKafka(source[1], "source-dest", ACTIVE_ACTIVE_CACHE);

        streamToKafka(dest[0], "dest-source", ACTIVE_ACTIVE_CACHE);
        streamToKafka(dest[1], "dest-source", ACTIVE_ACTIVE_CACHE);

    }

    /**
     * @param ign Ignite instance to watch for.
     * @param topic Kafka topic name.
     * @param caches Caches names to stream to kafka.
     * @return Future for CDC application.
     */
    private IgniteInternalFuture<?> streamToKafka(IgniteEx ign, String topic, String...caches) {
        return withSystemProperty(IGNITE_TO_KAFKA_TOPIC, topic, () ->
            withSystemProperty(IGNITE_TO_KAFKA_CACHES, String.join(",", caches), () -> {
                CDCIgniteToKafka cdc = new CDCIgniteToKafka();

                cdc.setKafkaProps(props);

                return runAsync(new IgniteCDC(ign.configuration(), cdc));
            })
        );
    }

    /**
     * @param key
     * @param value
     * @param run
     * @param <R>
     * @return
     */
    private static <R> R withSystemProperty(String key, String value, Supplier<R> run) {
        String prevVal = IgniteSystemProperties.getString(key);

        try {
            System.setProperty(key, value);

            return run.get();
        }
        finally {
            if (prevVal == null)
                System.clearProperty(key);
            else
                System.setProperty(key, prevVal);
        }
    }

    /** */
    private static class Data {
        /** */
        private final byte[] payload;

        /** */
        private final int crc;

        /** */
        public Data(byte[] payload, int crc) {
            this.payload = payload;
            this.crc = crc;
        }
    }
}
