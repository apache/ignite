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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cdc.serde.JavaObjectSerializer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.lang.GridTuple4;
import org.apache.ignite.internal.util.typedef.F;
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
 * Tests for kafka replication.
 */
public class KafkaToIgnitePluginTest extends GridCommonAbstractTest {
    /** */
    public static final String AP_TOPIC_NAME = "active-passive-topic";

    /** */
    public static final String AP_CACHE = "active-passive-cache";

    /** */
    public static final String ACTIVE_ACTIVE_CACHE = "active-active-cache";

    /** */
    public static final byte SOURCE_DRID = 26;

    /** */
    public static final byte DEST_DRID = 27;

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

    /** */
    private final Function<GridTuple4<String, IgniteEx, IntStream, Integer>, Runnable> genData = p -> () -> {
        String cacheName = p.get1();
        IgniteEx ign = p.get2();
        IntStream keys = p.get3();
        int iter = p.get4();

        IgniteCache<Integer, Data> cache = ign.getOrCreateCache(cacheName);

        FastCrc crc = new FastCrc();

        keys.forEach(i -> {
            byte[] payload = new byte[1024];

            ThreadLocalRandom.current().nextBytes(payload);

            crc.update(ByteBuffer.wrap(payload), 1024);

            cache.put(i, new Data(payload, crc.getValue(), iter));

            crc.reset();
        });
    };

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

        for (IgniteEx ex : source)
            ex.context().cache().context().versions().dataCenterId(SOURCE_DRID);

        discoPort += DFLT_PORT_RANGE + 1;
        commPort += DFLT_PORT_RANGE + 1;

        dest = new IgniteEx[] {
            startGrid(4),
            startGrid(5),
            startClientGrid(6)
        };

        assertFalse("source".equals(dest[0].cluster().tag()));

        dest[0].cluster().state(ACTIVE);
        dest[0].cluster().tag("destination");

        for (IgniteEx ex : dest)
            ex.context().cache().context().versions().dataCenterId(DEST_DRID);
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
        IgniteInternalFuture<?> fut1 = igniteToKafka(source[0], SOURCE_DRID, AP_TOPIC_NAME, AP_CACHE);
        IgniteInternalFuture<?> fut2 = igniteToKafka(source[1], SOURCE_DRID, AP_TOPIC_NAME, AP_CACHE);

        try {
            IgniteCache<Integer, Data> destCache1 = dest[0].createCache(AP_CACHE);

            //TODO: check conflicts on this kind of operation.
            destCache1.put(1, new Data(null, 0, 1));
            destCache1.remove(1);

            runAsync(genData.apply(F.t("cache-1", source[source.length - 1], IntStream.range(0, 50), 1)));
            runAsync(genData.apply(F.t(AP_CACHE, source[source.length - 1], IntStream.range(0, 50), 1)));

            IgniteInternalFuture<?> k2iFut = runAsync(new CDCKafkaToIgnite(dest[0], props, AP_CACHE));

            try {
                assertTrue(waitForCondition(() -> {
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
                }, getTestTimeout()));

                IntStream.range(0, 50).forEach(i -> source[source.length - 1].getOrCreateCache(AP_CACHE).remove(i));

                assertTrue(waitForCondition(() -> {
                    for (int i = 0; i < 50; i++) {
                        if (destCache1.containsKey(i))
                            return false;
                    }

                    return true;
                }, getTestTimeout()));
            }
            finally {
                k2iFut.cancel();
            }
        }
        finally {
            fut1.cancel();
            fut2.cancel();
        }
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_TO_KAFKA_CACHES, value = ACTIVE_ACTIVE_CACHE)
    public void testActiveActiveReplication() throws Exception {
        String sourceDestTopic = "source-dest";
        String destSourceTopic = "dest-source";

        IgniteCache<Integer, Data> sourceCache = source[0].getOrCreateCache(ACTIVE_ACTIVE_CACHE);
        IgniteCache<Integer, Data> destCache = dest[0].getOrCreateCache(ACTIVE_ACTIVE_CACHE);

        runAsync(genData.apply(F.t(ACTIVE_ACTIVE_CACHE, source[source.length - 1],
            IntStream.range(0, 100).filter(i -> i % 2 == 0), 1)));
        runAsync(genData.apply(F.t(ACTIVE_ACTIVE_CACHE, dest[dest.length - 1],
            IntStream.range(0, 100).filter(i -> i % 2 != 0), 1)));

        IgniteInternalFuture<?> cdcSrcFut1 = igniteToKafka(source[0], SOURCE_DRID, sourceDestTopic, ACTIVE_ACTIVE_CACHE);
        IgniteInternalFuture<?> cdcSrcFut2 = igniteToKafka(source[1], SOURCE_DRID, sourceDestTopic, ACTIVE_ACTIVE_CACHE);

        IgniteInternalFuture<?> cdcDestFut1 = igniteToKafka(dest[0], DEST_DRID, destSourceTopic, ACTIVE_ACTIVE_CACHE);
        IgniteInternalFuture<?> cdcDestFut2 = igniteToKafka(dest[1], DEST_DRID, destSourceTopic, ACTIVE_ACTIVE_CACHE);

        try {
            IgniteInternalFuture<?> k2iFut1 = runAsync(() -> {
                CDCKafkaToIgnite kafkaToIgn = new CDCKafkaToIgnite(dest[0], props, ACTIVE_ACTIVE_CACHE);

                kafkaToIgn.setTopic(sourceDestTopic);

                kafkaToIgn.run();
            });

            IgniteInternalFuture<?> k2iFut2 = runAsync(() -> {
                CDCKafkaToIgnite kafkaToIgn = new CDCKafkaToIgnite(source[0], props, ACTIVE_ACTIVE_CACHE);

                kafkaToIgn.setTopic(destSourceTopic);

                kafkaToIgn.run();
            });

            try {
                assertTrue(waitForCondition(() -> {
                    FastCrc crc = new FastCrc();

                    for (int i = 0; i < 100; i++) {
                        if (!sourceCache.containsKey(i) || !destCache.containsKey(i))
                            return false;

                        Data data = sourceCache.get(i);

                        assertEquals(data, destCache.get(i));

                        crc.reset();
                        crc.update(ByteBuffer.wrap(data.payload), data.payload.length);

                        assertEquals(crc.getValue(), data.crc);
                    }

                    return true;
                }, getTestTimeout()));
            }
            finally {
                k2iFut1.cancel();
                k2iFut2.cancel();
            }
        }
        finally {
            cdcSrcFut1.cancel();
            cdcSrcFut2.cancel();
            cdcDestFut1.cancel();
            cdcDestFut2.cancel();
        }
    }

    /**
     * @param ign Ignite instance to watch for.
     * @param drId Data center ID.
     * @param topic Kafka topic name.
     * @param caches Caches names to stream to kafka.
     * @return Future for CDC application.
     */
    private IgniteInternalFuture<?> igniteToKafka(IgniteEx ign, byte drId, String topic, String...caches) {
        return runAsync(() -> {
            CDCIgniteToKafka cdc = new CDCIgniteToKafka(topic, drId, new HashSet<>(Arrays.asList(caches)), false, props);

            cdc.setKafkaProps(props);

            new IgniteCDC(ign.configuration(), cdc).run();
        });
    }

    /** */
    private static class Data {
        /** */
        private final byte[] payload;

        /** */
        private final int crc;

        /** */
        private final int iter;

        /** */
        public Data(byte[] payload, int crc, int iter) {
            this.payload = payload;
            this.crc = crc;
            this.iter = iter;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Data data = (Data)o;
            return crc == data.crc && Arrays.equals(payload, data.payload);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(crc);
            result = 31 * result + Arrays.hashCode(payload);
            return result;
        }
    }
}
