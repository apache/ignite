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
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.DataChangeConsumer;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Test for cache with custom groupId created.
 * Test for one cache of groupId.
 */
public class KafkaToIgnitePluginTest extends GridCommonAbstractTest {
    /** */
    private static Properties props;

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

            cfg.getDataStorageConfiguration()
                .setWalForceArchiveTimeout(1_000)
                .setCdcEnabled(true)
                .setWalMode(WALMode.FSYNC);

            cfg.setConsistentId(igniteInstanceName);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        kafka.start();

        if (props == null) {
            props = new Properties();

            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-to-ignite-applier");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1_000);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        props = null;

        super.afterTestsStopped();
    }

    /** */
    @Test
    @WithSystemProperty(key = CDCIgniteToKafka.IGNITE_TO_KAFKA_TOPIC, value = "replication-data")
    @WithSystemProperty(key = CDCIgniteToKafka.IGNITE_TO_KAFKA_CACHES, value = "cache-2")
    public void testPartitionSwitchOnNodeJoin() throws Exception {
        IgniteEx ign1 = startGrid(1);
        IgniteEx ign2 = startGrid(2);

        IgniteInternalFuture<?> fut1 =
            runAsync(new IgniteCDC(ign1.configuration(), new DataChangeConsumer<>(new CDCIgniteToKafka())));
        IgniteInternalFuture<?> fut2 =
            runAsync(new IgniteCDC(ign2.configuration(), new DataChangeConsumer<>(new CDCIgniteToKafka())));

        IgniteEx cli = startClientGrid(3);

        Function<String, Runnable> genData = cacheName -> () -> {
            IgniteCache<Integer, Data> cache = cli.createCache(cacheName);

            FastCrc crc = new FastCrc();

            for (int i=0; i<1000; i++) {
                byte[] payload = new byte[1024];

                ThreadLocalRandom.current().nextBytes(payload);

                crc.update(ByteBuffer.wrap(payload), 1024);

                cache.put(i, new Data(payload, crc.getValue()));

                crc.reset();
            }
        };

        IgniteInternalFuture<?> runFut1 = runAsync(genData.apply("cache-1"));
        IgniteInternalFuture<?> runFut2 = runAsync(genData.apply("cache-2"));

        runFut1.get(getTestTimeout());
        runFut2.get(getTestTimeout());

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

        /** */
        public byte[] getPayload() {
            return payload;
        }

        /** */
        public int getCrc() {
            return crc;
        }
    }
}
