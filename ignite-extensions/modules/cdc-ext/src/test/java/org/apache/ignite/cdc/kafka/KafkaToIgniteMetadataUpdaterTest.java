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

package org.apache.ignite.cdc.kafka;

import java.util.Collections;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryNoopMetadataHandler;
import org.apache.ignite.internal.cdc.TypeMappingImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.Test;

import static org.apache.ignite.cdc.AbstractReplicationTest.ACTIVE_PASSIVE_CACHE;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationTest.DFLT_PARTS;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationTest.SRC_DEST_META_TOPIC;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationTest.SRC_DEST_TOPIC;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationTest.initKafka;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationTest.kafkaProperties;
import static org.apache.ignite.cdc.kafka.CdcKafkaReplicationTest.removeKafkaTopicsAndWait;
import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_KAFKA_REQ_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.logging.log4j.Level.DEBUG;

/**
 *
 */
public class KafkaToIgniteMetadataUpdaterTest extends GridCommonAbstractTest {
    /** Markers sent messages listener. */
    private static final LogListener MARKERS_LISTENER = LogListener.matches("Meta update markers sent.")
        .times(1)
        .build();

    /** Polled from meta topic message listener. */
    private static final LogListener POLLED_LISTENER = LogListener.matches("Polled from meta topic [rcvdEvts=1]")
        .times(1)
        .build();

    /** Poll skip messages listener. */
    private static final LogListener POLL_SKIP_LISTENER = LogListener.matches("Offsets unchanged, poll skipped")
        .times(1)
        .build();

    /** Kafka cluster. */
    private EmbeddedKafkaCluster kafka;

    /** Listening logger. */
    private ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        kafka = initKafka(kafka);

        listeningLog = new ListeningTestLogger(log);

        resetLog4j(DEBUG, false, IgniteToKafkaCdcStreamer.class.getName());
        resetLog4j(DEBUG, false, KafkaToIgniteMetadataUpdater.class.getName());

        listeningLog.registerListener(MARKERS_LISTENER);
        listeningLog.registerListener(POLLED_LISTENER);
        listeningLog.registerListener(POLL_SKIP_LISTENER);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        removeKafkaTopicsAndWait(kafka, getTestTimeout());

        MARKERS_LISTENER.reset();
        POLLED_LISTENER.reset();
        POLL_SKIP_LISTENER.reset();
    }

    /** */
    @Test
    public void testThrowsForUnknownTopic() {
        KafkaToIgniteCdcStreamerConfiguration cfg = streamerConfiguration();

        String topic = "not-existing-topic";

        cfg.setMetadataTopic(topic);

        assertThrows(null, () -> metadataUpdater(cfg), IgniteException.class, "Unknown topic: " + topic);
    }

    /** */
    @Test
    public void testUpdateMetadata() throws Exception {
        try (KafkaToIgniteMetadataUpdater updater = metadataUpdater()) {
            CdcConsumer cdcCnsmr = igniteToKafkaCdcStreamer();

            TypeMapping typeMapping = new TypeMappingImpl(1, "test", PlatformType.JAVA);

            // No messages expected.
            assertFalse(MARKERS_LISTENER.check());
            assertFalse(POLLED_LISTENER.check());
            assertFalse(POLL_SKIP_LISTENER.check());

            // Send metadata (type mapping).
            cdcCnsmr.onMappings(Collections.singleton(typeMapping).iterator());

            // Sent markers expected.
            assertTrue(waitForCondition(MARKERS_LISTENER::check, getTestTimeout()));
            assertFalse(POLLED_LISTENER.check());
            assertFalse(POLL_SKIP_LISTENER.check());

            // Polled from meta topic expected.
            updater.updateMetadata();
            assertTrue(POLLED_LISTENER.check());
            assertFalse(POLL_SKIP_LISTENER.check());

            // Poll skip expected.
            updater.updateMetadata();
            assertTrue(POLL_SKIP_LISTENER.check());
        }
    }

    /** */
    private IgniteToKafkaCdcStreamer igniteToKafkaCdcStreamer() {
        IgniteToKafkaCdcStreamer streamer = new IgniteToKafkaCdcStreamer()
            .setTopic(SRC_DEST_TOPIC)
            .setMetadataTopic(SRC_DEST_META_TOPIC)
            .setKafkaPartitions(DFLT_PARTS)
            .setKafkaProperties(kafkaProperties(kafka))
            .setCaches(Collections.singleton(ACTIVE_PASSIVE_CACHE))
            .setKafkaRequestTimeout(DFLT_KAFKA_REQ_TIMEOUT);

        GridTestUtils.setFieldValue(streamer, "log", listeningLog.getLogger(IgniteToKafkaCdcStreamer.class));

        streamer.start(new MetricRegistryImpl("test", null, null, log));

        return streamer;
    }

    /** */
    private KafkaToIgniteMetadataUpdater metadataUpdater() {
        return metadataUpdater(streamerConfiguration());
    }

    /** */
    private KafkaToIgniteMetadataUpdater metadataUpdater(KafkaToIgniteCdcStreamerConfiguration streamerCfg) {
        BinaryContext noOpCtx = new BinaryContext(BinaryNoopMetadataHandler.instance(), new IgniteConfiguration(), log) {
            @Override public boolean registerUserClassName(int typeId, String clsName, boolean failIfUnregistered,
                boolean onlyLocReg, byte platformId) {
                return true;
            }
        };

        return new KafkaToIgniteMetadataUpdater(noOpCtx, listeningLog, kafkaProperties(kafka), streamerCfg);
    }

    /** */
    private KafkaToIgniteCdcStreamerConfiguration streamerConfiguration() {
        KafkaToIgniteCdcStreamerConfiguration cfg = new KafkaToIgniteCdcStreamerConfiguration();
        cfg.setTopic(SRC_DEST_TOPIC);
        cfg.setMetadataTopic(SRC_DEST_META_TOPIC);
        cfg.setKafkaPartsFrom(0);
        cfg.setKafkaPartsTo(DFLT_PARTS);
        cfg.setKafkaRequestTimeout(DFLT_KAFKA_REQ_TIMEOUT);

        return cfg;
    }
}
