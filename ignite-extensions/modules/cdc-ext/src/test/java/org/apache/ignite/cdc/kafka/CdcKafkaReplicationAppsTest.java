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

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.startup.cmdline.CdcCommandLineStartup;

import static org.apache.ignite.cdc.kafka.KafkaToIgniteCdcStreamerConfiguration.DFLT_KAFKA_REQ_TIMEOUT;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class CdcKafkaReplicationAppsTest extends CdcKafkaReplicationTest {
    /** */
    public static final String INSTANCE_NAME = "INSTANCE_NAME";

    /** */
    public static final String DISCO_PORT = "DISCO_PORT";

    /** */
    public static final String DISCO_PORT_RANGE = "DISCO_PORT_RANGE";

    /** */
    public static final String REPLICATED_CACHE = "REPLICATED_CACHE";

    /** */
    public static final String TOPIC = "TOPIC";

    /** */
    public static final String METADATA_TOPIC = "METADATA_TOPIC";

    /** */
    public static final String CONSISTENT_ID = "CONSISTENT_ID";

    /** */
    public static final String PARTS = "PARTS";

    /** */
    public static final String PARTS_FROM = "PARTS_FROM";

    /** */
    public static final String PARTS_TO = "PARTS_TO";

    /** */
    public static final String THREAD_CNT = "THREAD_CNT";

    /** */
    public static final String MAX_BATCH_SIZE = "MAX_BATCH_SIZE";

    /** */
    public static final String KAFKA_REQ_TIMEOUT = "KAFKA_REQ_TIMEOUT";

    /** */
    public static final String PROPS_PATH = "PROPS_PATH";

    /** */
    public static final String HOST_ADDRESSES = "HOST_ADDRESSES";

    /** */
    private String kafkaPropsPath = null;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (kafkaPropsPath == null) {
            File file = File.createTempFile("kafka", "properties");

            file.deleteOnExit();

            try (FileOutputStream fos = new FileOutputStream(file)) {
                kafkaProperties().store(fos, null);
            }

            kafkaPropsPath = "file://" + file.getAbsolutePath();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<?> igniteToKafka(
        IgniteConfiguration igniteCfg,
        String topic,
        String metadataTopic,
        String cache
    ) {
        Map<String, String> params = new HashMap<>();

        params.put(INSTANCE_NAME, igniteCfg.getIgniteInstanceName());
        params.put(REPLICATED_CACHE, cache);
        params.put(TOPIC, topic);
        params.put(METADATA_TOPIC, metadataTopic);
        params.put(CONSISTENT_ID, String.valueOf(igniteCfg.getConsistentId()));
        params.put(PARTS, Integer.toString(DFLT_PARTS));
        params.put(MAX_BATCH_SIZE, Integer.toString(KEYS_CNT));
        params.put(PROPS_PATH, kafkaPropsPath);
        params.put(KAFKA_REQ_TIMEOUT, Long.toString(DFLT_KAFKA_REQ_TIMEOUT));

        return runAsync(
            () -> CdcCommandLineStartup.main(new String[] {prepareConfig("/replication/ignite-to-kafka.xml", params)})
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<?> kafkaToIgnite(
        String cacheName,
        String topic,
        String metadataTopic,
        IgniteConfiguration igniteCfg,
        IgniteEx[] dest,
        int partFrom,
        int partTo
    ) {
        Map<String, String> params = new HashMap<>();

        String cfg;

        if (clientType == ClientType.THIN_CLIENT) {
            cfg = "/replication/kafka-to-ignite-client.xml";

            String addresses = Arrays.stream(hostAddresses(dest)).map(addr -> "<value>" + addr + "</value>")
                .collect(Collectors.joining());

            params.put(HOST_ADDRESSES, addresses);
        }
        else {
            cfg = "/replication/kafka-to-ignite.xml";

            int discoPort = getFieldValue(igniteCfg.getDiscoverySpi(), "locPort");

            params.put(INSTANCE_NAME, igniteCfg.getIgniteInstanceName());
            params.put(DISCO_PORT, Integer.toString(discoPort));
            params.put(DISCO_PORT_RANGE, Integer.toString(discoPort + DFLT_PORT_RANGE));
        }

        params.put(REPLICATED_CACHE, cacheName);
        params.put(TOPIC, topic);
        params.put(METADATA_TOPIC, metadataTopic);
        params.put(PROPS_PATH, kafkaPropsPath);
        params.put(PARTS_FROM, Integer.toString(partFrom));
        params.put(PARTS_TO, Integer.toString(partTo));
        params.put(THREAD_CNT, Integer.toString((partTo - partFrom) / 3));
        params.put(KAFKA_REQ_TIMEOUT, Long.toString(DFLT_KAFKA_REQ_TIMEOUT));

        return runAsync(
            () -> KafkaToIgniteCommandLineStartup.main(new String[] {prepareConfig(cfg, params)})
        );
    }

    /** {@inheritDoc} */
    @Override protected void checkMetrics() {
        // Skip metrics check.
    }

    /** */
    private String prepareConfig(String path, Map<String, String> params) {
        try {
            String cfg = new String(Files.readAllBytes(Paths.get(CdcKafkaReplicationAppsTest.class.getResource(path).toURI())));

            for (String key : params.keySet()) {
                String subst = '{' + key + '}';

                while (cfg.contains(subst))
                    cfg = cfg.replace(subst, params.get(key));
            }

            File file = File.createTempFile("ignite-config", "xml");

            file.deleteOnExit();

            try (PrintWriter out = new PrintWriter(file)) {
                out.print(cfg);
            }

            return file.getAbsolutePath();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
