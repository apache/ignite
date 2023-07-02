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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cdc.kafka.KafkaToIgniteLoader.loadKafkaToIgniteStreamer;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Tests load {@link KafkaToIgniteCdcStreamer} from Srping xml file. */
public class KafkaToIgniteLoaderTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testLoadConfig() throws Exception {
        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/kafka-to-ignite-double-ignite-cfg.xml"),
            IgniteCheckedException.class,
            "Exact 1 IgniteConfiguration should be defined. Found 2"
        );

        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/kafka-to-ignite-without-kafka-properties.xml"),
            IgniteCheckedException.class,
            "Spring bean with provided name doesn't exist"
        );

        KafkaToIgniteCdcStreamer streamer = loadKafkaToIgniteStreamer("loader/kafka-to-ignite-correct.xml");

        assertNotNull(streamer);
    }

    /** */
    @Test
    public void testLoadIgniteClientConfig() throws Exception {
        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/thin/kafka-to-ignite-client-double-client-cfg.xml"),
            IgniteCheckedException.class,
            "Exact 1 ClientConfiguration should be defined. Found 2"
        );

        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/thin/kafka-to-ignite-client-without-kafka-properties.xml"),
            IgniteCheckedException.class,
            "Spring bean with provided name doesn't exist"
        );

        assertThrows(
            null,
            () -> loadKafkaToIgniteStreamer("loader/thin/kafka-to-ignite-client-with-ignite-cfg.xml"),
            IgniteCheckedException.class,
            "Either IgniteConfiguration or ClientConfiguration should be defined."
        );

        KafkaToIgniteClientCdcStreamer streamer = loadKafkaToIgniteStreamer("loader/thin/kafka-to-ignite-client-correct.xml");

        assertNotNull(streamer);
    }

    /** */
    @Test
    public void testInitSpringContextOnce() throws Exception {
        assertEquals(0, InitiationTestBean.initCnt.get());

        loadKafkaToIgniteStreamer("loader/kafka-to-ignite-initiation-context-test.xml");

        assertEquals(1, InitiationTestBean.initCnt.get());
    }

    /** */
    private static class InitiationTestBean {
        /** */
        static AtomicInteger initCnt = new AtomicInteger();

        /** */
        InitiationTestBean() {
            initCnt.incrementAndGet();
        }
    }
}
