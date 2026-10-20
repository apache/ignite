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

package org.apache.ignite.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_LONG_RUNNING_CACHE_FUTURES_EVENTS_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_METRICS;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class GridCachePartitionExchangeManagerLongRunningFuturesEventsMetricTest extends GridCommonAbstractTest {
    /** */
    private static final String DUMP_TIMEOUT = "3000";

    /** */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** Matches the diagnostic log line title, capturing the total number of long running cache futures in the group. */
    private static final Pattern TOTAL_PATTERN = Pattern.compile("long running cache futures \\[total=(\\d+)]");

    /** Drop backup responses to make the data streamer futures hang. */
    private final AtomicBoolean dropResponses = new AtomicBoolean(true);

    /** Total number of non-empty {@code long running cache futures} log headers observed per grid node. */
    private final Map<String, AtomicLong> logHdrHits = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new CustomTcpCommunicationSpi());
        cfg.setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME).setBackups(1).setAtomicityMode(ATOMIC));

        logHdrHits.put(igniteInstanceName, new AtomicLong());

        ListeningTestLogger log = new ListeningTestLogger(GridAbstractTest.log);

        log.registerListener(msg -> {
            Matcher m = TOTAL_PATTERN.matcher(msg);

            if (m.find() && Long.parseLong(m.group(1)) > 0)
                logHdrHits.get(igniteInstanceName).incrementAndGet();
        });

        cfg.setGridLogger(log);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        logHdrHits.clear();

        stopAllGrids();
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = DUMP_TIMEOUT)
    public void testLongRunningCacheFuturesEventsCountPerDumpWindow() throws Exception {
        startGrids(2);

        LongMetric cnt0 = longRunningFuturesEventsMetric(grid(0));
        LongMetric cnt1 = longRunningFuturesEventsMetric(grid(1));

        Ignite client = startClientGrid(2);

        try (IgniteDataStreamer<Integer, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100; i++)
                streamer.addData(i, i);

            streamer.flush();

            assertTrue(waitForCondition(() -> cnt0.value() > 0 || cnt1.value() > 0, Long.parseLong(DUMP_TIMEOUT) * 2));

            assertTrue(cnt0.value() == logHdrHits.get(grid(0).name()).get());
            assertTrue(cnt1.value() == logHdrHits.get(grid(1).name()).get());
        }
        finally {
            dropResponses.set(false);
        }

        assertTrue("Metric was not reset to 0 after futures were completed",
            waitForCondition(() -> cnt0.value() == 0 && cnt1.value() == 0, Long.parseLong(DUMP_TIMEOUT) * 3));
    }

    /** */
    private LongMetric longRunningFuturesEventsMetric(IgniteEx ignite) {
        LongMetric metric = ignite.context().metric().registry(PME_METRICS).findMetric(PME_LONG_RUNNING_CACHE_FUTURES_EVENTS_CNT);

        assertNotNull(metric);

        return metric;
    }

    /** Custom communication SPI for simulating long running cache futures. */
    private class CustomTcpCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (msg instanceof GridIoMessage ioMsg) {
                if (ioMsg.message() instanceof GridDhtAtomicDeferredUpdateResponse && dropResponses.get())
                    return;
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
