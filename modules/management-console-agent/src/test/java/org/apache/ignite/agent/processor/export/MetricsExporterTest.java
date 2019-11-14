/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.processor.export;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.dto.metric.MetricResponse;
import org.apache.ignite.agent.dto.metric.MetricSchema;
import org.apache.ignite.agent.dto.metric.MetricValueConsumer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class MetricsExporterTest {
    /** */
    private static MetricsExporter exporter;

    /** */
    private static final IgniteLogger LOG = new NullLogger();

    /**
     * @throws Exception if failed.
     */
    @Before
    public void setUp() throws Exception {
        exporter = new MetricsExporter(new StandaloneGridKernalContext(LOG, null, null));
    }

    /** */
    @Test
    public void testResponse() {
        String userTag = "testUserTag";

        doTestResponse(userTag);
    }

    /** */
    @Test
    public void testResponseWithoutUserTag() {
        String userTag = null;

        doTestResponse(userTag);
    }

    /** */
    private void doTestResponse(String userTag) {
        UUID clusterId = UUID.randomUUID();

        String consistentId = "testConsistentId";

        Map<String, MetricRegistry> metrics = generateMetrics();

        MetricResponse msg = exporter.metricMessage(clusterId, userTag, consistentId, metrics);

        assertEquals(clusterId, msg.clusterId());

        assertEquals(userTag, msg.userTag());

        assertEquals(consistentId, msg.consistentId());

        MetricSchema schema = msg.schema();

        Map<String, Byte> map = new HashMap<>();

        for (MetricRegistry reg : metrics.values()) {
            for (Metric m : reg) {
                byte type;

                if (m instanceof BooleanMetric)
                    type = 0;
                else if (m instanceof IntMetric)
                    type = 1;
                else if (m instanceof LongMetric)
                    type = 2;
                else if (m instanceof DoubleMetric)
                    type = 3;
                else
                    throw new IllegalArgumentException("Unknown metric type.");

                map.put(m.name(), type);
            }
        }

        msg.processData(schema, new MetricValueConsumer() {
            @Override public void onBoolean(String name, boolean val) {
                assertEquals((byte)0, (byte)map.remove(name));
            }

            @Override public void onInt(String name, int val) {
                assertEquals((byte)1, (byte)map.remove(name));
            }

            @Override public void onLong(String name, long val) {
                assertEquals((byte)2, (byte)map.remove(name));
            }

            @Override public void onDouble(String name, double val) {
                assertEquals((byte)3, (byte)map.remove(name));
            }
        });


        assertTrue(map.isEmpty());
    }

    /**
     * @return Metrics.
     */
    private Map<String, MetricRegistry> generateMetrics() {
        Map<String, MetricRegistry> metrics = new TreeMap<>();

        String namePref = "metric.name.";

        String grpPref = "grp.";

        for (int i = 0; i < 4; i++) {
            String grpName = grpPref + i;

            MetricRegistry reg = new MetricRegistry(grpName, grpName, LOG);

            metrics.put(grpName, reg);

            for (int j = 0; j < 16;) {
                reg.booleanMetric(namePref + "bool." + j++, "description");

                reg.intMetric(namePref + "int." + j++, "description");

                reg.longMetric(namePref + "long." + j++, "description");

                reg.doubleMetric(namePref + "double." + j++, "description");
            }
        }

        return metrics;
    }
}
