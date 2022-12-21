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

package org.apache.ignite.internal.processors.cache.objects;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_CONNECTOR_METRICS;
import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_NAME;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_NAME;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheObjectsCompressionConsumptionTest extends AbstractCacheObjectsCompressionTest {
    /** Region name. */
    private static final String REGION_NAME = "region";

    /** Thin client. */
    @Parameterized.Parameter
    public boolean thinClient;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "thinClient={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setName(REGION_NAME)
                        .setMetricsEnabled(true)
                        .setMaxSize(1000L * 1024 * 1024)
                        .setInitialSize(1000L * 1024 * 1024))
                .setMetricsEnabled(true));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testString() throws Exception {
        testConsumption((i) -> i, (i) -> HUGE_STRING + i);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testWrappedString() throws Exception {
        testConsumption((i) -> i, (i) -> new StringData(HUGE_STRING + i));
    }

    /**
     *
     */
    @org.junit.Test
    public void testIncompressible() {
        GridTestUtils.assertThrowsWithCause(
            () -> {
                testConsumption((i) -> i, (i) -> i);

                return null;
            }, AssertionError.class);
    }

    /**
     * @throws Exception If failed.
     */
    private void testConsumption(Function<Integer, Object> keyGen, Function<Integer, Object> valGen) throws Exception {
        List<Integer> cnts = new ArrayList<>();
        List<Consumption> raws = new ArrayList<>();
        List<Consumption> comps = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            int cnt = 5000 + i * 1000;
            Consumption raw;
            Consumption compressed;

            boolean reversed = i % 2 == 0;

            Function<Integer, Object> kGen = reversed ? valGen : keyGen;
            Function<Integer, Object> vGen = reversed ? keyGen : valGen;

            compressed = doTest(cnt, kGen, vGen);

            try {
                assertEquals(CompressionTransformer.CompressionType.defaultType(), CompressionTransformer.type);

                CompressionTransformer.type = CompressionTransformer.CompressionType.DISABLED;

                raw = doTest(cnt, kGen, vGen);
            }
            finally {
                CompressionTransformer.type = CompressionTransformer.CompressionType.defaultType();  // Restoring default.
            }

            assertTrue("Network, raw=" + raw.net + ", compressed=" + compressed.net, raw.net > compressed.net);
            assertTrue("Memory, raw=" + raw.mem + ", compressed=" + compressed.mem, raw.mem > compressed.mem);

            cnts.add(cnt);
            raws.add(raw);
            comps.add(compressed);
        }

        StringBuilder sb = new StringBuilder();

        sb.append("Comparision results:");

        for (int i = 0; i < cnts.size(); i++)
            sb.append("\nEntries=")
                .append(cnts.get(i))
                .append(",\tNetwork [raw=")
                .append(raws.get(i).net)
                .append(", compressed=")
                .append(comps.get(i).net)
                .append("],\tMemory [raw=")
                .append(raws.get(i).mem)
                .append(", compressed=")
                .append(comps.get(i).mem)
                .append("]");

        for (int i = 1; i < cnts.size(); i++) {
            long rnd = raws.get(i).net - raws.get(i - 1).net;
            long cnd = comps.get(i).net - comps.get(i - 1).net;
            long rmd = raws.get(i).mem - raws.get(i - 1).mem;
            long cmd = comps.get(i).mem - comps.get(i - 1).mem;

            sb.append("\nEntries=")
                .append(cnts.get(i - 1))
                .append("-")
                .append(cnts.get(i))
                .append(",\tNetwork diff [raw=")
                .append(rnd)
                .append(", compressed=")
                .append(cnd)
                .append(", profit=")
                .append((rnd - cnd) * 100 / rnd)
                .append("%],\tMemory diff [raw=")
                .append(rmd)
                .append(", compressed=")
                .append(cmd)
                .append(", profit=")
                .append((rmd - cmd) * 100 / rmd)
                .append("%]");
        }

        log.info(sb.toString());
    }

    /**
     *
     */
    private Consumption doTest(int cnt, Function<Integer, Object> keyGen, Function<Integer, Object> valGen) throws Exception {
        try {
            Ignite ignite = startGrids(2);

            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

            try (IgniteClient client = G.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + DFLT_PORT))) {
                ClientCache<Object, Object> cCache = client.cache(CACHE_NAME);

                for (int i = 0; i < cnt; i++) {
                    Object key = keyGen.apply(i);
                    Object val = valGen.apply(i);

                    if (thinClient) {
                        cCache.put(key, val);

                        assertEquals(cCache.get(key), val);
                    }
                    else {
                        cache.put(key, val);

                        assertEquals(cache.get(key), val);
                    }
                }
            }

            long net = 0;
            long mem = 0;

            for (Ignite node : G.allGrids()) {
                CommunicationSpi<?> spi = node.configuration().getCommunicationSpi();

                net += spi.getSentBytesCount();
                net += spi.getReceivedBytesCount();

                MetricRegistry reg = mreg(node, CLIENT_CONNECTOR_METRICS);

                net += reg.<LongMetric>findMetric(SENT_BYTES_METRIC_NAME).value();
                net += reg.<LongMetric>findMetric(RECEIVED_BYTES_METRIC_NAME).value();

                DataRegionMetrics metrics = node.dataRegionMetrics(REGION_NAME);

                mem += Math.round(metrics.getTotalUsedSize() * metrics.getPagesFillFactor());
            }

            return new Consumption(net, mem);
        }
        finally {
            stopAllGrids();
        }
    }

    /** Obtains the metric registry with the specified name from Ignite instance. */
    private MetricRegistry mreg(Ignite ignite, String name) {
        return ((IgniteEx)ignite).context().metric().registry(name);
    }

    /***/
    private static class Consumption {
        /** Network. */
        long net;

        /** Memory. */
        long mem;

        /**
         * @param net Network.
         * @param mem Memory.
         */
        public Consumption(long net, long mem) {
            this.net = net;
            this.mem = mem;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "[net=" + net + ", mem=" + mem + ']';
        }
    }
}
