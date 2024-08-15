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

package org.apache.ignite.internal.processors.cache.transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_CONNECTOR_METRICS;
import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_NAME;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_NAME;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheObjectCompressionConsumptionTest extends AbstractCacheObjectCompressionTest {
    /** Region name. */
    private static final String REGION_NAME = "region";

    /** Thin client. */
    @Parameterized.Parameter
    public ConsumptionTestMode mode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "mode={0}")
    public static Collection<?> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (ConsumptionTestMode mode : ConsumptionTestMode.values())
            res.add(new Object[] {mode});

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration drCgf = new DataRegionConfiguration()
            .setName(REGION_NAME)
            .setMetricsEnabled(true)
            .setMaxSize(1000L * 1024 * 1024)
            .setInitialSize(1000L * 1024 * 1024);

        if (mode == ConsumptionTestMode.PERSISTENT)
            drCgf.setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(drCgf)
                .setMetricsEnabled(true));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testString() throws Exception {
        testConsumption((i) -> i, this::hugeValue);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testWrappedString() throws Exception {
        testConsumption((i) -> i, (i) -> new StringData(hugeValue(i)));
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testStringArray() throws Exception {
        testConsumption((i) -> i, (i) -> new String[] {hugeValue(i)});
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testWrappedStringArray() throws Exception {
        testConsumption((i) -> i, (i) -> new StringData[] {new StringData(hugeValue(i))});
    }

    /**
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_USE_BINARY_ARRAYS, value = "true")
    @org.junit.Test
    public void testWrappedStringBinaryArray() throws Exception {
        testWrappedStringArray();
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

        for (int i = 1; i <= 4; i++) {
            int cnt = 2000 + i * 1000; // At least 2000 entries to guarantee compression profit.

            Consumption raw;
            Consumption compressed;

            CompressionTransformer.type = CompressionTransformer.CompressionType.DISABLED;

            raw = doTest(cnt, keyGen, valGen); // Compression disabled.

            CompressionTransformer.type = CompressionTransformer.CompressionType.defaultType();

            compressed = doTest(cnt, keyGen, valGen); // Compression enabled.

            assertTrue("Network, raw=" + raw.net + ", compressed=" + compressed.net, raw.net > compressed.net);
            assertTrue("Memory, raw=" + raw.mem + ", compressed=" + compressed.mem, raw.mem > compressed.mem);

            if (mode == ConsumptionTestMode.PERSISTENT)
                assertTrue("Persistence, raw=" + raw.persist + ", compressed=" + compressed.persist,
                    raw.persist > compressed.persist);

            cnts.add(cnt);
            raws.add(raw);
            comps.add(compressed);
        }

        StringBuilder sb = new StringBuilder();

        sb.append("\nComparison results [mode=").append(mode).append("]:");

        for (int i = 0; i < cnts.size(); i++) {
            long rn = raws.get(i).net;
            long cn = comps.get(i).net;
            long rm = raws.get(i).mem;
            long cm = comps.get(i).mem;
            long rp = raws.get(i).persist;
            long cp = comps.get(i).persist;

            sb.append("\nEntries=")
                .append(cnts.get(i))
                .append("\n\t")
                .append("Network     [raw=")
                .append(rn)
                .append(", compressed=")
                .append(cn)
                .append(", profit=")
                .append((rn - cn) * 100 / rn)
                .append("%],\n\t")
                .append("Memory      [raw=")
                .append(rm)
                .append(", compressed=")
                .append(cm)
                .append(", profit=")
                .append((rm - cm) * 100 / rm)
                .append("%],\n\t")
                .append("Persistence [raw=")
                .append(rp)
                .append(", compressed=")
                .append(cp)
                .append(", profit=")
                .append((mode == ConsumptionTestMode.PERSISTENT) ? (rp - cp) * 100 / rp : "NA")
                .append("%]");
        }

        for (int i = 1; i < cnts.size(); i++) {
            long rnd = raws.get(i).net - raws.get(i - 1).net;
            long cnd = comps.get(i).net - comps.get(i - 1).net;
            long rmd = raws.get(i).mem - raws.get(i - 1).mem;
            long cmd = comps.get(i).mem - comps.get(i - 1).mem;
            long rpd = raws.get(i).persist - raws.get(i - 1).persist;
            long cpd = comps.get(i).persist - comps.get(i - 1).persist;

            assertTrue(rnd > 0);
            assertTrue(cnd > 0);
            assertTrue(rmd > 0);
            assertTrue(cmd > 0);

            assertTrue(mode == ConsumptionTestMode.PERSISTENT ? rpd > 0 : rpd == 0);
            assertTrue(mode == ConsumptionTestMode.PERSISTENT ? cpd > 0 : cpd == 0);

            sb.append("\nDiff [entries=")
                .append(cnts.get(i - 1))
                .append("->")
                .append(cnts.get(i))
                .append("]\n\t")
                .append("Network     [raw=")
                .append(rnd)
                .append(", compressed=")
                .append(cnd)
                .append(", profit=")
                .append((rnd - cnd) * 100 / rnd)
                .append("%],\n\t")
                .append("Memory      [raw=")
                .append(rmd)
                .append(", compressed=")
                .append(cmd)
                .append(", profit=")
                .append((rmd - cmd) * 100 / rmd)
                .append("%],\n\t")
                .append("Persistence [raw=")
                .append(rpd)
                .append(", compressed=")
                .append(cpd)
                .append(", profit=")
                .append((mode == ConsumptionTestMode.PERSISTENT) ? (rpd - cpd) * 100 / rpd : "NA")
                .append("%]");
        }

        log.info(sb.toString());
    }

    /**
     *
     */
    private Consumption doTest(int cnt, Function<Integer, Object> keyGen, Function<Integer, Object> valGen) throws Exception {
        try {
            cleanPersistenceDir();

            Ignite ignite = startGrids(2);

            if (mode == ConsumptionTestMode.PERSISTENT)
                ignite.cluster().state(ClusterState.ACTIVE);

            awaitPartitionMapExchange();

            for (Ignite node : G.allGrids())
                node.configuration().getCommunicationSpi().resetMetrics();

            Ignite prim = primaryNode(0, CACHE_NAME);

            if (mode == ConsumptionTestMode.THIN_CLIENT || mode == ConsumptionTestMode.THIN_CLIENT_INTERNAL_API) {
                String host = prim.configuration().getLocalHost();
                int port = prim.configuration().getClientConnectorConfiguration().getPort();

                try (IgniteClient client = G.startClient(new ClientConfiguration().setAddresses(host + ":" + port))) {
                    ClientCache<Object, Object> cache = client.cache(CACHE_NAME);

                    for (int i = 0; i < cnt; i++) {
                        Object key = keyGen.apply(i);
                        Object val = valGen.apply(i);

                        if (mode == ConsumptionTestMode.THIN_CLIENT)
                            cache.put(key, val);
                        else {
                            assert mode == ConsumptionTestMode.THIN_CLIENT_INTERNAL_API;

                            Map<Object, T3<Object, GridCacheVersion, Long>> data = new HashMap<>();

                            GridCacheVersion otherVer = new GridCacheVersion(1, 1, 1, 0);

                            data.put(key, new T3<>(val, otherVer, 0L));

                            ((TcpClientCache)cache).putAllConflict(data);
                        }

                        assertEqualsArraysAware(cache.get(key), val);
                    }
                }
            }
            else {
                IgniteCache<Object, Object> cache = prim.getOrCreateCache(CACHE_NAME);

                for (int i = 0; i < cnt; i++) {
                    Object key = keyGen.apply(i);
                    Object val = valGen.apply(i);

                    cache.put(key, val);

                    assertEqualsArraysAware(cache.get(key), val);
                }
            }

            long net = 0;
            long mem = 0;
            long pers = 0;

            for (Ignite node : G.allGrids()) {
                if (mode == ConsumptionTestMode.PERSISTENT)
                    forceCheckpoint(node);

                CommunicationSpi<?> spi = node.configuration().getCommunicationSpi();

                net += spi.getSentBytesCount();
                net += spi.getReceivedBytesCount();

                long clNet = 0;

                MetricRegistry reg = mreg(node, CLIENT_CONNECTOR_METRICS);

                clNet += reg.<LongMetric>findMetric(SENT_BYTES_METRIC_NAME).value();
                clNet += reg.<LongMetric>findMetric(RECEIVED_BYTES_METRIC_NAME).value();

                if (mode != ConsumptionTestMode.THIN_CLIENT && mode != ConsumptionTestMode.THIN_CLIENT_INTERNAL_API)
                    assertEquals(0, clNet);

                net += clNet;

                DataRegionMetrics metrics = node.dataRegionMetrics(REGION_NAME);

                mem += metrics.getTotalAllocatedSize();

                String nodeFolder = ((IgniteEx)node).context().pdsFolderResolver().resolveFolders().folderName();

                pers += FileUtils.sizeOfDirectory(
                    U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR + "/" + nodeFolder, false));

                if (mode != ConsumptionTestMode.PERSISTENT)
                    assertEquals(0, pers);
            }

            return new Consumption(net, mem, pers);
        }
        finally {
            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** Obtains the metric registry with the specified name from Ignite instance. */
    private MetricRegistry mreg(Ignite ignite, String name) {
        return ((IgniteEx)ignite).context().metric().registry(name);
    }

    /***/
    private String hugeValue(int i) {
        return HUGE_STRING + i;
    }

    /***/
    private static class Consumption {
        /** Network. */
        long net;

        /** Memory. */
        long mem;

        /** Persistence. */
        long persist;

        /***/
        public Consumption(long net, long mem, long persist) {
            this.net = net;
            this.mem = mem;
            this.persist = persist;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "[net=" + net + ", mem=" + mem + ", pers=" + persist + ']';
        }
    }

    /**
     *
     */
    private enum ConsumptionTestMode {
        /** Node. */
        NODE,

        /** Thin client. */
        THIN_CLIENT,

        /** Thin client uses internal API. */
        THIN_CLIENT_INTERNAL_API,

        /** Node + Persistent. */
        PERSISTENT
    }
}
