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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.metric.PushMetricsExporterAdapter;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.CallbackExecutorLogListener;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_PORT_RANGE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class CdcPushMetricsExporterTest extends AbstractCdcTest {
    /** */
    private final AtomicBoolean metricsExported = new AtomicBoolean(false);

    /** */
    private ListeningTestLogger lsnrLog;

    /** */
    private IgniteConfiguration getSrcClusterCfg(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setCdcEnabled(true)))
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

        return cfg;
    }

    /** */
    private IgniteConfiguration getDestClusterCfg(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = getConfiguration(igniteInstanceName);

        cfg.setGridLogger(lsnrLog)
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true))
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setLocalPort(TcpDiscoverySpi.DFLT_PORT + DFLT_PORT_RANGE)
                .setLocalAddress("127.0.0.1")
                .setIpFinder(new TcpDiscoveryVmIpFinder()
                    .setAddresses(Collections.singleton("127.0.0.1:" + (TcpDiscoverySpi.DFLT_PORT + DFLT_PORT_RANGE) +
                        ".." + (TcpDiscoverySpi.DFLT_PORT + DFLT_PORT_RANGE + 3)))
                    .setShared(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected MetricExporterSpi[] metricExporters() {
        PushMetricsExporterAdapter pushMetricsExporter = new PushMetricsExporterAdapter() {
            @Override public void export() {
                metricsExported.set(true);
            }
        };

        pushMetricsExporter.setPeriod(100);

        return new MetricExporterSpi[] {
            pushMetricsExporter,
            new JmxMetricExporterSpi(),
        };
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        lsnrLog = new ListeningTestLogger(log);
    }

    /** Test checks that metrics are exported via exporter based on the push metrics exporter adapter. */
    @Test
    public void testPushMetricsExporter() throws Exception {
        IgniteConfiguration cfg = getSrcClusterCfg("ignite-0");

        Ignite ign = startGrid(cfg);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addAndWaitForConsumption(
            new UserCdcConsumer(),
            cfg,
            cache,
            null,
            CdcPushMetricsExporterTest::addData,
            0,
            KEYS_CNT,
            true
        );

        assertTrue(metricsExported.get());
    }

    /** Test checks that consumer can start ignite client node and connect to destination cluster. */
    @Test
    public void testIgniteToIgniteConsumer() throws Exception {
        IgniteConfiguration srcClusterCfg = getSrcClusterCfg("src-cluster");
        startGrid(srcClusterCfg);

        final CountDownLatch destClusterStarted = new CountDownLatch(1);

        lsnrLog.registerListener(new CallbackExecutorLogListener(".*Ignite node started OK \\(id=.*, instance name=dest-cluster\\)",
            destClusterStarted::countDown));

        IgniteConfiguration destClusterCfg = getDestClusterCfg("dest-cluster");

        IgniteProcessProxy targetIgn = startRemoteDestCluster(destClusterCfg);

        destClusterStarted.await(30, TimeUnit.SECONDS);

        final CountDownLatch cdcClientNodeJoined = new CountDownLatch(1);

        lsnrLog.registerListener(new CallbackExecutorLogListener(".*TestIgniteToIgniteConsumer started.",
            cdcClientNodeJoined::countDown));

        CdcConfiguration cdcCfg = new CdcConfiguration();

        IgniteConfiguration destClusterCliCfg = getDestClusterCfg("cdc-client")
            .setClientMode(true);

        cdcCfg.setConsumer(new TestIgniteToIgniteConsumer(destClusterCliCfg));

        CdcMain cdc = new CdcMain(srcClusterCfg, null, cdcCfg);

        runAsync(cdc);

        assertTrue(cdcClientNodeJoined.await(30, TimeUnit.SECONDS));

        cdc.stop();

        targetIgn.kill();
    }

    /**
     * @param destClusterCfg destination cluster config.
     * @return Ignite proxy for destination cluster started in another JVM.
     */
    private IgniteProcessProxy startRemoteDestCluster(IgniteConfiguration destClusterCfg) throws Exception {
        return new IgniteProcessProxy(
            destClusterCfg,
            destClusterCfg.getGridLogger(),
            null,
            false,
            emptyList()
        );
    }

    /** */
    public static void addData(IgniteCache<Integer, User> cache, int from, int to) {
        for (int i = from; i < to; i++)
            cache.put(i, createUser(i));
    }

    /** Test CDC consumer invoking the ignite client node. */
    public static class TestIgniteToIgniteConsumer extends UserCdcConsumer {
        /** */
        private final IgniteConfiguration destClusterCliCfg;

        /** */
        public TestIgniteToIgniteConsumer(IgniteConfiguration destClusterCliCfg) {
            this.destClusterCliCfg = destClusterCliCfg;
        }

        /** {@inheritDoc} */
        @Override public void start(MetricRegistry mreg) {
            Ignite ignite = Ignition.start(destClusterCliCfg);

            super.start(mreg);

            ignite.log().info("TestIgniteToIgniteConsumer started.");
        }
    }
}
