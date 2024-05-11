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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.Ignition;
import org.apache.ignite.cdc.thin.IgniteToIgniteClientCdcStreamer;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.cdc.AbstractReplicationTest.ClientType.CLIENT_NODE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CdcIgniteToIgniteReplicationTest extends AbstractReplicationTest {
    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActivePassiveCdc(String cache) {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < srcCluster.length; i++)
            futs.add(igniteToIgnite(srcCluster[i].configuration(), destClusterCliCfg[i], destCluster, cache));

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected List<IgniteInternalFuture<?>> startActiveActiveCdc() {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < srcCluster.length; i++)
            futs.add(igniteToIgnite(srcCluster[i].configuration(), destClusterCliCfg[i], destCluster, ACTIVE_ACTIVE_CACHE));

        for (int i = 0; i < destCluster.length; i++)
            futs.add(igniteToIgnite(destCluster[i].configuration(), srcClusterCliCfg[i], srcCluster, ACTIVE_ACTIVE_CACHE));

        return futs;
    }

    /** {@inheritDoc} */
    @Override protected void checkConsumerMetrics(Function<String, Long> longMetric) {
        assertNotNull(longMetric.apply(AbstractIgniteCdcStreamer.LAST_EVT_TIME));
        assertNotNull(longMetric.apply(AbstractIgniteCdcStreamer.EVTS_CNT));
    }

    /**
     * @param srcCfg Ignite source node configuration.
     * @param destCfg Ignite destination cluster configuration.
     * @param dest Ignite destination cluster.
     * @param cache Cache name to stream to kafka.
     * @return Future for Change Data Capture application.
     */
    protected IgniteInternalFuture<?> igniteToIgnite(
        IgniteConfiguration srcCfg,
        IgniteConfiguration destCfg,
        IgniteEx[] dest,
        String cache
    ) {
        return runAsync(() -> {
            CdcConfiguration cdcCfg = new CdcConfiguration();

            AbstractIgniteCdcStreamer streamer;

            if (clientType == ClientType.THIN_CLIENT) {
                streamer = new IgniteToIgniteClientCdcStreamer()
                    .setAliveCheckTimeout(5_000)
                    .setDestinationClientConfiguration(new ClientConfiguration()
                        .setAddresses(hostAddresses(dest)));
            }
            else {
                streamer = new IgniteToIgniteCdcStreamer().setDestinationIgniteConfiguration(
                    new IgniteConfiguration(destCfg).setFailureHandler(new StopNodeOrHaltFailureHandler(true, 30_000))
                );
            }

            streamer.setMaxBatchSize(KEYS_CNT);
            streamer.setCaches(Collections.singleton(cache));

            cdcCfg.setConsumer(streamer);
            cdcCfg.setMetricExporterSpi(new JmxMetricExporterSpi());

            CdcMain cdc = new CdcMain(srcCfg, null, cdcCfg);

            cdcs.add(cdc);

            cdc.run();
        });
    }

    /** */
    @Test
    public void testCdcStopOnClientNodeCrash() throws Exception {
        List<IgniteInternalFuture<?>> cdcFuts = startActivePassiveCdc(ACTIVE_PASSIVE_CACHE);

        if (clientType == CLIENT_NODE) {
            assertTrue("Waiting for clients to connect", waitForCondition(
                () -> destCluster[0].cluster().forClients().nodes().size() == srcCluster.length,
                30_000
            ));
        }
        else {
            assertTrue("Waiting for clients to connect", waitForCondition(
                () -> {
                    int cliCnt = 0;

                    for (IgniteEx dest : destCluster) {
                        SystemView<?> view = dest.context().systemView().view(ClientListenerProcessor.CLI_CONN_VIEW);

                        assertNotNull(view);

                        cliCnt += view.size();
                    }

                    return cliCnt >= srcCluster.length;
                },
                30_000
            ));
        }

        // Stopping destination cluster. IgniteToIgniteCdcStreamer connected to it.
        for (IgniteEx destIgnite : destCluster)
            Ignition.stop(destIgnite.name(), true);

        for (IgniteInternalFuture<?> cdcFut : cdcFuts) {
            assertTrue(
                "Waiting for clients fail and crash ignite-cdc",
                waitForCondition(cdcFut::isDone, 30_000)
            );
        }
    }
}
