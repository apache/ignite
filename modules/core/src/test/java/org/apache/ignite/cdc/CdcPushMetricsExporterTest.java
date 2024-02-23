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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.metric.PushMetricsExporterAdapter;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.junit.Test;

/** */
public class CdcPushMetricsExporterTest extends AbstractCdcTest {
    /** */
    private final AtomicBoolean metricsExported = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setCdcEnabled(true)));

        return cfg;
    }

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

    @Test
    public void testPushMetricsExporter() throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-0");

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

    /** */
    public static void addData(IgniteCache<Integer, User> cache, int from, int to) {
        for (int i = from; i < to; i++)
            cache.put(i, createUser(i));
    }
}
