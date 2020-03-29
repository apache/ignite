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

package org.apache.ignite.internal.metric;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.metric.PushMetricsExporterAdapter;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks that metrics will be export without exceptions during node start.
 */
public class ReadMetricsOnNodeStartupTest extends GridCommonAbstractTest {
    /** */
    public static final int EXPORT_TIMEOUT = 10;

    /** */
    private final CountDownLatch exportLatch = new CountDownLatch(1);

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(listeningLog);

        PushMetricsExporterAdapter adapter = new PushMetricsExporterAdapter() {
            @Override public void export() {
                try {
                    mreg.forEach(metrics -> {
                        // Read metric value.
                        metrics.forEach(Metric::getAsString);
                    });
                } catch (Throwable e) {
                    log.error("Exception on metric export", e);

                    throw e;
                }

                exportLatch.countDown();
            }
        };

        adapter.setPeriod(EXPORT_TIMEOUT);

        cfg.setMetricExporterSpi(adapter);

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testReadMetricsOnNodeStartup() throws Exception {
        LogListener lsnr = LogListener.matches(s -> s.contains("Exception")).atLeast(1).build();

        listeningLog.registerListener(lsnr);

        startGrid(0);

        exportLatch.await();

        stopGrid(0);

        assertFalse("There was an exception during metric read. See log for details.", lsnr.check());
    }
}
