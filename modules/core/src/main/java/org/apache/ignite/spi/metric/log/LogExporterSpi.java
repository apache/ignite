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

package org.apache.ignite.spi.metric.log;

import java.util.function.Predicate;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricExporterPushSpi;
import org.apache.ignite.spi.metric.MetricRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * This SPI implementation exports metrics to Ignite log.
 */
public class LogExporterSpi extends IgniteSpiAdapter implements MetricExporterPushSpi {
    /**
     * Monitoring registry.
     */
    private MetricRegistry mreg;

    /**
     * Metric filter.
     */
    private @Nullable Predicate<Metric> filter;

    /**
     * Timeout.
     */
    private long timeout;

    /** {@inheritDoc} */
    @Override public void export() {
        log.info("Metrics:");

        mreg.getMetrics().forEach(m -> {
            if (filter != null && !filter.test(m))
                return;

            log.info(m.getName() + " = " + m.getAsString());
        });
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /**
     * Sets timeout of this exporter.
     *
     * @param timeout Timeout.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public void setMetricRegistry(MetricRegistry mreg) {
        this.mreg = mreg;
    }

    /** {@inheritDoc} */
    @Override public void setExportFilter(Predicate<Metric> filter) {
        this.filter = filter;
    }
}
