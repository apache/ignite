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

package org.apache.ignite.internal.processors.metric;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Predicate;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.ReadOnlyMonitoringListRegistry;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Base class for exporters that pushes metrics to the external system.
 */
public abstract class PushMetricsExporterAdapter extends IgniteSpiAdapter implements MetricExporterSpi {
    /** Metric registry. */
    protected ReadOnlyMetricRegistry mreg;

    /** Monitoring list registry. */
    protected ReadOnlyMonitoringListRegistry mlreg;

    /** Metric filter. */
    protected  @Nullable Predicate<MetricRegistry> mregFilter;

    /** Monitoring list filter. */
    protected  @Nullable Predicate<MonitoringList<?, ?>> mlistFilter;

    /** Export period. */
    private long period;

    /** Push spi executor. */
    private ScheduledExecutorService execSvc;

    /** Export task future. */
    private ScheduledFuture<?> fut;

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        execSvc = Executors.newScheduledThreadPool(1);

        fut = execSvc.scheduleWithFixedDelay(() -> {
            try {
                export();
            }
            catch (Exception e) {
                log.error("Metrics export error. " +
                    "This exporter will be stopped [spiClass=" + getClass() + ",name=" + getName() + ']', e);

                throw e;
            }
        }, period, period, MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        fut.cancel(false);

        execSvc.shutdown();
    }

    /**
     * Callback to do the export of metrics info.
     * Method will be called into some Ignite managed thread each {@link #getPeriod()} millisecond.
     */
    public abstract void export();

    /**
     * Sets period in milliseconds after {@link #export()} method should be called.
     *
     * @param period Period in milliseconds.
     */
    public void setPeriod(long period) {
        this.period = period;
    }

    /** @return Period in milliseconds after {@link #export()} method should be called. */
    public long getPeriod() {
        return period;
    }

    /** {@inheritDoc} */
    @Override public void setMetricRegistry(ReadOnlyMetricRegistry mreg) {
        this.mreg = mreg;
    }

    /** {@inheritDoc} */
    @Override public void setMonitoringListRegistry(ReadOnlyMonitoringListRegistry mlreg) {
        this.mlreg = mlreg;

    }

    /** {@inheritDoc} */
    @Override public void setMetricExportFilter(Predicate<MetricRegistry> filter) {
        this.mregFilter = filter;
    }

    /** {@inheritDoc} */
    @Override public void setMonitoringListExportFilter(Predicate<MonitoringList<?, ?>> filter) {
        this.mlistFilter = filter;
    }
}
