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

package org.apache.ignite.spi.metric;

import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.metric.jmx.JmxExporterSpi;

/**
 * Exporter of monitoring information to the external recepient.
 * Expected, that each implementation would support some specific protocol.
 *
 * Implementation of this Spi should work by pull paradigm.
 * So after start SPI should respond to some incoming request.
 * HTTP servlet or JMX bean are good examples of expected implementations.
 *
 * @see ReadOnlyMetricRegistry
 * @see Metric
 * @see BooleanMetric
 * @see DoubleMetric
 * @see IntMetric
 * @see LongMetric
 * @see ObjectMetric
 * @see JmxExporterSpi
 */
public interface MetricExporterSpi extends IgniteSpi {
    /**
     * Sets metrics registry that SPI should export.
     * This method called before {@link #spiStart(String)}.
     *
     * Registry should be empty in the time this method called.
     * So all {@link MetricRegistry} that will be created by Ignite internal components can be obtained by
     * listeners passed to {@link MetricRegistry#addMetricCreationListener(Consumer)}.
     *
     * @param registry Metric registry.
     */
    public void setMetricRegistry(ReadOnlyMetricRegistry registry);

    /**
     * Sets export filter.
     * Metrics that not satisfy {@code filter} shouldn't be exported by this filter.
     *
     * @param filter Filter.
     */
    public void setExportFilter(Predicate<Metric> filter);
}
