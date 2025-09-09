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

import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Read only metric registry.
 * <p>
 *
 * <h2>Java example</h2>
 * See the example below of how the internal metrics can be obtained through your application
 * using {@link ReadOnlyMetricRegistry}.
 *
 * <pre>
 * JmxMetricExporterSpi jmxSpi = new JmxMetricExporterSpi();
 *
 * Ignite ignite = Ignition.start(new IgniteConfiguration()
 *     .setDataStorageConfiguration(new DataStorageConfiguration()
 *         .setDefaultDataRegionConfiguration(
 *             new DataRegionConfiguration()
 *                 .setMaxSize(12_000_000)))
 *     .setIgniteInstanceName("jmxExampleInstanceName")
 *     .setMetricExporterSpi(jmxSpi));
 *
 *
 * ReadOnlyMetricRegistry ioReg = jmxSpi.getSpiContext().getOrCreateMetricRegistry("io.dataregion.default");
 *
 * Set<String> listOfMetrics = StreamSupport.stream(ioReg.spliterator(), false)
 *     .map(Metric::name)
 *     .collect(toSet());
 *
 * System.out.println("The list of available data region metrics: " + listOfMetrics);
 * System.out.println("The 'default' data region MaxSize: " + ioReg.findMetric("MaxSize"));
 * </pre>
 *
 * @see JmxMetricExporterSpi
 * @see MetricExporterSpi
 *
 */
public interface ReadOnlyMetricRegistry extends Iterable<Metric> {
    /** @return Registry name. */
    public String name();

    /**
     * @param name Name of the metric.
     * @param <M> Type of the metric.
     * @return Metric with specified name if exists. Null otherwise.
     */
    @Nullable public <M extends Metric> M findMetric(String name);
}
