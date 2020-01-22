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

package org.apache.ignite;

import org.apache.ignite.services.Service;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Defines metric functionality for {@link Ignite} node.
 * {@link Ignite} implements two layer hierarchy for storing metrics.
 * Metrics for specific subsystem or internal entity(like {@link IgniteCache} or {@link Service}) are grouped into {@link ReadOnlyMetricRegistry}.
 * Each {@link ReadOnlyMetricManager} provides access to collections of metrics.
 *
 * @see <a href="https://apacheignite.readme.io/docs/new-metrics">Metrics documentation</a>
 * @see MetricExporterSpi
 * @see ReadOnlyMetricRegistry
 * @see ReadOnlyMetricManager
 * @see Metric
 * @see LongMetric
 * @see IntMetric
 * @see DoubleMetric
 * @see BooleanMetric
 * @see ObjectMetric
 * @see HistogramMetric
 */
public interface IgniteMetric extends Iterable<ReadOnlyMetricRegistry> {
    /**
     * @param name Metric registry name.
     * @return Metric registry if exists, {@code null} otherwise.
     */
    @Nullable ReadOnlyMetricRegistry registry(String name);
}
