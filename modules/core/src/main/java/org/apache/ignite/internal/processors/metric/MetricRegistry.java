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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Metric registry.
 */
public class MetricRegistry implements ReadOnlyMetricRegistry {
    /** Registry name. */
    private final String name;

    /** Registered metrics. */
    private final Map<String, Metric> metrics;

    //TODO: Reimplement this
    /** HitRate config provider. */
    //private final Function<String, Long> hitRateCfgProvider;

    /** Histogram config provider. */
    //private final Function<String, long[]> histogramCfgProvider;

    /**
     * @param name Registry name.
     * @param metrics Metrics.
     */
    MetricRegistry(String name, Map<String, Metric> metrics) {
        this.name = name;
        this.metrics = Collections.unmodifiableMap(metrics);

        //TODO: Reimplement this
/*
        this.log = log;
        this.hitRateCfgProvider = hitRateCfgProvider;
        this.histogramCfgProvider = histogramCfgProvider;
*/
    }

    /** {@inheritDoc} */
    @Nullable @Override public <M extends Metric> M findMetric(String name) {
        return (M)metrics.get(name);
    }

    /** Resets state of this metric registry. */
    public void reset() {
        for (Metric m : metrics.values())
            m.reset();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Metric> iterator() {
        return metrics.values().iterator();
    }

    /** @return Group name. */
    @Override public String name() {
        return name;
    }

    //TODO: Reimplement all below
    /**
     * Creates and register hit rate metric.
     *
     * It will accumulates approximate hit rate statistics.
     * Calculates number of hits in last rateTimeInterval milliseconds.
     *
     * @param rateTimeInterval Rate time interval.
     * @param size Array size for underlying calculations.
     * @return {@link HitRateMetric}
     * @see HitRateMetric
     */
/*
    public HitRateMetric hitRateMetric(String name, @Nullable String desc, long rateTimeInterval, int size) {
        String fullName = metricName(regName, name);

        HitRateMetric metric = addMetric(name, new HitRateMetric(fullName, desc, rateTimeInterval, size));

        Long cfgRateTimeInterval = hitRateCfgProvider.apply(fullName);

        if (cfgRateTimeInterval != null)
            metric.reset(cfgRateTimeInterval, DFLT_SIZE);

        return metric;
    }
*/

    /**
     * Creates and registre named histogram gauge.
     *
     * @param name Name
     * @param bounds Bounds of measurements.
     * @param desc Description.
     * @return {@link HistogramMetricImpl}
     */
/*
    public HistogramMetric histogram(String name, long[] bounds, @Nullable String desc) {
        String fullName = metricName(grpName, name);

        HistogramMetricImpl metric = addMetric(name, new HistogramMetricImpl(fullName, desc, bounds));

        long[] cfgBounds = histogramCfgProvider.apply(fullName);

        if (cfgBounds != null)
            metric.reset(cfgBounds);

        return metric;
    }
*/

}
