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

package org.apache.ignite.internal.processors.metric.impl;

import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.metric.HistogramMetric;

import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;

/**
 * Utility class to build or parse metric name in dot notation.
 *
 * @see GridMetricManager
 * @see MetricRegistry
 */
public class MetricUtils {
    /** Metric name part separator. */
    public static final String SEPARATOR = ".";

    /** Histogram metric last interval high bound. */
    public static final String INF = "_inf";

    /** Histogram name divider. */
    public static final char HISTOGRAM_NAME_DIVIDER = '_';

    /**
     * Builds metric name. Each parameter will separated by '.' char.
     *
     * @param names Metric name parts.
     * @return Metric name.
     */
    public static String metricName(String... names) {
        assert names != null;
        assert ensureAllNamesNotEmpty(names);

        if (names.length == 1)
            return names[0];

        return String.join(SEPARATOR, names);
    }

    /**
     * Splits full metric name to registry name and metric name.
     *
     * @param name Full metric name.
     * @return Array consist of registry name and metric name.
     */
    public static T2<String, String> fromFullName(String name) {
        return new T2<>(
            name.substring(0, name.lastIndexOf(SEPARATOR)),
            name.substring(name.lastIndexOf(SEPARATOR) + 1)
        );
    }

    /**
     * @param cacheName Cache name.
     * @param isNear Is near flag.
     * @return Cache metrics registry name.
     */
    public static String cacheMetricsRegistryName(String cacheName, boolean isNear) {
        if (isNear)
            return metricName(CACHE_METRICS, cacheName, "near");

        return metricName(CACHE_METRICS, cacheName);
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     *
     * @param m Metric.
     * @param expect The expected value.
     * @param update The new value.
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    public static boolean compareAndSet(AtomicLongMetric m, long expect, long update) {
        return AtomicLongMetric.updater.compareAndSet(m, expect, update);
    }

    /**
     * Update metrics value only if current value if less then {@code update}.
     *
     * @param m Metric to update.
     * @param update New value.
     */
    public static void setIfLess(AtomicLongMetric m, long update) {
        long v = m.value();

        while (v > update && !AtomicLongMetric.updater.compareAndSet(m, v, update))
            v = m.value();
    }

    /**
     * Update metrics value only if current value if greater then {@code update}.
     *
     * @param m Metric to update.
     * @param update New value.
     */
    public static void setIfGreater(AtomicLongMetric m, long update) {
        long v = m.value();

        while (v < update && !AtomicLongMetric.updater.compareAndSet(m, v, update))
            v = m.value();
    }

    /**
     * Asserts all arguments are not empty.
     *
     * @param names Names.
     * @return True.
     */
    private static boolean ensureAllNamesNotEmpty(String... names) {
        for (int i = 0; i < names.length; i++)
            assert names[i] != null && !names[i].isEmpty() : i + " element is empty [" + String.join(".", names) + "]";

        return true;
    }

    /**
     * Generates histogram bucket names.
     *
     * Example of metric names if bounds are 10,100:
     *  histogram_0_10 (less than 10)
     *  histogram_10_100 (between 10 and 100)
     *  histogram_100_inf (more than 100)
     *
     * @param metric Histogram metric
     * @return Histogram intervals names.
     */
    public static String[] histogramBucketNames(HistogramMetric metric) {
        String name = metric.name();
        long[] bounds = metric.bounds();

        String[] names = new String[bounds.length + 1];

        long min = 0;

        for (int i = 0; i < bounds.length; i++) {
            names[i] = name + HISTOGRAM_NAME_DIVIDER + min + HISTOGRAM_NAME_DIVIDER + bounds[i];

            min = bounds[i];
        }

        names[bounds.length] = name + HISTOGRAM_NAME_DIVIDER + min + INF;

        return names;
    }
}
