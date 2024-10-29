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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CUSTOM_METRICS;

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

    /** Name prefix of a custom metric. */
    private static final String CUSTOM_METRICS_PREF = CUSTOM_METRICS + SEPARATOR;

    /** Custom metric name pattern. Permits empty string, spaces, tabs, dot at the start or end, consiquent dots. */
    private static final Pattern CUSTOM_NAME_PATTERN = Pattern.compile("(?!\\.)(?!.*\\.$)(?!.*\\.\\.)(?!.*[\\s]+.*).+");

    /**
     * Chechs and builds metric name.
     *
     * @param names Metric name parts.
     * @return Metric name.
     */
    public static String metricName(String... names) {
        assert names != null && names.length > 0 : "Metric name must consist of at least one element.";

        boolean custom = customMetric(names[0]);

        for (int i = 0; i < names.length; i++) {
            if (F.isEmpty(names[i]) || (custom && !CUSTOM_NAME_PATTERN.matcher(names[i]).matches())) {
                throw new IllegalArgumentException("Illegal metric or registry name: '" + names[i] + "'. Spaces, " +
                    "nulls, empty name or name parts are not allowed.");
            }
        }

        return String.join(SEPARATOR, names);
    }

    /**
     * @return {@code True} if {@code name} is or start with the custom metric prefix.
     */
    public static boolean customMetric(String name) {
        return name != null && (name.startsWith(CUSTOM_METRICS_PREF) || name.equals(CUSTOM_METRICS));
    }

    /** Adds {@link GridMetricManager#CUSTOM_METRICS} to {@code name}. */
    public static String customName(String name) {
        return metricName(CUSTOM_METRICS, name);
    }

    /**
     * Splits full metric name to registry name and metric name.
     *
     * @param name Full metric name.
     * @return Array consist of registry name and metric name.
     */
    public static T2<String, String> fromFullName(String name) {
        int metricNamePos = name.lastIndexOf(SEPARATOR);

        String regName;
        String metricName;

        if (metricNamePos == -1) {
            regName = name;
            metricName = "";
        }
        else {
            regName = name.substring(0, metricNamePos);
            metricName = name.substring(metricNamePos + 1);
        }

        return new T2<>(regName, metricName);
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
     * @param cacheOrGroupName Cache or group name, depending whether group is implicit or not.
     * @return Cache metrics registry name.
     */
    public static String cacheGroupMetricsRegistryName(String cacheOrGroupName) {
        return metricName(CACHE_GROUP_METRICS_PREFIX, cacheOrGroupName);
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

    /**
     * Build SQL-like name from Java code style name.
     * Some examples:
     *
     * cacheName -> CACHE_NAME.
     * affinitiKeyName -> AFFINITY_KEY_NAME.
     *
     * @param name Name to convert.
     * @return SQL compatible name.
     */
    public static String toSqlName(String name) {
        return name
            .replaceAll("([A-Z])", "_$1")
            .replaceAll('\\' + SEPARATOR, "_").toUpperCase();
    }

    /**
     * Extract attributes for system view.
     *
     * @param sysView System view.
     * @return Attributes map.
     */
    public static Map<String, Class<?>> systemViewAttributes(SystemView<?> sysView) {
        Map<String, Class<?>> attrs = new LinkedHashMap<>(sysView.walker().count());

        sysView.walker().visitAll(new SystemViewRowAttributeWalker.AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                attrs.put(name, clazz);
            }
        });

        return attrs;
    }
}
