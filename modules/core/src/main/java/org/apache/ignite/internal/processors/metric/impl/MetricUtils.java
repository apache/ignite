/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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

    /**
     * Example - metric registry name - "io.statistics.PRIMARY_KEY_IDX".
     * root = io - JMX tree root.
     * subName = statistics.PRIMARY_KEY_IDX - bean name.
     *
     * @param regName Metric registry name.
     * @return Parsed names parts.
     */
    public static MetricName parse(String regName) {
        int firstDot = regName.indexOf('.');

        if (firstDot == -1)
            return new MetricName(null, regName);

        String grp = regName.substring(0, firstDot);
        String beanName = regName.substring(firstDot + 1);

        return new MetricName(grp, beanName);
    }

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
    public static boolean compareAndSet(LongMetricImpl m, long expect, long update) {
        return LongMetricImpl.updater.compareAndSet(m, expect, update);
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
     * Parsed metric registry name parts.
     *
     * Example - metric registry name - "io.statistics.PRIMARY_KEY_IDX".
     * root = io - JMX tree root.
     * subName = statistics.PRIMARY_KEY_IDX - bean name.
     */
    public static class MetricName {
        /** JMX group name. */
        private String root;

        /** JMX bean name. */
        private String subName;

        /** */
        MetricName(String root, String subName) {
            this.root = root;
            this.subName = subName;
        }

        /**
         * @return JMX group name.
         */
        public String root() {
            return root;
        }

        /**
         * @return JMX bean name.
         */
        public String subName() {
            return subName;
        }
    }
}
