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
     * Example - metric name - "io.statistics.PRIMARY_KEY_IDX.pagesCount".
     * root = io - JMX tree root.
     * subName = statistics.PRIMARY_KEY_IDX - bean name.
     * msetName = io.statistics.PRIMARY_KEY_IDX - prefix to search metrics for a bean.
     * mname = pagesCount - metric name.
     *
     * @param name Metric name.
     * @return Parsed names parts.
     */
    public static MetricName parse(String name) {
        int firstDot = name.indexOf('.');
        int lastDot = name.lastIndexOf('.');

        String grp = name.substring(0, firstDot);
        String beanName = name.substring(firstDot + 1, lastDot);
        String msetName = name.substring(0, lastDot);
        String mname = name.substring(lastDot + 1);

        return new MetricName(grp, beanName, msetName, mname);
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
        for (int i=0; i<names.length; i++)
            assert names[i] != null && !names[i].isEmpty() : i + " element is empty [" + String.join(".", names) + "]";

        return true;
    }

    /**
     * Parsed metric name parts.
     *
     * Example - metric name - "io.statistics.PRIMARY_KEY_IDX.pagesCount".
     * root = io - JMX tree root.
     * subName = statistics.PRIMARY_KEY_IDX - bean name.
     * msetName = io.statistics.PRIMARY_KEY_IDX - prefix to search metrics for a bean.
     * mname = pagesCount - metric name.
     */
    public static class MetricName {
        /** JMX group name. */
        private String root;

        /** JMX bean name. */
        private String subName;

        /** Prefix to search metrics that belongs to metric set. */
        private String msetName;

        /** Metric name. */
        private String mname;

        /** */
        MetricName(String root, String subName, String msetName, String mname) {
            this.root = root;
            this.subName = subName;
            this.msetName = msetName;
            this.mname = mname;
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

        /**
         * @return Prefix to search other metrics for metric set represented by this prefix.
         */
        public String msetName() {
            return msetName;
        }

        /**
         * @return Metric name.
         */
        public String mname() {
            return mname;
        }
    }
}
