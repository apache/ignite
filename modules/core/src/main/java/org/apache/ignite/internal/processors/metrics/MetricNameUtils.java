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

package org.apache.ignite.internal.processors.metrics;

import org.apache.ignite.spi.metric.MetricRegistry;

/**
 * Utility class to build or parse metric name in dot notation.
 *
 * @see GridMetricManager
 * @see MetricRegistry
 */
public class MetricNameUtils {
    /**
     * Example - metric name - "io.statistics.PRIMARY_KEY_IDX.pagesCount".
     * root = io - JMX tree root.
     * subName = statistics.PRIMARY_KEY_IDX - bean name.
     * msetName = io.statistics.PRIMARY_KEY_IDX - prefix to search metrics for a bean.
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

        return new MetricName(grp, beanName, msetName);
    }

    /**
     * Builds metric name. Each parameter will separated by '.' char.
     *
     * @param names Metric name parts.
     * @return Metric name.
     */
    public static String metricName(String...names) {
        StringBuilder path = new StringBuilder();

        for (int i = 0; i < names.length; i++) {
            if (names[i] == null)
                continue;

            if (i > 0 && path.length() > 0)
                path.append('.');

            path.append(names[i]);
        }

        return path.toString();
    }

    /**
     * Parsed metric name parts.
     *
     * Example - metric name - "io.statistics.PRIMARY_KEY_IDX.pagesCount".
     * root = io - JMX tree root.
     * subName = statistics.PRIMARY_KEY_IDX - bean name.
     * msetName = io.statistics.PRIMARY_KEY_IDX - prefix to search metrics for a bean.
     */
    public static class MetricName {
        /**
         * JMX group name.
         */
        private String root;

        /**
         * JMX bean name.
         */
        private String subName;

        /**
         * Prefix to search metrics that belongs to metric set.
         */
        private String msetName;

        /** */
        MetricName(String root, String subName, String msetName) {
            this.root = root;
            this.subName = subName;
            this.msetName = msetName;
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
    }
}
