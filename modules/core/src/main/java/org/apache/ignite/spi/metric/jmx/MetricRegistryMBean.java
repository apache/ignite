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

package org.apache.ignite.spi.metric.jmx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;

import static java.util.Arrays.binarySearch;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.HISTOGRAM_NAME_DIVIDER;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.INF;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.histogramBucketNames;

/**
 * MBean for exporting values of metric registry.
 */
public class MetricRegistryMBean extends ReadOnlyDynamicMBean {
    /** Metric registry. */
    ReadOnlyMetricRegistry mreg;

    /**
     * @param mreg Metric registry.
     */
    public MetricRegistryMBean(ReadOnlyMetricRegistry mreg) {
        this.mreg = mreg;
    }

    /** {@inheritDoc} */
    @Override public Object getAttribute(String attribute) {
        if (attribute.equals("MBeanInfo"))
            return getMBeanInfo();

        Metric metric = mreg.findMetric(attribute);

        if (metric == null)
            return searchHistogram(attribute, mreg);

        if (metric instanceof BooleanMetric)
            return ((BooleanMetric)metric).value();
        else if (metric instanceof DoubleMetric)
            return ((DoubleMetric)metric).value();
        else if (metric instanceof IntMetric)
            return ((IntMetric)metric).value();
        else if (metric instanceof LongMetric)
            return ((LongMetric)metric).value();
        else if (metric instanceof ObjectMetric)
            return ((ObjectMetric)metric).value();

        throw new IllegalArgumentException("Unknown metric class. " + metric.getClass());
    }

    /** {@inheritDoc} */
    @Override public MBeanInfo getMBeanInfo() {
        Iterator<Metric> iter = mreg.iterator();

        List<MBeanAttributeInfo> attributes = new ArrayList<>();

        iter.forEachRemaining(metric -> {
            if (metric instanceof HistogramMetric) {
                String[] names = histogramBucketNames((HistogramMetric)metric);

                assert names.length == ((HistogramMetric)metric).value().length;

                for (String name : names) {
                    String n = name.substring(mreg.name().length() + 1);

                    attributes.add(new MBeanAttributeInfo(
                        n,
                        Long.class.getName(),
                        metric.description() != null ? metric.description() : n,
                        true,
                        false,
                        false));
                }
            }
            else {
                attributes.add(new MBeanAttributeInfo(
                    metric.name().substring(mreg.name().length() + 1),
                    metricClass(metric),
                    metric.description() != null ? metric.description() : metric.name(),
                    true,
                    false,
                    false));
            }

        });

        return new MBeanInfo(
            ReadOnlyMetricManager.class.getName(),
            mreg.name(),
            attributes.toArray(new MBeanAttributeInfo[attributes.size()]),
            null,
            null,
            null);
    }

    /**
     * @param metric Metric.
     * @return Class of metric value.
     */
    private String metricClass(Metric metric) {
        if (metric instanceof BooleanMetric)
            return Boolean.class.getName();
        else if (metric instanceof DoubleMetric)
            return Double.class.getName();
        else if (metric instanceof IntMetric)
            return Integer.class.getName();
        else if (metric instanceof LongMetric)
            return Long.class.getName();
        else if (metric instanceof ObjectMetric)
            return ((ObjectMetric)metric).type().getName();

        throw new IllegalArgumentException("Unknown metric class. " + metric.getClass());
    }

    /**
     * Parse attribute name for a histogram and search it's value.
     *
     * @param name Attribute name.
     * @param mreg Metric registry to search histogram in.
     * @return Specific bucket value or {@code null} if not found.
     * @see MetricUtils#histogramBucketNames(HistogramMetric)
     */
    public static Long searchHistogram(String name, ReadOnlyMetricRegistry mreg) {
        int highBoundIdx;

        boolean isInf = name.endsWith(INF);

        if (isInf)
            highBoundIdx = name.length() - 4;
        else {
            highBoundIdx = name.lastIndexOf(HISTOGRAM_NAME_DIVIDER);

            if (highBoundIdx == -1)
                return null;
        }

        int lowBoundIdx = name.lastIndexOf(HISTOGRAM_NAME_DIVIDER, highBoundIdx - 1);

        if (lowBoundIdx == -1)
            return null;

        Metric m = mreg.findMetric(name.substring(0, lowBoundIdx));

        if (!(m instanceof HistogramMetric))
            return null;

        HistogramMetric h = (HistogramMetric)m;

        long[] bounds = h.bounds();
        long[] values = h.value();

        long lowBound;

        try {
            lowBound = Long.parseLong(name.substring(lowBoundIdx + 1, highBoundIdx));
        }
        catch (NumberFormatException e) {
            return null;
        }

        if (isInf) {
            if (bounds[bounds.length - 1] == lowBound)
                return values[values.length - 1];

            return null;
        }

        long highBound;

        try {
            highBound = Long.parseLong(name.substring(highBoundIdx + 1));
        }
        catch (NumberFormatException e) {
            return null;
        }

        int idx = binarySearch(bounds, highBound);

        if (idx < 0)
            return null;

        if ((idx == 0 && lowBound != 0) || (idx != 0 && bounds[idx - 1] != lowBound))
            return null;

        return values[idx];
    }
}
