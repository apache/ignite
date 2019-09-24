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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;

import static java.util.Arrays.binarySearch;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.INF;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.histogramBucketNames;

/**
 * MBean for exporting values of metric registry.
 */
public class MetricRegistryMBean extends ReadOnlyDynamicMBean {
    /** Metric registry. */
    MetricRegistry mreg;

    /** Cached histogram metrics intervals names. */
    private final Map<String, T2<long[], String[]>> histogramNames = new HashMap<>();

    /**
     * @param mreg Metric registry.
     */
    public MetricRegistryMBean(MetricRegistry mreg) {
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
                String[] names = histogramBucketNames((HistogramMetric)metric, histogramNames);

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
            ReadOnlyMetricRegistry.class.getName(),
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
     * @see MetricUtils#histogramBucketNames(HistogramMetric, Map)
     */
    public static Long searchHistogram(String name, MetricRegistry mreg) {
        Scanner sc = new Scanner(name).useDelimiter("_");

        if (!sc.hasNext())
            return null;

        Metric m = mreg.findMetric(sc.next());

        if (!(m instanceof HistogramMetric))
            return null;

        HistogramMetric h = (HistogramMetric)m;

        long[] bounds = h.bounds();
        long[] values = h.value();

        if (!sc.hasNextLong())
            return null;

        long lowBound = sc.nextLong();

        //If `highBound` not presented this can be last interval `[max]_inf`.
        if (!sc.hasNextLong()) {
            if (sc.hasNext() && INF.equals(sc.next()) && bounds[bounds.length - 1] == lowBound)
                return values[values.length - 1];

            return null;
        }

        long highBound = sc.nextLong();

        int idx = binarySearch(bounds, highBound);

        if (idx < 0)
            return null;

        if ((idx == 0 && lowBound != 0) || (idx != 0 && bounds[idx - 1] != lowBound))
            return null;

        return values[idx];
    }
}
