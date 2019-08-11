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

package org.apache.ignite.spi.metric.jmx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;

/**
 * MBean for exporting values of metric registry.
 */
public class MetricRegistryMBean implements DynamicMBean {
    /** Metric registry. */
    MetricRegistry mreg;

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
            return null;

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
            attributes.add(new MBeanAttributeInfo(
                metric.name().substring(mreg.name().length() + 1),
                metricClass(metric),
                metric.description() != null ? metric.description() : metric.name(),
                true,
                false,
                false));
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

    /** {@inheritDoc} */
    @Override public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();

        for (String attribute : attributes) {
            Object val = getAttribute(attribute);

            list.add(val);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Attribute attribute) {
        throw new UnsupportedOperationException("setAttribute not supported.");
    }

    /** {@inheritDoc} */
    @Override public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes not supported.");
    }

    /** {@inheritDoc} */
    @Override public Object invoke(String actionName, Object[] params, String[] signature) {
        if (actionName.equals("getAttribute"))
            return getAttribute((String)params[0]);

        throw new UnsupportedOperationException("invoke not supported.");
    }
}
