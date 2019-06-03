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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.metrics.MetricNameUtils.MetricName;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricRegistry;
import org.apache.ignite.spi.metric.ObjectMetric;

import static org.apache.ignite.internal.processors.metrics.MetricNameUtils.parse;

/**
 * MBean for exporting values of metric set.
 */
public class MetricSetMBean implements DynamicMBean {
    /**
     * Metric set name.
     */
    private String msetName;

    /**
     * Metric set.
     */
    private Map<String, Metric> mset = new HashMap<>();

    /**
     * @param msetName Metric set name.
     * @param mreg Metric registry.
     * @param first First set entry.
     * @param m
     */
    public MetricSetMBean(String msetName, MetricRegistry mreg, Metric first) {
        this.msetName = msetName;

        mreg.addMetricCreationListener(m -> {
            if (m.getName().startsWith(msetName)) {
                MetricName parsed = parse(m.getName());

                if (!parsed.msetName().equals(msetName))
                    return;

                mset.put(parsed.mname(), m);
            }
        });

        mset.put(parse(first.getName()).mname(), first);
    }

    /** {@inheritDoc} */
    @Override public Object getAttribute(String attribute)
        throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (attribute.equals("MBeanInfo"))
            return getMBeanInfo();

        Metric metric = mset.get(attribute);

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
        Iterator<Metric> iter = mset.values().iterator();

        MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[mset.size()];

        int sz = attributes.length;
        for (int i = 0; i < sz; i++) {
            Metric metric = iter.next();

            attributes[i] = new MBeanAttributeInfo(
                metric.getName().substring(msetName.length() + 1),
                metricClass(metric),
                metric.getName(),
                true,
                false,
                false);
        }

        return new MBeanInfo(
            MetricRegistry.class.getName(),
            msetName,
            attributes,
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
            return "java.lang.Boolean";
        else if (metric instanceof DoubleMetric)
            return "java.lang.Double";
        else if (metric instanceof IntMetric)
            return "java.lang.Int";
        else if (metric instanceof LongMetric)
            return "java.lang.Long";
        else if (metric instanceof ObjectMetric)
            return ((ObjectMetric)metric).type().getName();

        throw new IllegalArgumentException("Unknown metric class. " + metric.getClass());
    }

    /** {@inheritDoc} */
    @Override public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();

        try {
            for (String attribute : attributes) {
                Object val = getAttribute(attribute);

                list.add(val);
            }
        }
        catch (ReflectionException | MBeanException | AttributeNotFoundException e) {
            throw new IgniteException(e);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Attribute attribute)
        throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException("setAttribute not supported.");
    }

    /** {@inheritDoc} */
    @Override public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes not supported.");
    }

    /** {@inheritDoc} */
    @Override public Object invoke(String actionName, Object[] params, String[] signature)
        throws MBeanException, ReflectionException {
        if (actionName.equals("getAttribute")) {
            try {
                return getAttribute((String)params[0]);
            }
            catch (AttributeNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        throw new UnsupportedOperationException("invoke not supported.");
    }
}
