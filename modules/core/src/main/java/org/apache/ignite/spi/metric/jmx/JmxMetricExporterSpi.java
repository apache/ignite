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
import java.util.List;
import java.util.function.Predicate;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.ReadOnlyMonitoringListRegistry;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.parse;
import static org.apache.ignite.internal.util.IgniteUtils.makeMBeanName;
import static org.apache.ignite.spi.metric.jmx.MonitoringListMBean.LIST;

/**
 * This SPI implementation exports metrics as JMX beans.
 */
public class JmxMetricExporterSpi extends IgniteSpiAdapter implements MetricExporterSpi {
    /** Metric registry. */
    private ReadOnlyMetricRegistry mreg;

    /** Monitoring list registry. */
    private ReadOnlyMonitoringListRegistry mlreg;

    /** Metric filter. */
    private @Nullable Predicate<MetricRegistry> metricFilter;

    /** Monitoring list filter. */
    private @Nullable Predicate<MonitoringList<?, ?>> monListFilter;

    /** Registered beans. */
    private final List<ObjectName> mBeans = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        mreg.forEach(this::register);
        mlreg.forEach(this::register);

        mreg.addMetricRegistryCreationListener(this::register);
        mreg.addMetricRegistryRemoveListener(this::unregister);

        mlreg.addListCreationListener(this::register);
        mlreg.addListRemoveListener(this::unregister);
    }

    /**
     * Registers JMX bean for specific monitoring list.
     *
     * @param mlist Monitoring list.
     */
    private void register(MonitoringList<?, ?> mlist) {
        if (monListFilter != null && !monListFilter.test(mlist)) {
            if (log.isDebugEnabled())
                U.debug(log, "Monitoring list filtered and will not be registered.[name=" + mlist.name() + ']');

            return;
        }
        else if (log.isDebugEnabled())
            log.debug("Found new monitoring list [name=" + mlist.name() + ']');

        try {
            MonitoringListMBean mlBean = new MonitoringListMBean<>(mlist);

            ObjectName mbean = U.registerMBean(
                ignite().configuration().getMBeanServer(),
                igniteInstanceName,
                LIST,
                mlist.name(),
                mlBean,
                MonitoringListMBean.class);

            mBeans.add(mbean);

            if (log.isDebugEnabled())
                log.debug("MetricGroup JMX bean created. " + mbean);
        }
        catch (JMException e) {
            log.error("MBean for monitoring list '" + mlist.name() + "' can't be created.", e);
        }
    }

    /**
     * Unregister JMX bean for specific monitoring list.
     *
     * @param mlist Monitoring list.
     */
    private void unregister(MonitoringList<?,?> mlist) {
        unregister(LIST, mlist.name());
    }

    /**
     * Registers JMX bean for specific metric registry.
     *
     * @param mreg Metric registry.
     */
    private void register(MetricRegistry mreg) {
        if (metricFilter != null && !metricFilter.test(mreg)) {
            if (log.isDebugEnabled())
                U.debug(log, "Metric registry filtered and will not be registered.[registry=" + mreg.name() + ']');

            return;
        }
        else if (log.isDebugEnabled())
            log.debug("Found new metric registry [name=" + mreg.name() + ']');

        MetricUtils.MetricName n = parse(mreg.name());

        try {
            MetricRegistryMBean mregBean = new MetricRegistryMBean(mreg);

            ObjectName mbean = U.registerMBean(
                ignite().configuration().getMBeanServer(),
                igniteInstanceName,
                n.root(),
                n.subName(),
                mregBean,
                MetricRegistryMBean.class);

            mBeans.add(mbean);

            if (log.isDebugEnabled())
                log.debug("MetricGroup JMX bean created. " + mbean);
        }
        catch (JMException e) {
            log.error("MBean for metric registry '" + mreg.name() + "' can't be created.", e);
        }
    }

    /**
     * Unregister JMX bean for specific metric registry.
     *
     * @param mreg Metric registry.
     */
    private void unregister(MetricRegistry mreg) {
        MetricUtils.MetricName n = parse(mreg.name());

        unregister(n.root(), n.subName());
    }

    /**
     * @param grp MBean group.
     * @param name MBean name.
     */
    private void unregister(String grp, String name) {
        try {
            ObjectName mbeanName = makeMBeanName(igniteInstanceName, grp, name);

            boolean rmv = mBeans.remove(mbeanName);

            assert rmv;

            unregBean(ignite, mbeanName);
        }
        catch (MalformedObjectNameException e) {
            log.error("MBean for metric registry '" + grp + ',' + name + "' can't be unregistered.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        Ignite ignite = ignite();

        if (ignite == null)
            return;

        for (ObjectName bean : mBeans)
            unregBean(ignite, bean);
    }

    private void unregBean(Ignite ignite, ObjectName bean) {
        MBeanServer jmx = ignite.configuration().getMBeanServer();

        try {
            jmx.unregisterMBean(bean);

            if (log.isDebugEnabled())
                log.debug("Unregistered SPI MBean: " + bean);
        }
        catch (JMException e) {
            log.error("Failed to unregister SPI MBean: " + bean, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setMetricRegistry(ReadOnlyMetricRegistry reg) {
        this.mreg = reg;
    }

    /** {@inheritDoc} */
    @Override public void setMonitoringListRegistry(ReadOnlyMonitoringListRegistry mlreg) {
        this.mlreg = mlreg;
    }

    /** {@inheritDoc} */
    @Override public void setMetricExportFilter(Predicate<MetricRegistry> filter) {
        this.metricFilter = filter;
    }

    /** {@inheritDoc} */
    @Override public void setMonitoringListExportFilter(Predicate<MonitoringList<?, ?>> filter) {
        this.monListFilter = filter;
    }
}
