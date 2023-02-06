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
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.makeMBeanName;

/**
 * <h2>Overview</h2>
 *
 * Ignite provides this default built-in implementation of {@link MetricExporterSpi} it exports metrics as JMX beans.
 * This implementation works by `pull` architecture which means that after the Ignite node start it should respond to
 * incoming user request.
 *
 * <h2>Java Example</h2>
 * See the example below of how the internal metrics can be obtained through your application by
 * constructing MBean names.
 *
 * <p>
 * <pre>
 * Ignite ignite = Ignition.start(new IgniteConfiguration()
 *      .setDataStorageConfiguration(new DataStorageConfiguration()
 *          .setDefaultDataRegionConfiguration(
 *              new DataRegionConfiguration()
 *                  .setMaxSize(12_000_000)))
 *      .setIgniteInstanceName("jmxExampleInstanceName"));
 *
 *  String igniteInstanceName = ignite.name();
 *  String metricGroup = "io";
 *
 *  // NOTE: The special characters of metric name must be escaped.
 *  String metricName = "\"dataregion.default\"";
 *
 *  SB sb = new SB("org.apache:");
 *
 *  if (IgniteSystemProperties.getBoolean("IGNITE_MBEAN_APPEND_CLASS_LOADER_ID", true))
 *      sb.a("clsLdr=").a(Integer.toHexString(Ignite.class.getClassLoader().hashCode())).a(',');
 *
 *  if (IgniteSystemProperties.getBoolean("IGNITE_MBEAN_APPEND_JVM_ID"))
 *      sb.a("jvmId=").a(ManagementFactory.getRuntimeMXBean().getName()).a(',');
 *
 *  sb.a("igniteInstanceName=").a(igniteInstanceName).a(',')
 *      .a("group=").a(metricGroup).a(',')
 *      .a("name=").a(metricName);
 *
 *  DynamicMBean dataRegionMBean = MBeanServerInvocationHandler.newProxyInstance(
 *      ignite.configuration().getMBeanServer(),
 *      new ObjectName(sb.toString()),
 *      DynamicMBean.class,
 *      false);
 *
 *  Set<String> listOfMetrics = Arrays.stream(dataRegionMBean.getMBeanInfo().getAttributes())
 *      .map(MBeanFeatureInfo::getName)
 *      .collect(toSet());
 *
 *  System.out.println("The list of available data region metrics: " + listOfMetrics);
 *  System.out.println("The 'default' data region MaxSize: " + dataRegionMBean.getAttribute("MaxSize"));
 * </pre>
 *
 * @see ReadOnlyMetricManager
 * @see ReadOnlyMetricRegistry
 * @see IgniteSystemProperties#IGNITE_MBEANS_DISABLED
 */
public class JmxMetricExporterSpi extends IgniteSpiAdapter implements MetricExporterSpi {
    /** Metric registry. */
    private ReadOnlyMetricManager mreg;

    /** Metric filter. */
    private @Nullable Predicate<ReadOnlyMetricRegistry> filter;

    /** Registered beans. */
    private final List<ObjectName> mBeans = Collections.synchronizedList(new ArrayList<>());

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        mreg.forEach(this::register);

        mreg.addMetricRegistryCreationListener(this::register);
        mreg.addMetricRegistryRemoveListener(this::unregister);
    }

    /**
     * Registers JMX bean for specific metric registry.
     *
     * @param mreg Metric registry.
     */
    private void register(ReadOnlyMetricRegistry mreg) {
        if (filter != null && !filter.test(mreg)) {
            if (log.isDebugEnabled())
                log.debug("Metric registry filtered and will not be registered.[registry=" + mreg.name() + ']');

            return;
        }
        else if (log.isDebugEnabled())
            log.debug("Found new metric registry [name=" + mreg.name() + ']');

        MetricName n = parse(mreg.name());

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
    private void unregister(ReadOnlyMetricRegistry mreg) {
        if (filter != null && !filter.test(mreg))
            return;

        MetricName n = parse(mreg.name());

        try {
            ObjectName mbeanName = makeMBeanName(igniteInstanceName, n.root(), n.subName());

            boolean rmv = mBeans.remove(mbeanName);

            assert rmv;

            unregBean(ignite, mbeanName);
        }
        catch (MalformedObjectNameException e) {
            log.error("MBean for metric registry '" + n.root() + ',' + n.subName() + "' can't be unregistered.", e);
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

    /**
     * @param ignite Ignite instance.
     * @param bean Bean name to unregister.
     */
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
    @Override public void setMetricRegistry(ReadOnlyMetricManager reg) {
        mreg = reg;
    }

    /** {@inheritDoc} */
    @Override public void setExportFilter(Predicate<ReadOnlyMetricRegistry> filter) {
        this.filter = filter;
    }

    /**
     * Example - metric registry name - "io.statistics.PRIMARY_KEY_IDX".
     * root = io - JMX tree root.
     * subName = statistics.PRIMARY_KEY_IDX - bean name.
     *
     * @param regName Metric registry name.
     * @return Parsed names parts.
     */
    private MetricName parse(String regName) {
        int firstDot = regName.indexOf('.');

        if (firstDot == -1)
            return new MetricName(null, regName);

        String grp = regName.substring(0, firstDot);
        String beanName = regName.substring(firstDot + 1);

        return new MetricName(grp, beanName);
    }

    /**
     * Parsed metric registry name parts.
     *
     * Example - metric registry name - "io.statistics.PRIMARY_KEY_IDX".
     * root = io - JMX tree root.
     * subName = statistics.PRIMARY_KEY_IDX - bean name.
     */
    private class MetricName {
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
