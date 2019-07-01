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
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.metric.MetricGroup;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils.MetricName;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.parse;

/**
 * This SPI implementation exports metrics as JMX beans.
 */
public class JmxExporterSpi extends IgniteSpiAdapter implements MetricExporterSpi {
    /** Monitoring registry. */
    private ReadOnlyMetricRegistry mreg;

    /** Metric filter. */
    private @Nullable Predicate<MetricGroup> filter;

    /** Registered beans. */
    private final List<ObjectName> mBeans = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        mreg.addMetricGroupCreationListener(m -> {
            if (filter != null && !filter.test(m))
                return;

            if (log.isDebugEnabled())
                log.debug("Found new metric set [name=" + m.name() + ']');

            MetricName n = parse(m.name());

            try {
                MetricGroupMBean msetBean = new MetricGroupMBean(m);

                ObjectName mbean = U.registerMBean(
                    ignite().configuration().getMBeanServer(),
                    igniteInstanceName,
                    n.root(),
                    n.subName(),
                    msetBean,
                    MetricGroupMBean.class);

                mBeans.add(mbean);

                if (log.isDebugEnabled())
                    log.debug("MetricSet JMX bean created. " + mbean);
            }
            catch (JMException e) {
                log.error("MBean for " + m.name() + " can't be created.", e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        Ignite ignite = ignite();

        if (ignite == null)
            return;

        MBeanServer jmx = ignite.configuration().getMBeanServer();

        for (ObjectName bean : mBeans) {
            try {
                jmx.unregisterMBean(bean);

                if (log.isDebugEnabled())
                    log.debug("Unregistered SPI MBean: " + bean);
            }
            catch (JMException e) {
                log.error("Failed to unregister SPI MBean: " + bean, e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void setMetricRegistry(ReadOnlyMetricRegistry reg) {
        this.mreg = reg;
    }

    /** {@inheritDoc} */
    @Override public void setExportFilter(Predicate<MetricGroup> filter) {
        this.filter = filter;
    }
}
