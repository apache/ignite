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

package org.apache.ignite.spi.systemview.jmx;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.systemview.ReadOnlySystemViewRegistry;
import org.apache.ignite.spi.systemview.SystemViewExporterSpi;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.spi.systemview.jmx.SystemViewMBean.VIEWS;

/**
 * This SPI implementation exports system views as JMX beans.
 */
public class JmxSystemViewExporterSpi extends IgniteSpiAdapter implements SystemViewExporterSpi {
    /** System view registry. */
    private ReadOnlySystemViewRegistry sysViewReg;

    /** System view filter. */
    @Nullable private Predicate<SystemView<?>> filter;

    /** Registered beans. */
    private final List<ObjectName> mBeans = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        sysViewReg.forEach(this::register);

        sysViewReg.addSystemViewCreationListener(this::register);
    }

    /**
     * Registers JMX bean for specific system view.
     *
     * @param sysView System view.
     */
    protected void register(SystemView<?> sysView) {
        if (filter != null && !filter.test(sysView)) {
            if (log.isDebugEnabled())
                log.debug("System view filtered and will not be registered [name=" + sysView.name() + ']');

            return;
        }
        else if (log.isDebugEnabled())
            log.debug("Found new system view [name=" + sysView.name() + ']');

        try {
            SystemViewMBean mlBean = new SystemViewMBean<>(sysView);

            ObjectName mbean = U.registerMBean(
                ignite().configuration().getMBeanServer(),
                igniteInstanceName,
                VIEWS,
                sysView.name(),
                mlBean,
                SystemViewMBean.class);

            mBeans.add(mbean);

            if (log.isDebugEnabled())
                log.debug("MetricRegistry MBean created [mbean=" + mbean + ']');
        }
        catch (JMException e) {
            log.error("MBean for system view '" + sysView.name() + "' can't be created.", e);
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
    @Override public void setSystemViewRegistry(ReadOnlySystemViewRegistry sysViewReg) {
        this.sysViewReg = sysViewReg;
    }

    /** {@inheritDoc} */
    @Override public void setExportFilter(Predicate<SystemView<?>> filter) {
        this.filter = filter;
    }
}
