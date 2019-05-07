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

package org.apache.ignite.spi.monitoring.jmx;

import java.util.HashSet;
import java.util.Set;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.monitoring.GridMonitoringManager;
import org.apache.ignite.internal.processors.monitoring.MonitoringGroup;
import org.apache.ignite.internal.processors.monitoring.lists.MonitoringList;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.monitoring.MonitoringExposerSpi;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class JmxPullExposerSpi extends IgniteSpiAdapter implements MonitoringExposerSpi {
    /** Monitoring manager. */
    private GridMonitoringManager mgr;

    /** MBean names stored to be unregistered later. */
    private final Set<ObjectName> mBeanNames = new HashSet<>();

    /** Logger. */
    @LoggerResource
    protected IgniteLogger log;

    public static final String BASE = "org.apache.ignite";

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        mgr.addSensorGroupCreationListener(this::onSensorGroupCreation);

        mgr.addListCreationListener(this::onListCreation);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    private void onSensorGroupCreation(SensorGroup grp) {
        try {
            String monGrp;
            String grpName;

            if (grp.getName() == null) {
                monGrp = null;
                grpName = grp.getGroup().name();
            }
            else {
                monGrp = grp.getGroup().name();
                grpName = grp.getName();
            }

            registerMBean(monGrp, grpName, new SensorGroupMBean(grp), SensorGroupMBean.class,
                ((IgniteEx)ignite()).context());
        }
        catch (IgniteCheckedException e) {
            log.error("MBean for sensorGroup " + grp.getName() + " can't be created.", e);
        }
    }

    private void onListCreation(T2<MonitoringGroup, MonitoringList<?, ?>> list) {
        try {
            registerMBean("lists", list.get2().getName(), new ListMBean(list.get2()), ListMBean.class,
                ((IgniteEx)ignite()).context());
        }
        catch (IgniteCheckedException e) {
            log.error("MBean for group " + list.get1() + " and list " + list.get2().getName() + " can't be created.", e);
        }
    }

    private <T> void registerMBean(String grp, String name, T impl, Class<T> itf,
        GridKernalContext ctx) throws IgniteCheckedException {
        try {
            ObjectName objName = U.registerMBean(
                ctx.config().getMBeanServer(),
                ctx.config().getIgniteInstanceName(),
                grp, name, impl, itf);

            if (log.isDebugEnabled())
                log.debug("Registered MBean: " + objName);

            mBeanNames.add(objName);
        }
        catch (JMException e) {
            throw new IgniteCheckedException("Failed to register MBean " + name, e);
        }
    }

    /**
     * Unregisters all previously registered MBeans.
     *
     * @return {@code true} if all mbeans were unregistered successfully; {@code false} otherwise.
     */
    private boolean unregisterAllMBeans() {
        boolean success = true;

        for (ObjectName name : mBeanNames)
            success = success && unregisterMBean(name, ((IgniteEx)ignite()).context());

        return success;
    }

    /**
     * TODO: FIXME COPY PASTE from IgniteMBeansManager - needs to be refactored.
     *
     * Unregisters given MBean.
     *
     * @param mbean MBean to unregister.
     * @return {@code true} if successfully unregistered, {@code false} otherwise.
     */
    private boolean unregisterMBean(ObjectName mbean, GridKernalContext ctx) {
        try {
            ctx.config().getMBeanServer().unregisterMBean(mbean);

            if (log.isDebugEnabled())
                log.debug("Unregistered MBean: " + mbean);

            return true;
        }
        catch (JMException e) {
            U.error(log, "Failed to unregister MBean.", e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart0() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setMonitoringProcessor(GridMonitoringManager gridMonitoringManager) {
        this.mgr = gridMonitoringManager;
    }
}
