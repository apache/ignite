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

package org.apache.ignite.spi;

import java.text.DateFormat;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.monitoring.GridMonitoringManager;
import org.apache.ignite.internal.processors.monitoring.sensor.ClosureSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.LongClosureSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.LongSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class provides convenient adapter for MBean implementations.
 *
 * @deprecated Use GridMonitoringManager instead.
 */
@Deprecated
public class IgniteSpiMBeanAdapter implements IgniteSpiManagementMBean {
    private final String name;

    private final ClosureSensor<String> startTimestampFormatted;

    private final ClosureSensor<String> upTimeFormatted;

    private final LongSensor startTimestamp;

    private final LongClosureSensor upTime;

    private final ClosureSensor<UUID> localNodeId;

    private final ClosureSensor<String> igniteHome;

    /**
     * Constructor
     *
     * @param spiAdapter Spi implementation.
     */
    public IgniteSpiMBeanAdapter(IgniteSpiAdapter spiAdapter) {
        name = spiAdapter.getName();

        GridMonitoringManager mon = ((IgniteEx)spiAdapter.ignite).context().monitoring();

        SensorGroup<String> spiSensors = mon.sensorsGroup(name);

        startTimestampFormatted = spiSensors.sensor("startTimestampFormatted",
            () -> DateFormat.getDateTimeInstance().format(new Date(spiAdapter.getStartTstamp())));

        upTimeFormatted = spiSensors.sensor("upTimeFormatted", () -> X.timeSpan2DHMSM(getUpTime()));

        startTimestamp = spiSensors.longSensor("startTimestamp", spiAdapter.getStartTstamp());

        upTime = spiSensors.longSensor("upTime", new LongClosureSensor.LongClosure() {
            @Override public long call() {
                final long startTstamp = startTimestamp.getValue();

                return startTstamp == 0 ? 0 : U.currentTimeMillis() - startTstamp;
            }
        });

        localNodeId = spiSensors.sensor("localNodeId", () -> spiAdapter.ignite.cluster().localNode().id());

        igniteHome = spiSensors.sensor("igniteHome", () -> spiAdapter.ignite.configuration().getIgniteHome());
    }

    /** {@inheritDoc} */
    @Override public final String getStartTimestampFormatted() {
        return startTimestampFormatted.getValue();
    }

    /** {@inheritDoc} */
    @Override public final String getUpTimeFormatted() {
        return upTimeFormatted.getValue();
    }

    /** {@inheritDoc} */
    @Override public final long getStartTimestamp() {
        return startTimestamp.getValue();
    }

    /** {@inheritDoc} */
    @Override public final long getUpTime() {
        return upTime.getValue();
    }

    /** {@inheritDoc} */
    @Override public UUID getLocalNodeId() {
        return localNodeId.getValue();
    }

    /** {@inheritDoc} */
    @Override public final String getIgniteHome() {
        return igniteHome.getValue();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }
}
