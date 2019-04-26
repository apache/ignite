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

package org.apache.ignite.internal.processors.monitoring;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.monitoring.lists.MonitoringList;
import org.apache.ignite.internal.processors.monitoring.lists.MonitoringListImpl;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroupImpl;
import org.apache.ignite.spi.monitoring.MonitoringExposerSpi;

/**
 *
 */
public class GridMonitoringManager extends GridManagerAdapter<MonitoringExposerSpi> {
    Map<MonitoringGroup, SensorGroup> sensors = new HashMap<>();

    Map<String, SensorGroup> sensorGroups = new HashMap<>();

    Map<MonitoringGroup, Map<String, MonitoringList<?, ?>>> lists = new HashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public GridMonitoringManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getMonitoringExposerSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        for (MonitoringExposerSpi spi : getSpis())
            spi.setMonitoringProcessor(this);

        startSpi();
    }

    @Override protected void onKernalStart0() throws IgniteCheckedException {
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();
    }

    public SensorGroup<MonitoringGroup> sensors(MonitoringGroup group) {
        return sensors.computeIfAbsent(group, g -> new SensorGroupImpl(group));
    }

    public SensorGroup<String> sensorsGroup(String name) {
        return sensorGroups.computeIfAbsent(name, n -> new SensorGroupImpl(name));
    }

    public <Id, Row> MonitoringList<Id, Row> list(MonitoringGroup group, String name) {
        Map<String, MonitoringList<?, ?>> lists = this.lists.computeIfAbsent(group, g -> new HashMap<>());

        MonitoringList<Id, Row> list = new MonitoringListImpl<>(group, name);

        MonitoringList<?, ?> old = lists.putIfAbsent(name, list);

        assert old == null;

        return list;
    }

    public Map<MonitoringGroup, SensorGroup> sensors() {
        return sensors;
    }

    public Map<String, SensorGroup> sensorGroups() {
        return sensorGroups;
    }

    public Map<MonitoringGroup, Map<String, MonitoringList<?, ?>>> lists() {
        return lists;
    }
}
