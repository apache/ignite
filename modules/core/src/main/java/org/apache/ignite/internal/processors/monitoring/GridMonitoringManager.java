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
import org.apache.ignite.internal.processors.monitoring.sensor.DoubleSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.LongSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.Sensor;
import org.apache.ignite.spi.monitoring.MonitoringExposerSpi;

/**
 *
 */
public class GridMonitoringManager extends GridManagerAdapter<MonitoringExposerSpi> {
    Map<MonitoringGroup, Map<String, Sensor>> sensors = new HashMap<>();

    Map<MonitoringGroup, Map<String, MonitoringList<?, ?>>> lists = new HashMap<>();

    Map<MonitoringGroup, Map<String, Object>> values = new HashMap<>();

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
        value(MonitoringGroup.INSTANCE_INFO, "igniteInstanceName", ctx.igniteInstanceName());
        value(MonitoringGroup.INSTANCE_INFO, "nodeId", ctx.localNodeId());
        value(MonitoringGroup.INSTANCE_INFO, "version", ctx.grid().version().toString());
        value(MonitoringGroup.INSTANCE_INFO, "consistentId", ctx.grid().localNode().consistentId());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();
    }

    public LongSensor longSensor(MonitoringGroup group, String name) {
        return longSensor(group, name, 0);
    }

    public LongSensor longSensor(MonitoringGroup group, String name, long value) {
        Map<String, Sensor> sensorGroups = sensors.computeIfAbsent(group, g -> new HashMap<>());

        LongSensor sensor = new LongSensor(group, name, value);

        Sensor old = sensorGroups.putIfAbsent(name, sensor);

        assert old == null;

        return sensor;
    }

    public DoubleSensor doubleSensor(MonitoringGroup group, String name) {
        return doubleSensor(group, name, 0);
    }

    public DoubleSensor doubleSensor(MonitoringGroup group, String name, double value) {
        Map<String, Sensor> sensorGroups = sensors.computeIfAbsent(group, g -> new HashMap<>());

        DoubleSensor sensor = new DoubleSensor(group, name, value);

        Sensor old = sensorGroups.putIfAbsent(name, sensor);

        assert old == null;

        return sensor;
    }

    public <Id, Row> MonitoringList<Id, Row> list(MonitoringGroup group, String name) {
        Map<String, MonitoringList<?, ?>> lists = this.lists.computeIfAbsent(group, g -> new HashMap<>());

        MonitoringList<Id, Row> list = new MonitoringListImpl<>(group, name);

        MonitoringList<?, ?> old = lists.putIfAbsent(name, list);

        assert old == null;

        return list;
    }

    public void value(MonitoringGroup group, String name, Object value) {
        values
            .computeIfAbsent(group, g -> new HashMap<>())
            .put(name, value);
    }

    public Map<MonitoringGroup, Map<String, Sensor>> sensors() {
        return sensors;
    }

    public Map<MonitoringGroup, Map<String, MonitoringList<?, ?>>> lists() {
        return lists;
    }

    public Map<MonitoringGroup, Map<String, Object>> values() {
        return values;
    }
}
