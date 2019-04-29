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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.monitoring.lists.MonitoringList;
import org.apache.ignite.internal.processors.monitoring.lists.MonitoringListImpl;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroupImpl;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.monitoring.MonitoringExposerSpi;

/**
 *
 */
public class GridMonitoringManager extends GridManagerAdapter<MonitoringExposerSpi> {
    private Map<MonitoringGroup, SensorGroup> sensors = new HashMap<>();

    private Map<String, SensorGroup> sensorGrps = new HashMap<>();

    private Map<MonitoringGroup, Map<String, MonitoringList<?, ?>>> lists = new HashMap<>();

    private List<Consumer<SensorGroup<String>>> sensorGrpCreationLsnrs = new ArrayList<>();

    private List<Consumer<SensorGroup<MonitoringGroup>>> sensorsCreationLsnrs = new ArrayList<>();

    private List<Consumer<T2<MonitoringGroup, MonitoringList<?, ?>>>> listCreationLsnrs = new ArrayList<>();

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

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();
    }

    public SensorGroup<MonitoringGroup> sensors(MonitoringGroup grp) {
        SensorGroup<MonitoringGroup> sensorGrp = sensors.computeIfAbsent(grp, g -> new SensorGroupImpl(grp));

        notifyListeners(sensorGrp, sensorsCreationLsnrs);

        return sensorGrp;
    }

    public SensorGroup<String> sensorsGroup(String name) {
        SensorGroup<String> grp = sensorGrps.computeIfAbsent(name, n -> new SensorGroupImpl(name));

        notifyListeners(grp, sensorGrpCreationLsnrs);

        return grp;
    }

    public <Id, Row> MonitoringList<Id, Row> list(
        MonitoringGroup grp, String name, Class<Row> rowClass) {
        Map<String, MonitoringList<?, ?>> lists = this.lists.computeIfAbsent(grp, g -> new HashMap<>());

        MonitoringList<Id, Row> list = new MonitoringListImpl<>(grp, name);

        MonitoringList<?, ?> old = lists.putIfAbsent(name, list);

        assert old == null;

        notifyListeners(new T2(grp, list), listCreationLsnrs);

        return list;
    }

    public Map<MonitoringGroup, SensorGroup> sensors() {
        return sensors;
    }

    public Map<String, SensorGroup> sensorGroups() {
        return sensorGrps;
    }

    public Map<MonitoringGroup, Map<String, MonitoringList<?, ?>>> lists() {
        return lists;
    }

    public void addSensorGroupCreationListener(Consumer<SensorGroup<String>> lsnr) {
        sensorGrpCreationLsnrs.add(lsnr);
    }

    public void addSensorsCreationListener(Consumer<SensorGroup<MonitoringGroup>> lsnr) {
        sensorsCreationLsnrs.add(lsnr);
    }

    public void addListCreationListener(Consumer<T2<MonitoringGroup, MonitoringList<?, ?>>> lsnr) {
        listCreationLsnrs.add(lsnr);
    }

    private <T> void notifyListeners(T t, List<Consumer<T>> lsnrs) {
        for (Consumer<T> lsnr : lsnrs) {
            try {
                lsnr.accept(t);
            }
            catch (Exception e) {
                log.warning("Listener error", e);
            }
        }
    }
}
