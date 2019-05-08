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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class GridMonitoringManager extends GridManagerAdapter<MonitoringExposerSpi> {
    /** */
    private ConcurrentMap<MonitoringGroup, ConcurrentMap<String, SensorGroup>> sensorGrps = new ConcurrentHashMap<>();

    /** */
    private ConcurrentMap<MonitoringGroup, ConcurrentMap<String, MonitoringList<?, ?>>> lists = new ConcurrentHashMap<>();

    /** CONCURRENCY! */
    private List<Consumer<SensorGroup>> sensorGrpCreationLsnrs = new ArrayList<>();

    /** CONCURRENCY! */
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
        for (MonitoringExposerSpi spi : getSpis())
            spi.onKernalStart0();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();
    }

    /** */
    public SensorGroup sensorsGroup(MonitoringGroup monGrp, Consumer<SensorGroup> updater) {
        return sensorsGroup(monGrp, null, updater);
    }

    /** */
    public SensorGroup sensorsGroup(MonitoringGroup monGrp) {
        return sensorsGroup(monGrp, null, null);
    }

    /** */
    public SensorGroup sensorsGroup(MonitoringGroup monGrp, @Nullable String name) {
        return sensorsGroup(monGrp, name, null);
    }

    /** */
    public SensorGroup sensorsGroup(MonitoringGroup monGrp, @Nullable String name,
        @Nullable Consumer<SensorGroup> updater) {
        if (name == null)
            name = "";

        ConcurrentMap<String, SensorGroup> grps = sensorGrps.computeIfAbsent(monGrp, mg -> new ConcurrentHashMap<>());

        SensorGroup newGrp = new SensorGroupImpl(monGrp, name, updater);

        SensorGroup oldGrp = grps.putIfAbsent(name, newGrp);

        if (oldGrp == null) {
            notifyListeners(newGrp, sensorGrpCreationLsnrs);

            return newGrp;
        }

        return oldGrp;
    }

    /** */
    public <Id, Row> MonitoringList<Id, Row> list(MonitoringGroup grp, String name, Class<Row> rowClass) {
        Map<String, MonitoringList<?, ?>> grpLists = this.lists.computeIfAbsent(grp, g -> new ConcurrentHashMap<>());

        MonitoringList<Id, Row> newList = new MonitoringListImpl<>(grp, name, rowClass);

        MonitoringList<Id, Row> oldList = (MonitoringList<Id, Row>) grpLists.putIfAbsent(name, newList);

        if (oldList == null) {
            notifyListeners(new T2(grp, newList), listCreationLsnrs);

            return newList;
        }

        return oldList;
    }

    /** */
    public ConcurrentMap<MonitoringGroup, ConcurrentMap<String, SensorGroup>> sensorGroups() {
        return sensorGrps;
    }

    /** */
    public ConcurrentMap<MonitoringGroup, ConcurrentMap<String, MonitoringList<?, ?>>> lists() {
        return lists;
    }

    /** */
    public void addSensorGroupCreationListener(Consumer<SensorGroup> lsnr) {
        sensorGrpCreationLsnrs.add(lsnr);
    }

    /** */
    public void addListCreationListener(Consumer<T2<MonitoringGroup, MonitoringList<?, ?>>> lsnr) {
        listCreationLsnrs.add(lsnr);
    }

    /** */
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
