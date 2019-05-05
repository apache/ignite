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

package org.apache.ignite.spi.monitoring.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.monitoring.GridMonitoringManager;
import org.apache.ignite.internal.processors.monitoring.MonitoringGroup;
import org.apache.ignite.internal.processors.monitoring.lists.MonitoringList;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.monitoring.MonitoringExposerSpi;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class SqlViewPullExposerSpi extends IgniteSpiAdapter implements MonitoringExposerSpi {
    /** Monitoring manager. */
    private GridMonitoringManager mgr;

    /** */
    private List<SqlSystemView> pendingViews;

    /** */
    private volatile boolean idxStarted;

    /** */
    private Object regMux = new Object();

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        mgr.addSensorGroupCreationListener(this::onSensorGroupCreation);

        mgr.addSensorsCreationListener(this::onSensorCreation);

        mgr.addListCreationListener(this::onListCreation);
    }

    /** */
    private void onListCreation(T2<MonitoringGroup, MonitoringList<?,?>> list) {
        registerSystemView(new ListSystemView(list, ((IgniteEx)ignite()).context()));
    }

    /** */
    private void onSensorCreation(SensorGroup<MonitoringGroup> grp) {
        registerSystemView(new SensorGroupLocalSystemView(grp, ((IgniteEx)ignite()).context()));
    }

    /** */
    private void onSensorGroupCreation(SensorGroup<String> grp) {
        registerSystemView(new SensorGroupLocalSystemView(grp, ((IgniteEx)ignite()).context()));
    }

    /** */
    private void registerSystemView(SqlSystemView view) {
        synchronized (regMux) {
            if (!idxStarted) {
                if (pendingViews == null)
                    pendingViews = new ArrayList<>();

                pendingViews.add(view);

                return;
            }

            GridKernalContext ctx = ((IgniteEx)ignite()).context();

            SchemaManager mgr = ((IgniteH2Indexing)ctx.query().getIndexing()).schemaManager();

            mgr.createSysteView(QueryUtils.SCHEMA_MONITORING, view);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart0() {
        synchronized (regMux) {
            if (idxStarted)
                return;

            idxStarted = true;

            for (SqlSystemView view : pendingViews)
                registerSystemView(view);

            pendingViews = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void setMonitoringProcessor(GridMonitoringManager mgr) {
        this.mgr = mgr;
    }
}
