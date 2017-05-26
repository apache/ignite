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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.internal.visor.cache.VisorCache;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.internal.visor.igfs.VisorIgfs;
import org.apache.ignite.internal.visor.igfs.VisorIgfsEndpoint;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

/**
 * Data collector task result.
 */
public class VisorNodeDataCollectorTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid active flag. */
    private boolean active;

    /** Unhandled exceptions from nodes. */
    private Map<UUID, VisorExceptionWrapper> unhandledEx = new HashMap<>();

    /** Nodes grid names. */
    private Map<UUID, String> gridNames = new HashMap<>();

    /** Nodes topology versions. */
    private Map<UUID, Long> topVersions = new HashMap<>();

    /** All task monitoring state collected from nodes. */
    private Map<UUID, Boolean> taskMonitoringEnabled = new HashMap<>();

    /** Nodes error counts. */
    private Map<UUID, Long> errCnts = new HashMap<>();

    /** All events collected from nodes. */
    private List<VisorGridEvent> evts = new ArrayList<>();

    /** Exceptions caught during collecting events from nodes. */
    private Map<UUID, VisorExceptionWrapper> evtsEx = new HashMap<>();

    /** All caches collected from nodes. */
    private Map<UUID, Collection<VisorCache>> caches = new HashMap<>();

    /** Exceptions caught during collecting caches from nodes. */
    private Map<UUID, VisorExceptionWrapper> cachesEx = new HashMap<>();

    /** All IGFS collected from nodes. */
    private Map<UUID, Collection<VisorIgfs>> igfss = new HashMap<>();

    /** All IGFS endpoints collected from nodes. */
    private Map<UUID, Collection<VisorIgfsEndpoint>> igfsEndpoints = new HashMap<>();

    /** Exceptions caught during collecting IGFS from nodes. */
    private Map<UUID, VisorExceptionWrapper> igfssEx = new HashMap<>();

    /** Topology version of latest completed partition exchange from nodes. */
    private Map<UUID, VisorAffinityTopologyVersion> readyTopVers = new HashMap<>();

    /** Whether pending exchange future exists from nodes. */
    private Map<UUID, Boolean> pendingExchanges = new HashMap<>();

    /**
     * Default constructor.
     */
    public VisorNodeDataCollectorTaskResult() {
        // No-op.
    }

    /**
     * @return {@code true} If no data was collected.
     */
    public boolean isEmpty() {
        return
            gridNames.isEmpty() &&
            topVersions.isEmpty() &&
            unhandledEx.isEmpty() &&
            taskMonitoringEnabled.isEmpty() &&
            evts.isEmpty() &&
            evtsEx.isEmpty() &&
            caches.isEmpty() &&
            cachesEx.isEmpty() &&
            igfss.isEmpty() &&
            igfsEndpoints.isEmpty() &&
            igfssEx.isEmpty() &&
            readyTopVers.isEmpty() &&
            pendingExchanges.isEmpty();
    }

    /**
     * @return {@code True} if grid is active.
     */
    public boolean isActive() {
        return active;
    }

    /**
     * @param active active New value of grid active flag.
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @return Unhandled exceptions from nodes.
     */
    public Map<UUID, VisorExceptionWrapper> getUnhandledEx() {
        return unhandledEx;
    }

    /**
     * @return Nodes grid names.
     */
    public Map<UUID, String> getGridNames() {
        return gridNames;
    }

    /**
     * @return Nodes topology versions.
     */
    public Map<UUID, Long> getTopologyVersions() {
        return topVersions;
    }

    /**
     * @return All task monitoring state collected from nodes.
     */
    public Map<UUID, Boolean> isTaskMonitoringEnabled() {
        return taskMonitoringEnabled;
    }

    /**
     * @return All events collected from nodes.
     */
    public List<VisorGridEvent> getEvents() {
        return evts;
    }

    /**
     * @return Exceptions caught during collecting events from nodes.
     */
    public Map<UUID, VisorExceptionWrapper> getEventsEx() {
        return evtsEx;
    }

    /**
     * @return All caches collected from nodes.
     */
    public Map<UUID, Collection<VisorCache>> getCaches() {
        return caches;
    }

    /**
     * @return Exceptions caught during collecting caches from nodes.
     */
    public Map<UUID, VisorExceptionWrapper> getCachesEx() {
        return cachesEx;
    }

    /**
     * @return All IGFS collected from nodes.
     */
    public Map<UUID, Collection<VisorIgfs>> getIgfss() {
        return igfss;
    }

    /**
     * @return All IGFS endpoints collected from nodes.
     */
    public Map<UUID, Collection<VisorIgfsEndpoint>> getIgfsEndpoints() {
        return igfsEndpoints;
    }

    /**
     * @return Exceptions caught during collecting IGFS from nodes.
     */
    public Map<UUID, VisorExceptionWrapper> getIgfssEx() {
        return igfssEx;
    }

    /**
     * @return Nodes error counts.
     */
    public Map<UUID, Long> getErrorCounts() {
        return errCnts;
    }

    /**
     * @return Topology version of latest completed partition exchange from nodes.
     */
    public Map<UUID, VisorAffinityTopologyVersion> getReadyAffinityVersions() {
        return readyTopVers;
    }

    /**
     * @return Whether pending exchange future exists from nodes.
     */
    public Map<UUID, Boolean> getPendingExchanges() {
        return pendingExchanges;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(active);
        U.writeMap(out, unhandledEx);
        U.writeMap(out, gridNames);
        U.writeMap(out, topVersions);
        U.writeMap(out, taskMonitoringEnabled);
        U.writeMap(out, errCnts);
        U.writeCollection(out, evts);
        U.writeMap(out, evtsEx);
        U.writeMap(out, caches);
        U.writeMap(out, cachesEx);
        U.writeMap(out, igfss);
        U.writeMap(out, igfsEndpoints);
        U.writeMap(out, igfssEx);
        U.writeMap(out, readyTopVers);
        U.writeMap(out, pendingExchanges);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        active = in.readBoolean();
        unhandledEx = U.readMap(in);
        gridNames = U.readMap(in);
        topVersions = U.readMap(in);
        taskMonitoringEnabled = U.readMap(in);
        errCnts = U.readMap(in);
        evts = U.readList(in);
        evtsEx = U.readMap(in);
        caches = U.readMap(in);
        cachesEx = U.readMap(in);
        igfss = U.readMap(in);
        igfsEndpoints = U.readMap(in);
        igfssEx = U.readMap(in);
        readyTopVers = U.readMap(in);
        pendingExchanges = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeDataCollectorTaskResult.class, this);
    }
}
