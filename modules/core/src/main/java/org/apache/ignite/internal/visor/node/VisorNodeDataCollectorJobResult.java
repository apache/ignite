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
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.internal.visor.cache.VisorCache;
import org.apache.ignite.internal.visor.cache.VisorMemoryMetrics;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.internal.visor.igfs.VisorIgfs;
import org.apache.ignite.internal.visor.igfs.VisorIgfsEndpoint;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

/**
 * Data collector job result.
 */
public class VisorNodeDataCollectorJobResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid name. */
    private String gridName;

    /** Node topology version. */
    private long topVer;

    /** Task monitoring state collected from node. */
    private boolean taskMonitoringEnabled;

    /** Node events. */
    private List<VisorGridEvent> evts = new ArrayList<>();

    /** Exception while collecting node events. */
    private VisorExceptionWrapper evtsEx;

    /** Node data region metrics. */
    private List<VisorMemoryMetrics> memoryMetrics = new ArrayList<>();

    /** Exception while collecting memory metrics. */
    private VisorExceptionWrapper memoryMetricsEx;

    /** Node caches. */
    private List<VisorCache> caches = new ArrayList<>();

    /** Exception while collecting node caches. */
    private VisorExceptionWrapper cachesEx;

    /** Node IGFSs. */
    private List<VisorIgfs> igfss = new ArrayList<>();

    /** All IGFS endpoints collected from nodes. */
    private List<VisorIgfsEndpoint> igfsEndpoints = new ArrayList<>();

    /** Exception while collecting node IGFSs. */
    private VisorExceptionWrapper igfssEx;

    /** Errors count. */
    private long errCnt;

    /** Topology version of latest completed partition exchange. */
    private VisorAffinityTopologyVersion readyTopVer;

    /** Whether pending exchange future exists. */
    private boolean hasPendingExchange;

    /** Persistence metrics. */
    private VisorPersistenceMetrics persistenceMetrics;

    /** Exception while collecting persistence metrics. */
    private VisorExceptionWrapper persistenceMetricsEx;

    /** Rebalance percent. */
    private double rebalance;

    /**
     * Default constructor.
     */
    public VisorNodeDataCollectorJobResult() {
        // No-op.
    }

    /**
     * @return Grid name.
     */
    public String getGridName() {
        return gridName;
    }

    /**
     * @param gridName New grid name value.
     */
    public void setGridName(String gridName) {
        this.gridName = gridName;
    }

    /**
     * @return Current topology version.
     */
    public long getTopologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version value.
     */
    public void setTopologyVersion(long topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Current task monitoring state.
     */
    public boolean isTaskMonitoringEnabled() {
        return taskMonitoringEnabled;
    }

    /**
     * @param taskMonitoringEnabled New value of task monitoring state.
     */
    public void setTaskMonitoringEnabled(boolean taskMonitoringEnabled) {
        this.taskMonitoringEnabled = taskMonitoringEnabled;
    }

    /**
     * @return Collection of collected events.
     */
    public List<VisorGridEvent> getEvents() {
        return evts;
    }

    /**
     * @return Exception caught during collecting events.
     */
    public VisorExceptionWrapper getEventsEx() {
        return evtsEx;
    }

    /**
     * @param evtsEx Exception caught during collecting events.
     */
    public void setEventsEx(VisorExceptionWrapper evtsEx) {
        this.evtsEx = evtsEx;
    }

    /**
     * @return Collected data region metrics.
     */
    public List<VisorMemoryMetrics> getMemoryMetrics() {
        return memoryMetrics;
    }

    /**
     * @return Exception caught during collecting memory metrics.
     */
    public VisorExceptionWrapper getMemoryMetricsEx() {
        return memoryMetricsEx;
    }

    /**
     * @param memoryMetricsEx Exception caught during collecting memory metrics.
     */
    public void setMemoryMetricsEx(VisorExceptionWrapper memoryMetricsEx) {
        this.memoryMetricsEx = memoryMetricsEx;
    }

    /**
     * @return Collected cache metrics.
     */
    public List<VisorCache> getCaches() {
        return caches;
    }

    /**
     * @return Exception caught during collecting caches metrics.
     */
    public VisorExceptionWrapper getCachesEx() {
        return cachesEx;
    }

    /**
     * @param cachesEx Exception caught during collecting caches metrics.
     */
    public void setCachesEx(VisorExceptionWrapper cachesEx) {
        this.cachesEx = cachesEx;
    }

    /**
     * @return Collected IGFSs metrics.
     */
    public List<VisorIgfs> getIgfss() {
        return igfss;
    }

    /**
     * @return Collected IGFSs endpoints.
     */
    public List<VisorIgfsEndpoint> getIgfsEndpoints() {
        return igfsEndpoints;
    }

    /**
     * @return Exception caught during collecting IGFSs metrics.
     */
    public VisorExceptionWrapper getIgfssEx() {
        return igfssEx;
    }

    /**
     * @param igfssEx Exception caught during collecting IGFSs metrics.
     */
    public void setIgfssEx(VisorExceptionWrapper igfssEx) {
        this.igfssEx = igfssEx;
    }

    /**
     * @return Errors count.
     */
    public long getErrorCount() {
        return errCnt;
    }

    /**
     * @param errCnt Errors count.
     */
    public void setErrorCount(long errCnt) {
        this.errCnt = errCnt;
    }

    /**
     * @return Topology version of latest completed partition exchange.
     */
    public VisorAffinityTopologyVersion getReadyAffinityVersion() {
        return readyTopVer;
    }

    /**
     * @param readyTopVer Topology version of latest completed partition exchange.
     */
    public void setReadyAffinityVersion(VisorAffinityTopologyVersion readyTopVer) {
        this.readyTopVer = readyTopVer;
    }

    /**
     * @return Whether pending exchange future exists.
     */
    public boolean isHasPendingExchange() {
        return hasPendingExchange;
    }

    /**
     * @param hasPendingExchange Whether pending exchange future exists.
     */
    public void setHasPendingExchange(boolean hasPendingExchange) {
        this.hasPendingExchange = hasPendingExchange;
    }

    /**
     * Get persistence metrics.
     */
    public VisorPersistenceMetrics getPersistenceMetrics() {
        return persistenceMetrics;
    }

    /**
     * Set persistence metrics.
     *
     * @param persistenceMetrics Persistence metrics.
     */
    public void setPersistenceMetrics(VisorPersistenceMetrics persistenceMetrics) {
        this.persistenceMetrics = persistenceMetrics;
    }

    /**
     * @return Exception caught during collecting persistence metrics.
     */
    public VisorExceptionWrapper getPersistenceMetricsEx() {
        return persistenceMetricsEx;
    }

    /**
     * @param persistenceMetricsEx Exception caught during collecting persistence metrics.
     */
    public void setPersistenceMetricsEx(VisorExceptionWrapper persistenceMetricsEx) {
        this.persistenceMetricsEx = persistenceMetricsEx;
    }

    /**
     * @return Rebalance progress.
     */
    public double getRebalance() {
        return rebalance;
    }

    /**
     * @param  rebalance Rebalance progress.
     */
    public void setRebalance(double rebalance) {
        this.rebalance = rebalance;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, gridName);
        out.writeLong(topVer);
        out.writeBoolean(taskMonitoringEnabled);
        U.writeCollection(out, evts);
        out.writeObject(evtsEx);
        U.writeCollection(out, memoryMetrics);
        out.writeObject(memoryMetricsEx);
        U.writeCollection(out, caches);
        out.writeObject(cachesEx);
        U.writeCollection(out, igfss);
        U.writeCollection(out, igfsEndpoints);
        out.writeObject(igfssEx);
        out.writeLong(errCnt);
        out.writeObject(readyTopVer);
        out.writeBoolean(hasPendingExchange);
        out.writeObject(persistenceMetrics);
        out.writeObject(persistenceMetricsEx);
        out.writeDouble(rebalance);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        gridName = U.readString(in);
        topVer = in.readLong();
        taskMonitoringEnabled = in.readBoolean();
        evts = U.readList(in);
        evtsEx = (VisorExceptionWrapper)in.readObject();
        memoryMetrics = U.readList(in);
        memoryMetricsEx = (VisorExceptionWrapper)in.readObject();
        caches = U.readList(in);
        cachesEx = (VisorExceptionWrapper)in.readObject();
        igfss = U.readList(in);
        igfsEndpoints = U.readList(in);
        igfssEx = (VisorExceptionWrapper)in.readObject();
        errCnt = in.readLong();
        readyTopVer = (VisorAffinityTopologyVersion)in.readObject();
        hasPendingExchange = in.readBoolean();
        persistenceMetrics = (VisorPersistenceMetrics)in.readObject();
        persistenceMetricsEx = (VisorExceptionWrapper)in.readObject();
        rebalance = (protoVer > V1) ? in.readDouble() : -1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeDataCollectorJobResult.class, this);
    }
}
