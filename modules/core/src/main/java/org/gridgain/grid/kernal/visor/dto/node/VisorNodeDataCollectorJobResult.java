package org.gridgain.grid.kernal.visor.dto.node;

import org.gridgain.grid.kernal.visor.dto.cache.*;
import org.gridgain.grid.kernal.visor.dto.event.*;
import org.gridgain.grid.kernal.visor.dto.ggfs.*;
import org.gridgain.grid.kernal.visor.dto.streamer.*;

import java.io.*;
import java.util.*;

/**
 * Data collector job result.
 */
public class VisorNodeDataCollectorJobResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid name. */
    private String gridName;

    /** Node topology version. */
    private long topologyVersion;

    /** Task monitoring state collected from node. */
    private boolean taskMonitoringEnabled;

    /** Node events. */
    private final Collection<VisorGridEvent> events = new ArrayList<>();

    /** Exception while collecting node events. */
    private Throwable eventsEx;

    /** Node caches. */
    private final Collection<VisorCache> caches = new ArrayList<>();

    /** Exception while collecting node caches. */
    private Throwable cachesEx;

    /** Node GGFSs. */
    private final Collection<VisorGgfs> ggfss = new ArrayList<>();

    /** All GGFS endpoints collected from nodes. */
    private final Collection<VisorGgfsEndpoint> ggfsEndpoints = new ArrayList<>();

    /** Exception while collecting node GGFSs. */
    private Throwable ggfssEx;

    /** Node streamers. */
    private final Collection<VisorStreamer> streamers = new ArrayList<>();

    /** Exception while collecting node streamers. */
    private Throwable streamersEx;

    public String gridName() {
        return gridName;
    }

    public void gridName(String gridName) {
        this.gridName = gridName;
    }

    public long topologyVersion() {
        return topologyVersion;
    }

    public void topologyVersion(long topologyVersion) {
        this.topologyVersion = topologyVersion;
    }

    public boolean taskMonitoringEnabled() {
        return taskMonitoringEnabled;
    }

    public void taskMonitoringEnabled(boolean taskMonitoringEnabled) {
        this.taskMonitoringEnabled = taskMonitoringEnabled;
    }

    public Collection<VisorGridEvent> events() {
        return events;
    }

    public Throwable eventsEx() {
        return eventsEx;
    }

    public void eventsEx(Throwable eventsEx) {
        this.eventsEx = eventsEx;
    }

    public Collection<VisorCache> caches() {
        return caches;
    }

    public Throwable cachesEx() {
        return cachesEx;
    }

    public void cachesEx(Throwable cachesEx) {
        this.cachesEx = cachesEx;
    }

    public Collection<VisorGgfs> ggfss() {
        return ggfss;
    }

    public Collection<VisorGgfsEndpoint> ggfsEndpoints() {
        return ggfsEndpoints;
    }

    public Throwable ggfssEx() {
        return ggfssEx;
    }

    public void ggfssEx(Throwable ggfssEx) {
        this.ggfssEx = ggfssEx;
    }

    public Collection<VisorStreamer> streamers() {
        return streamers;
    }

    public Throwable streamersEx() {
        return streamersEx;
    }

    public void streamersEx(Throwable streamersEx) {
        this.streamersEx = streamersEx;
    }
}
