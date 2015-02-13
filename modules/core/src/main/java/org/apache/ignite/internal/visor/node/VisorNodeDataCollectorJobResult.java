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

import org.apache.ignite.internal.visor.cache.*;
import org.apache.ignite.internal.visor.event.*;
import org.apache.ignite.internal.visor.igfs.*;
import org.apache.ignite.internal.visor.streamer.*;

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
    private final Collection<VisorIgfs> ggfss = new ArrayList<>();

    /** All GGFS endpoints collected from nodes. */
    private final Collection<VisorIgfsEndpoint> ggfsEndpoints = new ArrayList<>();

    /** Exception while collecting node GGFSs. */
    private Throwable ggfssEx;

    /** Node streamers. */
    private final Collection<VisorStreamer> streamers = new ArrayList<>();

    /** Exception while collecting node streamers. */
    private Throwable streamersEx;

    /**
     * @return Grid name.
     */
    public String gridName() {
        return gridName;
    }

    /**
     * @param gridName New grid name value.
     */
    public void gridName(String gridName) {
        this.gridName = gridName;
    }

    /**
     * @return Current topology version.
     */
    public long topologyVersion() {
        return topologyVersion;
    }

    /**
     * @param topologyVersion New topology version value.
     */
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

    public Collection<VisorIgfs> igfss() {
        return ggfss;
    }

    public Collection<VisorIgfsEndpoint> igfsEndpoints() {
        return ggfsEndpoints;
    }

    public Throwable igfssEx() {
        return ggfssEx;
    }

    public void igfssEx(Throwable ggfssEx) {
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
