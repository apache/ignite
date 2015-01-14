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

package org.gridgain.grid.kernal.visor.node;

import org.gridgain.grid.kernal.visor.cache.*;
import org.gridgain.grid.kernal.visor.event.*;
import org.gridgain.grid.kernal.visor.ggfs.*;
import org.gridgain.grid.kernal.visor.streamer.*;

import java.io.*;
import java.util.*;

/**
 * Data collector task result.
 */
public class VisorNodeDataCollectorTaskResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unhandled exceptions from nodes. */
    private final Map<UUID, Throwable> unhandledEx = new HashMap<>();

    /** Nodes grid names. */
    private final Map<UUID, String> gridNames = new HashMap<>();

    /** Nodes topology versions. */
    private final Map<UUID, Long> topologyVersions = new HashMap<>();

    /** All task monitoring state collected from nodes. */
    private final Map<UUID, Boolean> taskMonitoringEnabled = new HashMap<>();

    /** All events collected from nodes. */
    private final List<VisorGridEvent> events = new ArrayList<>();

    /** Exceptions caught during collecting events from nodes. */
    private final Map<UUID, Throwable> eventsEx = new HashMap<>();

    /** All caches collected from nodes. */
    private final Map<UUID, Collection<VisorCache>> caches = new HashMap<>();

    /** Exceptions caught during collecting caches from nodes. */
    private final Map<UUID, Throwable> cachesEx = new HashMap<>();

    /** All GGFS collected from nodes. */
    private final Map<UUID, Collection<VisorGgfs>> ggfss = new HashMap<>();

    /** All GGFS endpoints collected from nodes. */
    private final Map<UUID, Collection<VisorGgfsEndpoint>> ggfsEndpoints = new HashMap<>();

    /** Exceptions caught during collecting GGFS from nodes. */
    private final Map<UUID, Throwable> ggfssEx = new HashMap<>();

    /** All streamers collected from nodes. */
    private final Map<UUID, Collection<VisorStreamer>> streamers = new HashMap<>();

    /** Exceptions caught during collecting streamers from nodes. */
    private final Map<UUID, Throwable> streamersEx = new HashMap<>();

    /**
     * @return {@code true} If no data was collected.
     */
    public boolean isEmpty() {
        return
            gridNames.isEmpty() &&
                topologyVersions.isEmpty() &&
                unhandledEx.isEmpty() &&
                taskMonitoringEnabled.isEmpty() &&
                events.isEmpty() &&
                eventsEx.isEmpty() &&
                caches.isEmpty() &&
                cachesEx.isEmpty() &&
                ggfss.isEmpty() &&
                ggfsEndpoints.isEmpty() &&
                ggfssEx.isEmpty() &&
                streamers.isEmpty() &&
                streamersEx.isEmpty();
    }

    /**
     * @return Unhandled exceptions from nodes.
     */
    public Map<UUID, Throwable> unhandledEx() {
        return unhandledEx;
    }

    /**
     * @return Nodes grid names.
     */
    public Map<UUID, String> gridNames() {
        return gridNames;
    }

    /**
     * @return Nodes topology versions.
     */
    public Map<UUID, Long> topologyVersions() {
        return topologyVersions;
    }

    /**
     * @return All task monitoring state collected from nodes.
     */
    public Map<UUID, Boolean> taskMonitoringEnabled() {
        return taskMonitoringEnabled;
    }

    /**
     * @return All events collected from nodes.
     */
    public List<VisorGridEvent> events() {
        return events;
    }

    /**
     * @return Exceptions caught during collecting events from nodes.
     */
    public Map<UUID, Throwable> eventsEx() {
        return eventsEx;
    }

    /**
     * @return All caches collected from nodes.
     */
    public Map<UUID, Collection<VisorCache>> caches() {
        return caches;
    }

    /**
     * @return Exceptions caught during collecting caches from nodes.
     */
    public Map<UUID, Throwable> cachesEx() {
        return cachesEx;
    }

    /**
     * @return All GGFS collected from nodes.
     */
    public Map<UUID, Collection<VisorGgfs>> ggfss() {
        return ggfss;
    }

    /**
     * @return All GGFS endpoints collected from nodes.
     */
    public Map<UUID, Collection<VisorGgfsEndpoint>> ggfsEndpoints() {
        return ggfsEndpoints;
    }

    /**
     * @return Exceptions caught during collecting GGFS from nodes.
     */
    public Map<UUID, Throwable> ggfssEx() {
        return ggfssEx;
    }

    /**
     * @return All streamers collected from nodes.
     */
    public Map<UUID, Collection<VisorStreamer>> streamers() {
        return streamers;
    }

    /**
     * @return Exceptions caught during collecting streamers from nodes.
     */
    public Map<UUID, Throwable> streamersEx() {
        return streamersEx;
    }
}
