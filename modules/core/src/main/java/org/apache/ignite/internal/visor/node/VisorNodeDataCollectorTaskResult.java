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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.visor.cache.VisorCache;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.internal.visor.igfs.VisorIgfs;
import org.apache.ignite.internal.visor.igfs.VisorIgfsEndpoint;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

/**
 * Data collector task result.
 */
public class VisorNodeDataCollectorTaskResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unhandled exceptions from nodes. */
    private final Map<UUID, VisorExceptionWrapper> unhandledEx = new HashMap<>();

    /** Nodes grid names. */
    private final Map<UUID, String> gridNames = new HashMap<>();

    /** Nodes topology versions. */
    private final Map<UUID, Long> topVersions = new HashMap<>();

    /** All task monitoring state collected from nodes. */
    private final Map<UUID, Boolean> taskMonitoringEnabled = new HashMap<>();

    /** Nodes error counts. */
    private final Map<UUID, Long> errCnts = new HashMap<>();

    /** All events collected from nodes. */
    private final List<VisorGridEvent> evts = new ArrayList<>();

    /** Exceptions caught during collecting events from nodes. */
    private final Map<UUID, VisorExceptionWrapper> evtsEx = new HashMap<>();

    /** All caches collected from nodes. */
    private final Map<UUID, Collection<VisorCache>> caches = new HashMap<>();

    /** Exceptions caught during collecting caches from nodes. */
    private final Map<UUID, VisorExceptionWrapper> cachesEx = new HashMap<>();

    /** All IGFS collected from nodes. */
    private final Map<UUID, Collection<VisorIgfs>> igfss = new HashMap<>();

    /** All IGFS endpoints collected from nodes. */
    private final Map<UUID, Collection<VisorIgfsEndpoint>> igfsEndpoints = new HashMap<>();

    /** Exceptions caught during collecting IGFS from nodes. */
    private final Map<UUID, VisorExceptionWrapper> igfssEx = new HashMap<>();

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
            igfssEx.isEmpty();
    }

    /**
     * @return Unhandled exceptions from nodes.
     */
    public Map<UUID, VisorExceptionWrapper> unhandledEx() {
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
        return topVersions;
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
        return evts;
    }

    /**
     * @return Exceptions caught during collecting events from nodes.
     */
    public Map<UUID, VisorExceptionWrapper> eventsEx() {
        return evtsEx;
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
    public Map<UUID, VisorExceptionWrapper> cachesEx() {
        return cachesEx;
    }

    /**
     * @return All IGFS collected from nodes.
     */
    public Map<UUID, Collection<VisorIgfs>> igfss() {
        return igfss;
    }

    /**
     * @return All IGFS endpoints collected from nodes.
     */
    public Map<UUID, Collection<VisorIgfsEndpoint>> igfsEndpoints() {
        return igfsEndpoints;
    }

    /**
     * @return Exceptions caught during collecting IGFS from nodes.
     */
    public Map<UUID, VisorExceptionWrapper> igfssEx() {
        return igfssEx;
    }

    /**
     * @return Nodes error counts.
     */
    public Map<UUID, Long> errorCounts() {
        return errCnts;
    }
}