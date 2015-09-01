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

package org.apache.ignite.internal.processors.service;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;

/**
 * Service per-node assignment.
 */
public class GridServiceAssignments implements Serializable, GridCacheInternal {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private final UUID nodeId;

    /** Topology version. */
    private final long topVer;

    /** Service configuration. */
    private final ServiceConfiguration cfg;

    /** Assignments. */
    @GridToStringInclude
    private Map<UUID, Integer> assigns = Collections.emptyMap();

    /**
     * @param cfg Configuration.
     * @param nodeId Node ID.
     * @param topVer Topology version.
     */
    public GridServiceAssignments(ServiceConfiguration cfg, UUID nodeId, long topVer) {
        this.cfg = cfg;
        this.nodeId = nodeId;
        this.topVer = topVer;
    }

    /**
     * @return Configuration.
     */
    public ServiceConfiguration configuration() {
        return cfg;
    }

    /**
     * @return Service name.
     */
    public String name() {
        return cfg.getName();
    }

    /**
     * @return Service.
     */
    public Service service() {
        return cfg.getService();
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cfg.getCacheName();
    }

    /**
     * @return Affinity key.
     */
    public Object affinityKey() {
        return cfg.getAffinityKey();
    }

    /**
     * @return Origin node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Node filter.
     */
    public IgnitePredicate<ClusterNode> nodeFilter() {
        return cfg.getNodeFilter();
    }

    /**
     * @return Assignments.
     */
    public Map<UUID, Integer> assigns() {
        return assigns;
    }

    /**
     * @param assigns Assignments.
     */
    public void assigns(Map<UUID, Integer> assigns) {
        this.assigns = assigns;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceAssignments.class, this);
    }
}