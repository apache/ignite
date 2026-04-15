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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public class ServiceTopology implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Empty service topology instance. */
    public static final ServiceTopology EMPTY = new ServiceTopology();

    /** Topology snapshot. */
    @GridToStringInclude
    private final Map<UUID, Integer> snapshot;

    /**
     * Whether topology is transitional. Nodes may leave the cluster while the service topology is being recalculated.
     * In this case, the resulting service topology may be incomplete. We consider the mentioned service topology
     * transitional and expect it to be recalculated soon.
     */
    @GridToStringInclude
    private final boolean isTransitional;

    /** */
    private ServiceTopology() {
        snapshot = Collections.emptyMap();
        isTransitional = true;
    }

    /** */
    public ServiceTopology(Map<UUID, Integer> topSnapshot) {
        this(topSnapshot, false);
    }

    /**
     * @param snapshot Service topology snapshot.
     * @param isTransitional Whether topology is transitional. Nodes may leave the cluster while the service topology is being recalculated.
     * In this case, the resulting service topology may be incomplete. We consider the mentioned service topology
     * transitional and expect it to be recalculated soon.
     */
    public ServiceTopology(Map<UUID, Integer> snapshot, boolean isTransitional) {
        this.snapshot = Collections.unmodifiableMap(snapshot);
        this.isTransitional = isTransitional;
    }

    /** */
    public Map<UUID, Integer> snapshot() {
        return snapshot;
    }

    /** */
    public boolean isTransitional() {
        return isTransitional;
    }

    /** */
    public boolean containsNode(UUID nodeId) {
        return snapshot.containsKey(nodeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceTopology.class, this);
    }
}
