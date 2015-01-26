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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.tostring.*;

import java.util.*;

/**
 * Topology snapshot managed by discovery manager.
 */
public class GridDiscoveryTopologySnapshot {
    /** Topology version. */
    private long topVer;

    /** Topology nodes. */
    @GridToStringInclude
    private Collection<ClusterNode> topNodes;

    /**
     * Creates a topology snapshot with given topology version and topology nodes.
     *
     * @param topVer Topology version.
     * @param topNodes Topology nodes.
     */
    public GridDiscoveryTopologySnapshot(long topVer, Collection<ClusterNode> topNodes) {
        this.topVer = topVer;
        this.topNodes = topNodes;
    }

    /** {@inheritDoc} */
    public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    public Collection<ClusterNode> topologyNodes() {
        return topNodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDiscoveryTopologySnapshot.class, this);
    }
}
