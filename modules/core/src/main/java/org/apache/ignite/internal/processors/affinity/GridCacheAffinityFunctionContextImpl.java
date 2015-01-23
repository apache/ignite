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

package org.apache.ignite.internal.processors.affinity;

import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache affinity function context implementation. Simple bean that holds all required fields.
 */
public class GridCacheAffinityFunctionContextImpl implements CacheAffinityFunctionContext {
    /** Topology snapshot. */
    private List<ClusterNode> topSnapshot;

    /** Previous affinity assignment. */
    private List<List<ClusterNode>> prevAssignment;

    /** Discovery event that caused this topology change. */
    private IgniteDiscoveryEvent discoEvt;

    /** Topology version. */
    private long topVer;

    /** Number of backups to assign. */
    private int backups;

    /**
     * @param topSnapshot Topology snapshot.
     * @param topVer Topology version.
     */
    public GridCacheAffinityFunctionContextImpl(List<ClusterNode> topSnapshot, List<List<ClusterNode>> prevAssignment,
        IgniteDiscoveryEvent discoEvt, long topVer, int backups) {
        this.topSnapshot = topSnapshot;
        this.prevAssignment = prevAssignment;
        this.discoEvt = discoEvt;
        this.topVer = topVer;
        this.backups = backups;
    }

    /** {@inheritDoc} */
    @Nullable @Override public List<ClusterNode> previousAssignment(int part) {
        return prevAssignment.get(part);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> currentTopologySnapshot() {
        return topSnapshot;
    }

    /** {@inheritDoc} */
    @Override public long currentTopologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteDiscoveryEvent discoveryEvent() {
        return discoEvt;
    }

    /** {@inheritDoc} */
    @Override public int backups() {
        return backups;
    }
}
