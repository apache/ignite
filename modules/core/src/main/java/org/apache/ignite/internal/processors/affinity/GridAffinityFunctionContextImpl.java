/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.affinity;

import java.util.List;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cache affinity function context implementation. Simple bean that holds all required fields.
 */
public class GridAffinityFunctionContextImpl implements AffinityFunctionContext {
    /** Topology snapshot. */
    private final List<ClusterNode> topSnapshot;

    /** Previous affinity assignment. */
    private final List<List<ClusterNode>> prevAssignment;

    /** Discovery event that caused this topology change. */
    private final DiscoveryEvent discoEvt;

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Number of backups to assign. */
    private final int backups;

    /**
     * @param topSnapshot Topology snapshot.
     * @param topVer Topology version.
     */
    public GridAffinityFunctionContextImpl(List<ClusterNode> topSnapshot, List<List<ClusterNode>> prevAssignment,
        DiscoveryEvent discoEvt, @NotNull AffinityTopologyVersion topVer, int backups) {
        this.topSnapshot = topSnapshot;
        this.prevAssignment = prevAssignment;
        this.discoEvt = discoEvt;
        this.topVer = topVer;
        this.backups = backups;
    }

    /** {@inheritDoc} */
    @Nullable @Override public List<ClusterNode> previousAssignment(int part) {
        return prevAssignment == null ? null : prevAssignment.get(part);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> currentTopologySnapshot() {
        return topSnapshot;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion currentTopologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryEvent discoveryEvent() {
        return discoEvt;
    }

    /** {@inheritDoc} */
    @Override public int backups() {
        return backups;
    }

    /**
     * Gets the previous assignment.
     *
     * @return Previous assignment
     */
    public List<List<ClusterNode>> prevAssignment() {
        return prevAssignment;
    }
}