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

package org.apache.ignite.internal.managers.discovery;

import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

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

    /**
     * Gets topology version if this event is raised on
     * topology change and configured discovery SPI implementation
     * supports topology versioning.
     *
     * @return Topology version or {@code 0} if configured discovery SPI implementation
     *      does not support versioning.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * Gets topology nodes from topology snapshot. If SPI implementation does not support
     * versioning, the best effort snapshot will be captured.
     *
     * @return Topology snapshot.
     */
    public Collection<ClusterNode> topologyNodes() {
        return topNodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDiscoveryTopologySnapshot.class, this);
    }
}