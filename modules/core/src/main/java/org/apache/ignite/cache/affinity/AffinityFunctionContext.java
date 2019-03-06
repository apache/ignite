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

package org.apache.ignite.cache.affinity;

import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Affinity function context. This context is passed to {@link AffinityFunction} for
 * partition reassignment on every topology change event.
 */
public interface AffinityFunctionContext {
    /**
     * Gets affinity assignment for given partition on previous topology version. First node in returned list is
     * a primary node, other nodes are backups.
     *
     * @param part Partition to get previous assignment for.
     * @return List of nodes assigned to given partition on previous topology version or {@code null}
     *      if this information is not available.
     */
    @Nullable public List<ClusterNode> previousAssignment(int part);

    /**
     * Gets number of backups for new assignment.
     *
     * @return Number of backups for new assignment.
     */
    public int backups();

    /**
     * Gets current topology snapshot. Snapshot will contain only nodes on which particular cache is configured.
     * List of passed nodes is guaranteed to be sorted in a same order on all nodes on which partition assignment
     * is performed.
     *
     * @return Cache topology snapshot.
     */
    public List<ClusterNode> currentTopologySnapshot();

    /**
     * Gets current topology version number.
     *
     * @return Current topology version number.
     */
    public AffinityTopologyVersion currentTopologyVersion();

    /**
     * Gets discovery event caused topology change.
     *
     * @return Discovery event caused latest topology change or {@code null} if this information is
     *      not available.
     */
    @Nullable public DiscoveryEvent discoveryEvent();
}