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

package org.apache.ignite.configuration;

import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.spi.communication.CommunicationSpi;

/**
 * Communication Failure Context.
 */
public interface CommunicationFailureContext {
    /**
     * @return Current topology snapshot.
     */
    public List<ClusterNode> topologySnapshot();

    /**
     * @param node1 First node.
     * @param node2 Second node.
     * @return {@code True} if {@link CommunicationSpi} is able to establish connection from first node to second node.
     */
    public boolean connectionAvailable(ClusterNode node1, ClusterNode node2);

    /**
     * @return Currently started caches.
     */
    public Map<String, CacheConfiguration<?, ?>> startedCaches();

    /**
     * @param cacheName Cache name.
     * @return Cache partitions affinity assignment.
     */
    public List<List<ClusterNode>> cacheAffinity(String cacheName);

    /**
     * @param cacheName Cache name.
     * @return Cache partitions owners.
     */
    public List<List<ClusterNode>> cachePartitionOwners(String cacheName);

    /**
     * @param node Node to kill.
     */
    public void killNode(ClusterNode node);
}
