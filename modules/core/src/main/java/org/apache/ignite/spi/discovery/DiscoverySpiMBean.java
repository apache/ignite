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
package org.apache.ignite.spi.discovery;

import java.util.UUID;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.jetbrains.annotations.Nullable;

/**
 * Generic MBean interface to monitor DiscoverySpi subsystem.
 */
public interface DiscoverySpiMBean {
    /**
     * Gets current SPI state.
     *
     * @return Current SPI state.
     */
    @MXBeanDescription("SPI state.")
    public String getSpiState();

    /**
     * Gets joined nodes count.
     *
     * @return Nodes joined count.
     */
    @MXBeanDescription("Nodes joined count.")
    public long getNodesJoined();

    /**
     * Gets failed nodes count.
     *
     * @return Failed nodes count.
     */
    @MXBeanDescription("Nodes failed count.")
    public long getNodesFailed();

    /**
     * Gets left nodes count.
     *
     * @return Left nodes count.
     */
    @MXBeanDescription("Nodes left count.")
    public long getNodesLeft();

    /**
     * Gets current coordinator.
     *
     * @return Gets current coordinator.
     * @deprecated Use {@link #getCoordinatorNodeFormatted()} instead.
     */
    @Deprecated
    @MXBeanDescription("Coordinator node ID.")
    @Nullable public UUID getCoordinator();

    /**
     * Gets current coordinator node formatted as a string.
     *
     * @return Current coordinator string representation.
     */
    @MXBeanDescription("Coordinator node formatted as a string.")
    @Nullable public String getCoordinatorNodeFormatted();

    /**
     * Gets local node formatted as a string.
     *
     * @return Local node string representation.
     */
    @MXBeanDescription("Local node formatted as a string.")
    public String getLocalNodeFormatted();

     /**
      * Exclude node from discovery.
      *
      * @param nodeId Node id string.
      */
     @MXBeanDescription("Exclude node from cluster.")
     public void excludeNode(String nodeId);
}
