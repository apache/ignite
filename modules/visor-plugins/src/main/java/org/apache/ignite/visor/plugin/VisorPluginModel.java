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

package org.apache.ignite.visor.plugin;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Gateway to Visor functionality from plugins.
 * Plugins will receive model instance from Visor, usually passed
 * into constructor, e.g. {@link VisorExtensionPoint#VisorExtensionPoint(VisorPluginModel)}.
 */
public interface VisorPluginModel {
    /**
     * Whether Visor connected to the grid.
     *
     * @return {@code true} if Visor connected to the grid.
     */
    public boolean connected();

    /**
     * Get logger.
     *
     * @return Logger.
     */
    public IgniteLogger logger();

    /**
     * Install topology listener.
     *
     * @param lsnr Listener to add.
     * @throws IllegalStateException If Visor not connected to the grid.
     */
    public void addTopologyListener(VisorTopologyListener lsnr) throws IllegalStateException;

    /**
     * Uninstall topology listener.
     *
     * @param lsnr Listener to remove.
     * @throws IllegalStateException If Visor not connected to the grid.
     */
    public void removeTopologyListener(VisorTopologyListener lsnr) throws IllegalStateException;

    /**
     * @return Collection of all nodes IDs.
     */
    public Collection<UUID> nodeIds();

    /**
     * Executes given task on this grid projection. For step-by-step explanation of task execution process
     * refer to {@link org.apache.ignite.compute.ComputeTask} documentation.
     *
     * @param taskCls Class of the task to execute. If class has {@link org.apache.ignite.compute.ComputeTaskName} annotation,
     *      then task is deployed under a name specified within annotation. Otherwise, full
     *      class name is used as task name.
     * @param nodeIds Node IDs on with execute task.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @param <A> Argument type.
     * @param <R> Result type.
     * @return Task result.
     * @throws IllegalStateException If Visor not connected to the grid.
     */
    public <A, R> R execute(Class<? extends ComputeTask<A, R>> taskCls, Collection<UUID> nodeIds, @Nullable A arg)
        throws IllegalStateException;

    /**
     * Executes given task on this grid projection. For step-by-step explanation of task execution process
     * refer to {@link org.apache.ignite.compute.ComputeTask} documentation.
     *
     * @param taskName Name of the task to execute.
     * @param nodeIds Node IDs on with execute task.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @param <A> Argument type.
     * @param <R> Result type.
     * @return Task result.
     * @throws IllegalStateException If Visor not connected to the grid.
     */
    public <A, R> R execute(String taskName, Collection<UUID> nodeIds, @Nullable A arg) throws IllegalStateException;
}