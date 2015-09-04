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