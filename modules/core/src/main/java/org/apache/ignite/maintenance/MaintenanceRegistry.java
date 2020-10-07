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

package org.apache.ignite.maintenance;

import java.util.List;
import java.util.UUID;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MaintenanceRegistry} is a service local to each Ignite node
 * that allows to request performing maintenance actions on that particular node.
 *
 * <p>
 *     When a node gets into a situation when some specific actions are required
 *     it enters the special mode called maintenance mode.
 *     In maintenance mode it doesn't join to the rest of the cluster but still allows to connect to it
 *     with control.{sh|bat} script or via JXM interface and perform needed actions.
 * </p>
 */
@IgniteExperimental
public interface MaintenanceRegistry {
    /**
     * @return {@code True} if any maintenance task was found.
     */
    public boolean isMaintenanceMode();

    /**
     * @param task {@link MaintenanceTask} object with maintenance information that needs
     *                                     to be stored to maintenance registry.
     *
     * @throws IgniteCheckedException If handling or storing maintenance task failed.
     *
     * @return Previously registered {@link MaintenanceTask} with the same ID
     * or null if no tasks were registered for this ID.
     */
    public @Nullable MaintenanceTask registerMaintenanceTask(MaintenanceTask task) throws IgniteCheckedException;

    /**
     * Deletes {@link MaintenanceTask} of given ID from maintenance registry.
     *
     * @param mntcId
     */
    public void unregisterMaintenanceTask(UUID mntcId);

    /**
     * Returns active {@link MaintenanceTask} by its ID.
     * There are active tasks only when node entered Maintenance Mode.
     *
     * {@link MaintenanceTask} becomes active when node enters Maintenance Mode and doesn't resolve the task
     * during maintenance prepare phase.
     *
     * @return {@link MaintenanceTask} object for given maintenance ID or null if no maintenance task was found.
     */
    @Nullable public MaintenanceTask activeMaintenanceTask(UUID maitenanceId);

    /**
     * @param id UUID of {@link MaintenanceTask} this callback is registered for.
     * @param cb {@link MaintenanceWorkflowCallback} interface used by MaintenanceRegistry to execute
     *                                              maintenance steps by workflow.
     */
    public void registerWorkflowCallback(@NotNull UUID id, @NotNull MaintenanceWorkflowCallback cb);

    /**
     * @param maintenanceId
     * @return
     */
    public List<MaintenanceAction> actionsForMaintenanceTask(UUID maintenanceId);

    /**
     * Examine all components if they need to execute maintenance actions.
     *
     * As user may resolve some maintenance situations by hand when the node was turned off,
     * component may find out that no maintenance is needed anymore.
     *
     * {@link MaintenanceTask Maintenance tasks} for these components are removed
     * and their {@link MaintenanceAction maintenance actions} are not executed.
     */
    public void prepareAndExecuteMaintenance();

    /**
     * Handles all {@link MaintenanceTask maintenance tasks} left
     * after {@link MaintenanceRegistry#prepareAndExecuteMaintenance()} check.
     *
     * If a task defines an action that should be started automatically (e.g. defragmentation starts automatically,
     * no additional confirmation from user is required), it is executed.
     *
     * Otherwise waits for user to trigger actions for maintenance tasks.
     */
    public void proceedWithMaintenance();
}
