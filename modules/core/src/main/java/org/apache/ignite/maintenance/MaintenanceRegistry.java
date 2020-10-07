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
     * @return {@code True} if any maintenance record was found.
     */
    public boolean isMaintenanceMode();

    /**
     * @param rec {@link MaintenanceRecord} object with maintenance information that needs
     *                                     to be stored to maintenance registry.
     *
     * @throws IgniteCheckedException If handling or storing maintenance record failed.
     *
     * @return Previously registered {@link MaintenanceRecord} with the same ID
     * or null if no records were registered for this ID.
     */
    public @Nullable MaintenanceRecord registerMaintenanceRecord(MaintenanceRecord rec) throws IgniteCheckedException;

    /**
     * Deletes {@link MaintenanceRecord} of given ID from maintenance registry.
     *
     * @param mntcId
     */
    public void unregisterMaintenanceRecord(UUID mntcId);

    /**
     * Returns active {@link MaintenanceRecord} by its ID.
     * There are active records only when node entered Maintenance Mode.
     *
     * {@link MaintenanceRecord} becomes active when node enters Maintenance Mode and doesn't resolve the record
     * during maintenance prepare phase.
     *
     * @return {@link MaintenanceRecord} object for given maintenance ID or null if no maintenance record was found.
     */
    @Nullable public MaintenanceRecord activeMaintenanceRecord(UUID maitenanceId);

    /**
     * @param id UUID of {@link MaintenanceRecord} this callback is registered for.
     * @param cb {@link MaintenanceWorkflowCallback} interface used by MaintenanceRegistry to execute
     *                                              maintenance steps by workflow.
     */
    public void registerWorkflowCallback(@NotNull UUID id, @NotNull MaintenanceWorkflowCallback cb);

    /**
     * @param maintenanceId
     * @return
     */
    public List<MaintenanceAction> actionsForMaintenanceRecord(UUID maintenanceId);

    /**
     * Examine all components if they need to execute maintenance actions.
     *
     * As user may resolve some maintenance situations by hand when the node was turned off,
     * component may find out that no maintenance is needed anymore.
     *
     * {@link MaintenanceRecord Maintenance records} for these components are removed
     * and their {@link MaintenanceAction maintenance actions} are not executed.
     */
    public void prepareAndExecuteMaintenance();

    /**
     * Handles all {@link MaintenanceRecord maintenance records} left
     * after {@link MaintenanceRegistry#prepareAndExecuteMaintenance()} check.
     *
     * If a record defines an action that should be started automatically (e.g. defragmentation starts automatically,
     * no additional confirmation from user is required), it is executed.
     *
     * Otherwise waits for user to trigger actions for maintenance records.
     */
    public void proceedWithMaintenance();
}
