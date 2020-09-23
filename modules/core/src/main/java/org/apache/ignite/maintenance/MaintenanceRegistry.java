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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

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
     *
     * @return {@code True}
     */
    public boolean prepareMaintenance();

    /**
     *
     */
    public void proceedWithMaintenance();

    /**
     * @param rec {@link MaintenanceRecord} object with maintenance information that needs
     *                                     to be stored to maintenance registry.
     *
     * @throws IgniteCheckedException If handling or storing maintenance record failed.
     */
    public void registerMaintenanceRecord(MaintenanceRecord rec) throws IgniteCheckedException;

    /**
     * @return {@link MaintenanceRecord} object for given maintenance ID or null if no maintenance record was found.
     */
    @Nullable public MaintenanceRecord maintenanceRecord(UUID maitenanceId);

    /**
     * @return {@code True} if any maintenance record was found.
     */
    public boolean isMaintenanceMode();

    /**
     * Deletes {@link MaintenanceRecord} of given ID from maintenance registry.
     *
     * @param mntcId
     */
    public void clearMaintenanceRecord(UUID mntcId);

    /**
     * Registers {@link MaintenanceAction} object with information from corresponding Maintenance record.
     *
     * @param mntcId The ID of {@link MaintenanceRecord} action is registered for.
     * @param action {@link MaintenanceAction} object with executable logic to address given maintenance reason.
     */
    public void registerMaintenanceAction(@NotNull UUID mntcId, @NotNull MaintenanceAction action);

    /**
     * Gets {@link MaintenanceAction} object for given maintenance record ID.
     *
     * @param mntcId ID of {@link MaintenanceRecord}.
     * @return {@link MaintenanceAction} registered for given ID or null of no actions were registered.
     */
    @Nullable public MaintenanceAction maintenanceAction(UUID mntcId);
}
