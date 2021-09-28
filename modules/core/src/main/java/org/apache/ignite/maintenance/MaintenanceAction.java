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

import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Maintenance action interface enables user to execute operations related to a particular {@link MaintenanceTask}.
 *
 * These operations may resolve maintenance situation (e.g. remove corrupted data files), get information
 * about other ongoing maintenance action (e.g. if some action requires a lot of time and user wants to know
 * current progress of the action) or cancel other ongoing action.
 *
 * List of maintenance actions available for each task is defined by {@link MaintenanceWorkflowCallback}.
 *
 * {@link MaintenanceRegistry} provides an access to maintenance actions for a {@link MaintenanceTask} with
 * call {@link MaintenanceRegistry#actionsForMaintenanceTask(String)}
 *
 */
@IgniteExperimental
public interface MaintenanceAction<T> {
    /**
     * Executes operations of current maintenance action.
     *
     * @return Result of the maintenance action.
     */
    public T execute();

    /**
     * @return Mandatory human-readable name of maintenance action.
     * All actions of single {@link MaintenanceWorkflowCallback} should have unique names.
     */
    @NotNull public String name();

    /**
     * @return Optional user-readable description of maintenance action.
     */
    @Nullable public String description();
}
