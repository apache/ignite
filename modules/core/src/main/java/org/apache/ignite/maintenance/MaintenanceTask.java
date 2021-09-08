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
 * Represents request to handle maintenance situation.
 *
 * Maintenance request can be created programmatically
 * with {@link MaintenanceRegistry#registerMaintenanceTask(MaintenanceTask)} public API call.
 *
 * Lifecycle of Maintenance Task is managed by {@link MaintenanceRegistry}.
 *
 * Task contains unique ID of maintenance situation (e.g. situation of PDS corruption or defragmentation),
 * description of task and optional parameters.
 *
 * When task is created node should be restarted to enter maintenance mode.
 * In that mode node can start actions needed to resolve maintenance situation or wait for user to trigger them.
 *
 * Components that may need to perform maintenance actions as part of their recovery workflow should check
 * maintenance status on startup and supply {@link MaintenanceWorkflowCallback} implementation to
 * {@link MaintenanceRegistry#registerWorkflowCallback(String, MaintenanceWorkflowCallback)} to allow Maintenance Registry
 * to find maintenance actions and start them automatically or by user request.
 *
 * Matching between {@link MaintenanceTask} and {@link MaintenanceWorkflowCallback} is performed based on
 * the name of maintenance task that should be unique among all registered tasks.
 */
@IgniteExperimental
public class MaintenanceTask {
    /** */
    private final String name;

    /** */
    private final String description;

    /** */
    private final String params;

    /**
     * @param name Mandatory name of maintenance task. Name should be unique among all other tasks.
     * @param description Mandatory description of maintenance situation.
     * @param params Optional parameters that may be needed to perform maintenance actions.
     */
    public MaintenanceTask(@NotNull String name, @NotNull String description, @Nullable String params) {
        this.name = name;
        this.description = description;
        this.params = params;
    }

    /**
     * @return Name of Maintenance Task unique among all registered tasks.
     */
    public @NotNull String name() {
        return name;
    }

    /**
     * @return Human-readable not-nullable description of the task.
     */
    public @NotNull String description() {
        return description;
    }

    /**
     * @return Optional parameters that could be used by actions associated with this Maintenance Task.
     */
    public @Nullable String parameters() {
        return params;
    }
}
