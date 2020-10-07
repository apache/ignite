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

import java.util.UUID;

import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents request to handle maintenance situation stored on disk.
 *
 * Maintenance request can be created programmatically
 * with {@link MaintenanceRegistry#registerMaintenanceRecord(MaintenanceRecord)} public API call.
 *
 * Record contains unique ID of maintenance situation (e.g. situation of PDS corruption or defragmentation),
 * description of record and optional parameters.
 *
 * When record is created node should be restarted to enter maintenance mode.
 * In that mode node can start actions needed to resolve maintenance situation or wait for user to trigger them.
 *
 * Components that may need to perform maintenance actions as part of their recovery workflow should check
 * maintenance status on startup and supply {@link MaintenanceWorkflowCallback} implementation to
 * {@link MaintenanceRegistry#registerWorkflowCallback(UUID, MaintenanceWorkflowCallback)} to allow Maintenance Registry
 * to find maintenance actions and start them automatically or by user request.
 *
 * Matching between {@link MaintenanceRecord} and {@link MaintenanceWorkflowCallback} is performed based on
 * unique ID of maintenance situation.
 */
@IgniteExperimental
public class MaintenanceRecord {
    /** */
    private final UUID id;

    /** */
    private final String description;

    /** */
    private final String params;

    /**
     * @param id Mandatory unique ID of maintenance record.
     * @param description Mandatory description of maintenance situation.
     * @param params Optional parameters that may be needed to perform maintenance actions.
     */
    public MaintenanceRecord(UUID id, String description, String params) {
        this.id = id;
        this.description = description;
        this.params = params;
    }

    /** */
    public @NotNull UUID id() {
        return id;
    }

    /** */
    public @NotNull String description() {
        return description;
    }

    /** */
    public @Nullable String parameters() {
        return params;
    }
}
