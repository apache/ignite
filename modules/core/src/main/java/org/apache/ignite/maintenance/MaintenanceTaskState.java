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

/** Maintenance task state for logging. */
public class MaintenanceTaskState {
    /** */
    public enum Status {
        /** {@link MaintenanceTask Maintenance task} registered from storage. */
        REGISTERED("REGISTERED"),

        /** Active {@link MaintenanceTask Maintenance task}. */
        ACTIVE("ACTIVE    "),

        /** Completed {@link MaintenanceTask Maintenance task}. */
        COMPLETE("COMPLETE  "),

        /** Failed {@link MaintenanceTask Maintenance task}. */
        FAILED("FAILED    "),

        /** {@link MaintenanceTask Maintenance task} called manually. */
        CALLED("CALLED    ");

        /** */
        private final String val;

        /** */
        Status(String val) {
            this.val = val;
        }

        /** @return Status value. */
        public String val() {
            return val;
        }
    }

    /** */
    private final MaintenanceTask task;

    /** */
    private volatile Status status;

    /** */
    private volatile String currentAction;

    /**
     * @param task {@link MaintenanceTask Maintenance task}.
     * @param status Status.
     */
    public MaintenanceTaskState(MaintenanceTask task, Status status) {
        this.task = task;
        this.status = status;
    }

    /**
     * @return {@link MaintenanceTask Maintenance task}.
     */
    public MaintenanceTask getTask() {
        return task;
    }

    /**
     * @return Status.
     */
    public Status getStatus() {
        return status;
    }

    /**
     * @param status Status.
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * @return Current action.
     */
    public String getCurrentAction() {
        return currentAction;
    }

    /**
     * @param currentAction Current action.
     * @return Self.
     */
    public MaintenanceTaskState setCurrentAction(String currentAction) {
        this.currentAction = currentAction;
        return this;
    }
}
