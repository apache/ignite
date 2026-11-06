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

package org.apache.ignite.internal.maintenance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.maintenance.MaintenanceTask;

/** Maintenance task state for logging. */
public class MaintenanceTaskState {
    /** */
    public enum State {
        /** Active {@link MaintenanceTask Maintenance task}. */
        ACTIVE,

        /** {@link MaintenanceTask Maintenance task} cancelled. */
        CANCELLED,

        /** Completed {@link MaintenanceTask Maintenance task}. */
        COMPLETED,

        /** Failed {@link MaintenanceTask Maintenance task}. */
        FAILED,

        /**
         * {@link MaintenanceTask Maintenance task}
         * registered throw {@link MaintenanceProcessor#registerWorkflowCallback},
         * not registered from storage.
         */
        NOT_REGISTERED,

        /** {@link MaintenanceTask Maintenance task} registered from storage. */
        REGISTERED,

        /** {@link MaintenanceTask Maintenance task} unregistered. */
        UNREGISTERED;
    }

    /** */
    private final MaintenanceTask task;

    /** */
    private final List<State> state = new ArrayList<>();

    /** */
    private volatile String currAction;

    /**
     * @param task {@link MaintenanceTask Maintenance task}.
     * @param state State.
     */
    public MaintenanceTaskState(MaintenanceTask task, State state) {
        this.task = task;
        this.state.add(state);
    }

    /** @return {@link MaintenanceTask Maintenance task}. */
    public MaintenanceTask task() {
        return task;
    }

    /** @return State. */
    public List<State> state() {
        return Collections.unmodifiableList(state);
    }

    /** @param state State. */
    public void state(State state) {
        this.state.add(state);
    }

    /** @return Current action. */
    public String currAction() {
        return currAction;
    }

    /**
     * @param currAction Current action.
     * @return Self.
     */
    public MaintenanceTaskState currAction(String currAction) {
        this.currAction = currAction;
        return this;
    }
}
