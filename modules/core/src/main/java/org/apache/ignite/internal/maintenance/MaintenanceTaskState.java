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
import java.util.concurrent.CopyOnWriteArrayList;
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
    }

    /** */
    private final MaintenanceTask task;

    /** */
    private boolean isRegistered = true;

    /** */
    private final List<ActionLifecycle> executedActions = new CopyOnWriteArrayList<>();

    /** @param task {@link MaintenanceTask Maintenance task}. */
    public MaintenanceTaskState(MaintenanceTask task) {
        this.task = task;
    }

    /** @return {@link MaintenanceTask Maintenance task}. */
    public MaintenanceTask task() {
        return task;
    }

    /** @return {@code true} if the task is registered in the active tasks. */
    public boolean isRegistered() {
        return isRegistered;
    }

    /** Task unregistered from the active tasks. */
    public void unregistered() {
        isRegistered = false;
    }

    /** @return Executed actions lifecycles. */
    public List<ActionLifecycle> executedActionsLifecycles() {
        return new ArrayList<>(executedActions);
    }

    /**
     * Add current action lifecycle.
     * @param lifecycle Current executed action lifecycle.
     */
    public void addActionLifecycle(ActionLifecycle lifecycle) {
        executedActions.add(lifecycle);
    }

    /**
     * Add cuttent action lifecycle if exists.
     * @param actionName Action name.
     */
    public ActionLifecycle getActionLifecycle(String actionName) {
        for (ActionLifecycle lifecycle : executedActions) {
            if (lifecycle.actionName().equals(actionName))
                return lifecycle;
        }

        return new ActionLifecycle(actionName);
    }

    /** */
    public static final class ActionLifecycle {
        /** */
        private final String actionName;

        /** */
        private final List<State> states = new CopyOnWriteArrayList<>();

        /** */
        public ActionLifecycle(String actionName) {
            this.actionName = actionName;
        }

        /** */
        public String actionName() {
            return actionName;
        }

        /** */
        public List<State> states() {
            return Collections.unmodifiableList(states);
        }

        /** */
        public void state(State state) {
            states.add(state);
        }
    }
}
