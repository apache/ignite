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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** */
public class MaintenanceProcessor extends GridProcessorAdapter implements MaintenanceRegistry {
    /** */
    private static final String DISABLED_ERR_MSG = "Maintenance Mode is not supported for in-memory and client nodes";

    /**
     * Active {@link MaintenanceTask}s are the ones that were read from disk when node entered Maintenance Mode.
     */
    private final Map<String, MaintenanceTask> activeTasks = new ConcurrentHashMap<>();

    /**
     * Requested {@link MaintenanceTask}s are collection of tasks requested by user
     * or other components when node operates normally (not in Maintenance Mode).
     */
    private final Map<String, MaintenanceTask> requestedTasks = new ConcurrentHashMap<>();

    /** */
    private final Map<String, MaintenanceWorkflowCallback> workflowCallbacks = new ConcurrentHashMap<>();

    /** */
    private final MaintenanceFileStore fileStorage;

    /** */
    private final boolean disabled;

    /** */
    private volatile boolean maintenanceMode;

    /**
     * @param ctx Kernal context.
     */
    public MaintenanceProcessor(GridKernalContext ctx) {
        super(ctx);

        disabled = !CU.isPersistenceEnabled(ctx.config()) || ctx.clientNode();

        if (disabled) {
            fileStorage = new MaintenanceFileStore(true,
                null,
                null,
                null);

            return;
        }

        fileStorage = new MaintenanceFileStore(false,
            ctx.pdsFolderResolver(),
            ctx.config().getDataStorageConfiguration().getFileIOFactory(),
            log);
    }

    /** {@inheritDoc} */
    @Nullable @Override public MaintenanceTask registerMaintenanceTask(MaintenanceTask task) throws IgniteCheckedException {
        return registerMaintenanceTask0(task, null);
    }

    /** {@inheritDoc} */
    @Override public void registerMaintenanceTask(
        MaintenanceTask task,
        UnaryOperator<MaintenanceTask> remappingFunction
    ) throws IgniteCheckedException {
        assert remappingFunction != null;

        registerMaintenanceTask0(task, remappingFunction);
    }

    /**
     * Registers maintenance task and applies remapping function to a previously registered task with the same name
     * if remapping function is not {@code null}.
     *
     * @param task Maintenance task.
     * @param remappingFunction Remapping function.
     * @return Previously registered task or {@code null} if there was none or if remapping function is not {@code null}.
     * @throws IgniteCheckedException If failed to register maintenance task.
     */
    @Nullable private MaintenanceTask registerMaintenanceTask0(
        MaintenanceTask task,
        @Nullable UnaryOperator<MaintenanceTask> remappingFunction
    ) throws IgniteCheckedException {
        if (disabled)
            throw new IgniteCheckedException(DISABLED_ERR_MSG);

        if (isMaintenanceMode()) {
            throw new IgniteCheckedException("Node is already in Maintenance Mode, " +
                "registering additional maintenance task is not allowed in Maintenance Mode.");
        }

        MaintenanceTask taskToWrite;
        MaintenanceTask oldTask;

        if (remappingFunction != null) {
            taskToWrite = requestedTasks.compute(task.name(), (taskName, prevTask) -> {
                if (prevTask != null)
                    return remappingFunction.apply(prevTask);

                return task;
            });

            oldTask = null;
        }
        else {
            taskToWrite = task;

            oldTask = requestedTasks.put(task.name(), task);

            if (oldTask != null) {
                log.info(
                    "Maintenance Task with name " + task.name() +
                        " is already registered" +
                        (oldTask.parameters() != null ? " with parameters " + oldTask.parameters() : ".") +
                        " It will be replaced with new task" +
                        task.parameters() != null ? " with parameters " + task.parameters() : "" + "."
                );
            }
        }

        try {
            fileStorage.writeMaintenanceTask(taskToWrite);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to register maintenance task " + task, e);
        }

        return oldTask;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        try {
            fileStorage.stop();
        }
        catch (IOException e) {
            log.warning("Failed to free maintenance file resources", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (disabled)
            return;

        try {
            fileStorage.init();

            activeTasks.putAll(fileStorage.getAllTasks());

            maintenanceMode = !activeTasks.isEmpty();
        }
        catch (Throwable t) {
            log.warning("Caught exception when starting MaintenanceProcessor," +
                " maintenance mode won't be entered", t);

            activeTasks.clear();

            fileStorage.clear();
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareAndExecuteMaintenance() {
        if (isMaintenanceMode()) {
            workflowCallbacks.entrySet().removeIf(cbE -> {
                    if (!cbE.getValue().shouldProceedWithMaintenance()) {
                        unregisterMaintenanceTask(cbE.getKey());

                        return true;
                    }

                    return false;
                }
            );
        }

        if (!workflowCallbacks.isEmpty()) {
            if (log.isInfoEnabled()) {
                String mntcTasksNames = String.join(", ", workflowCallbacks.keySet());

                log.info("Node requires maintenance, non-empty set of maintenance tasks is found: [" +
                    mntcTasksNames + ']');
            }

            proceedWithMaintenance();
        }
        else if (isMaintenanceMode()) {
            if (log.isInfoEnabled()) {
                log.info("All maintenance tasks are fixed, no need to enter maintenance mode. " +
                    "Restart the node to get it back to normal operations.");
            }
        }
    }

    /**
     * Handles all {@link MaintenanceTask maintenance tasks} left
     * after {@link MaintenanceRegistry#prepareAndExecuteMaintenance()} check.
     *
     * If a task defines an action that should be started automatically (e.g. defragmentation starts automatically,
     * no additional confirmation from user is required), it is executed.
     *
     * Otherwise waits for user to trigger actions for maintenance tasks.
     */
    private void proceedWithMaintenance() {
        for (Map.Entry<String, MaintenanceWorkflowCallback> cbE : workflowCallbacks.entrySet()) {
            MaintenanceAction<?> mntcAct = cbE.getValue().automaticAction();

            if (mntcAct != null) {
                try {
                    mntcAct.execute();
                }
                catch (Throwable t) {
                    log.warning("Failed to execute automatic action for maintenance task: " +
                        activeTasks.get(cbE.getKey()), t);

                    throw t;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceTask activeMaintenanceTask(String maitenanceTaskName) {
        return activeTasks.get(maitenanceTaskName);
    }

    /** {@inheritDoc} */
    @Override public boolean isMaintenanceMode() {
        return maintenanceMode;
    }

    /** {@inheritDoc} */
    @Override public boolean unregisterMaintenanceTask(String maintenanceTaskName) {
        if (disabled)
            return false;

        boolean deleted;

        if (isMaintenanceMode())
            deleted = activeTasks.remove(maintenanceTaskName) != null;
        else
            deleted = requestedTasks.remove(maintenanceTaskName) != null;

        try {
            fileStorage.deleteMaintenanceTask(maintenanceTaskName);
        }
        catch (IOException e) {
            log.warning("Failed to clear maintenance task with name "
                + maintenanceTaskName
                + " from file, whole file will be deleted", e
            );

            fileStorage.clear();
        }

        return deleted;
    }

    /** {@inheritDoc} */
    @Override public void registerWorkflowCallback(@NotNull String maintenanceTaskName, @NotNull MaintenanceWorkflowCallback cb) {
        if (disabled)
            throw new IgniteException(DISABLED_ERR_MSG);

        List<MaintenanceAction<?>> actions = cb.allActions();

        if (actions == null || actions.isEmpty())
            throw new IgniteException("Maintenance workflow callback should provide at least one maintenance action");

        int size = actions.size();
        long distinctSize = actions.stream().map(MaintenanceAction::name).distinct().count();

        if (distinctSize < size)
            throw new IgniteException("All actions of a single workflow should have unique names: " +
                actions.stream().map(MaintenanceAction::name).collect(Collectors.joining(", ")));

        Optional<String> wrongActionName = actions
            .stream()
            .map(MaintenanceAction::name)
            .filter(name -> !U.alphanumericUnderscore(name))
            .findFirst();

        if (wrongActionName.isPresent())
            throw new IgniteException(
                "All actions' names should contain only alphanumeric and underscore symbols: "
                    + wrongActionName.get());

        workflowCallbacks.put(maintenanceTaskName, cb);
    }

    /** {@inheritDoc} */
    @Override public List<MaintenanceAction<?>> actionsForMaintenanceTask(String maintenanceTaskName) {
        if (disabled)
            throw new IgniteException(DISABLED_ERR_MSG);

        if (!activeTasks.containsKey(maintenanceTaskName))
            throw new IgniteException("Maintenance workflow callback for given task name not found, " +
                "cannot retrieve maintenance actions for it: " + maintenanceTaskName);

        return workflowCallbacks.get(maintenanceTaskName).allActions();
    }

    /** {@inheritDoc} */
    @Nullable @Override public MaintenanceTask requestedTask(String maintenanceTaskName) {
        return requestedTasks.get(maintenanceTaskName);
    }
}
