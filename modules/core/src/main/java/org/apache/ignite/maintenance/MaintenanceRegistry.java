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
import java.util.function.UnaryOperator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
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
 *
 * <p>
 *     Implementing new task for maintenance mode requires several pieces of code.
 *
 *     <ul>
 *         <li>
 *             First, component requiring Maintenance Mode should be able to register new {@link MaintenanceTask}
 *             with {@link MaintenanceRegistry#registerMaintenanceTask(MaintenanceTask)} method.
 *
 *             Registration could happen automatically (e.g. if component detects some emergency situation
 *             that requires user intervention)
 *             or by user request (e.g. for a planned maintenance that requires
 *             detaching node from the rest of the cluster).
 *         </li>
 *         <li>
 *             Component responsible for handling this {@link MaintenanceTask}
 *             on startup checks if the task is registered (thus it should go to Maintenance Mode).
 *             If task is found component provides to {@link MaintenanceRegistry} its own implementation
 *             of {@link MaintenanceWorkflowCallback} interface
 *             via method {@link MaintenanceRegistry#registerWorkflowCallback(String, MaintenanceWorkflowCallback)}.
 *         </li>
 *         <li>
 *             {@link MaintenanceWorkflowCallback} should provide {@link MaintenanceRegistry} with
 *             {@link MaintenanceAction}s that are able to resolve maintenance task,
 *             get information about it and so on.
 *             Logic of these actions is completely up to the component providing it
 *             and depends only on particular maintenance task.
 *         </li>
 *         <li>
 *             When maintenance task is fixed, it should be removed from {@link MaintenanceRegistry}
 *             with call {@link MaintenanceRegistry#unregisterMaintenanceTask(String)}.
 *         </li>
 *     </ul>
 * </p>
 */
@IgniteExperimental
public interface MaintenanceRegistry {
    /**
     * @return {@code True} if any maintenance task was found.
     */
    public boolean isMaintenanceMode();

    /**
     * Method to register {@link MaintenanceTask} locally on the node where method is called.
     * <p>
     *     For now it is not allowed to register new Maintenance Tasks in Maintenance Mode
     *     so this method should be called only when node operates normally.
     *     This may change in the future so it will become possible to create other maintenance tasks
     *     on node that is already entered Maintenance Mode.
     * </p>
     * <p>
     *     When task is registered node continues to operate normally
     *     and will enter Maintenance Mode only after restart.
     * </p>
     *
     * @param task {@link MaintenanceTask} object with maintenance information that needs
     *                                     to be stored to maintenance registry.
     *
     * @throws IgniteCheckedException If handling or storing maintenance task failed.
     *
     * @return Previously registered {@link MaintenanceTask} with the same ID
     * or null if no tasks were registered for this ID.
     */
    @Nullable public MaintenanceTask registerMaintenanceTask(MaintenanceTask task) throws IgniteCheckedException;

    /**
     * Method to register {@link MaintenanceTask} locally on the node where method is called. If an old task
     * with the same name exists, applies remapping function to compute a new task.
     * Has the same restrictions as the {@link #registerMaintenanceTask(MaintenanceTask)}.
     *
     * @param task {@link MaintenanceTask} object with maintenance information that needs
     *                                     to be stored to maintenance registry.
     *
     * @param remappingFunction Function to compute a task if an old {@link MaintenanceTask} with the same name exists.
     *
     * @throws IgniteCheckedException If handling or storing maintenance task failed.
     */
    public void registerMaintenanceTask(
        MaintenanceTask task,
        UnaryOperator<MaintenanceTask> remappingFunction
    ) throws IgniteCheckedException;

    /**
     * Deletes {@link MaintenanceTask} of given ID from maintenance registry.
     *
     * @param maintenanceTaskName name of {@link MaintenanceTask} to be deleted.
     * @return {@code true} if existing task has been deleted.
     */
    public boolean unregisterMaintenanceTask(String maintenanceTaskName);

    /**
     * Returns active {@link MaintenanceTask} by its name.
     * There are active tasks only when node entered Maintenance Mode.
     *
     * {@link MaintenanceTask} becomes active when node enters Maintenance Mode and doesn't resolve the task
     * during maintenance prepare phase.
     *
     * @param maintenanceTaskName Maintenance Task name.
     *
     * @return {@link MaintenanceTask} object for given name or null if no maintenance task was found.
     */
    @Nullable public MaintenanceTask activeMaintenanceTask(String maintenanceTaskName);

    /**
     * Registers {@link MaintenanceWorkflowCallback} for a {@link MaintenanceTask} with a given name.
     *
     * Component registered {@link MaintenanceTask} automatically or by user request
     * is responsible for providing {@link MaintenanceRegistry} with an implementation of
     * {@link MaintenanceWorkflowCallback} where registry obtains {@link MaintenanceAction}s
     * to be executed for this task and does a preliminary check before starting maintenance.
     *
     * @param maintenanceTaskName name of {@link MaintenanceTask} this callback is registered for.
     * @param cb {@link MaintenanceWorkflowCallback} interface used by MaintenanceRegistry to execute
     *                                              maintenance steps by workflow.
     */
    public void registerWorkflowCallback(@NotNull String maintenanceTaskName, @NotNull MaintenanceWorkflowCallback cb);

    /**
     * All {@link MaintenanceAction}s provided by a component for {@link MaintenanceTask} with a given name.
     *
     * @param maintenanceTaskName name of Maintenance Task.
     * @return {@link List} of all available {@link MaintenanceAction}s for given Maintenance Task.
     *
     * @throws IgniteException if no Maintenance Tasks are registered for provided name.
     */
    public List<MaintenanceAction<?>> actionsForMaintenanceTask(String maintenanceTaskName);

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
     * Call the {@link #registerWorkflowCallback(String, MaintenanceWorkflowCallback)} if the active maintenance task
     * with given name exists.
     *
     * @param maintenanceTaskName name of {@link MaintenanceTask} this callback is registered for.
     * @param workflowCalProvider provider of {@link MaintenanceWorkflowCallback} which construct the callback by given
     * task.
     */
    public default void registerWorkflowCallbackIfTaskExists(
        @NotNull String maintenanceTaskName,
        @NotNull IgniteThrowableFunction<MaintenanceTask, MaintenanceWorkflowCallback> workflowCalProvider
    ) throws IgniteCheckedException {
        MaintenanceTask task = activeMaintenanceTask(maintenanceTaskName);

        if (task != null)
            registerWorkflowCallback(maintenanceTaskName, workflowCalProvider.apply(task));
    }

    /**
     * @param maintenanceTaskName Task's name.
     * @return Requested maintenance task or {@code null}.
     */
    @Nullable public MaintenanceTask requestedTask(String maintenanceTaskName);
}
