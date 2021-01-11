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
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstraction to decouple interaction between {@link MaintenanceRegistry}
 * and components that may require maintenance.
 *
 * If a component may cause node to enter maintenance mode, it should register this callback
 * in {@link MaintenanceRegistry} using method {@link MaintenanceRegistry#registerWorkflowCallback(String, MaintenanceWorkflowCallback)}
 *
 * {@link MaintenanceRegistry} during its workflow will collect necessary information about maintenance for components
 * without knowing implementation details of the components.
 */
@IgniteExperimental
public interface MaintenanceWorkflowCallback {
    /**
     * Called by {@link MaintenanceRegistry} and enables it to check if maintenance is still needed
     * for component that provided this callback.
     *
     * User may fix maintenance situation by hand when node was down thus before going to maintenance mode
     * we should be able to check if it is still necessary.
     *
     * @return {@code True} if maintenance is still needed for the component.
     */
    public boolean shouldProceedWithMaintenance();

    /**
     * Supplies list of {@link MaintenanceAction}s that user can call to fix maintenance situation for the component or
     * get information about ongoing actions. Should not be null or empty.
     *
     * @return Not null and non-empty {@link List} of {@link MaintenanceAction}.
     */
    @NotNull public List<MaintenanceAction<?>> allActions();

    /**
     * Component can provide optional {@link MaintenanceAction} that will be executed automatically
     * by {@link MaintenanceRegistry} when node enters maintenance mode.
     *
     * If no automatic actions are provided {@link MaintenanceRegistry} will wait for user
     * to trigger {@link MaintenanceAction} with logic to fix the maintenance situation.
     *
     * @return {@link MaintenanceAction} for automatic execution or null if maintenance situation
     * should not be fixed automatically.
     */
    @Nullable public MaintenanceAction<?> automaticAction();
}
