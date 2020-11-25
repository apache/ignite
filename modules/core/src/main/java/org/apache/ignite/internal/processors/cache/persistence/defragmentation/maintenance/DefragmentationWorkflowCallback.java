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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defragmentation specific callback for maintenance mode.
 */
public class DefragmentationWorkflowCallback implements MaintenanceWorkflowCallback {
    /** Defragmentation manager. */
    private final CachePartitionDefragmentationManager defrgMgr;

    /** Logger provider. */
    private final Function<Class<?>, IgniteLogger> logProvider;

    /**
     * @param logProvider Logger provider.
     * @param defrgMgr Defragmentation manager.
     */
    public DefragmentationWorkflowCallback(
        Function<Class<?>, IgniteLogger> logProvider,
        CachePartitionDefragmentationManager defrgMgr
    ) {
        this.defrgMgr = defrgMgr;
        this.logProvider = logProvider;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldProceedWithMaintenance() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public @NotNull List<MaintenanceAction<?>> allActions() {
        return Collections.singletonList(automaticAction());
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceAction<Boolean> automaticAction() {
        return new ExecuteDefragmentationAction(logProvider, defrgMgr);
    }
}
