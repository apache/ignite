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

package org.apache.ignite.internal.cache.query.index.sorted.maintenance;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.Nullable;

/**
 * Workflow for the index rebuild maintenance task.
 */
public class RebuildIndexWorkflowCallback implements MaintenanceWorkflowCallback {
    /** Indexes to rebuild. */
    private final List<MaintenanceRebuildIndexTarget> indexesToRebuild;

    /** Context. */
    private final GridKernalContext ctx;

    /**
     * Constructor.
     *
     * @param indexesToRebuild Indexes to rebuild.
     * @param ctx Context.
     */
    public RebuildIndexWorkflowCallback(List<MaintenanceRebuildIndexTarget> indexesToRebuild, GridKernalContext ctx) {
        this.indexesToRebuild = indexesToRebuild;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldProceedWithMaintenance() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public List<MaintenanceAction<?>> allActions() {
        return Collections.singletonList(new RebuildIndexAction(indexesToRebuild, ctx));
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceAction<?> automaticAction() {
        return new RebuildIndexAction(indexesToRebuild, ctx);
    }
}
