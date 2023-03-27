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

import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Action which allows to stop the defragmentation at any time from maintenance mode processor.
 */
class StopDefragmentationAction implements MaintenanceAction<Boolean> {
    /** Defragmentation manager. */
    private final CachePartitionDefragmentationManager defragmentationMgr;

    /**
     * @param defragmentationMgr Defragmentation manager.
     */
    public StopDefragmentationAction(CachePartitionDefragmentationManager defragmentationMgr) {
        this.defragmentationMgr = defragmentationMgr;
    }

    /** {@inheritDoc} */
    @Override public Boolean execute() {
        return defragmentationMgr.cancel();
    }

    /** {@inheritDoc} */
    @Override public @NotNull String name() {
        return "stop";
    }

    /** {@inheritDoc} */
    @Override public @Nullable String description() {
        return "Stopping the defragmentation process immediately";
    }
}
