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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.maintenance.MaintenanceTask;

import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager.DEFRAGMENTATION_MNTC_TASK_NAME;

/**
 * Maintenance task for defragmentation.
 */
public class DefragmentationParameters {
    /** */
    public static final String SEPARATOR = "/";

    /** */
    private final List<String> cacheNames;

    /**
     * @param cacheNames Names of caches for defragmentations.
     */
    private DefragmentationParameters(List<String> cacheNames) {
        this.cacheNames = cacheNames;
    }

    /**
     * Convert parameter to maintenance storage.
     *
     * @param cacheNames Names of caches for defragmentations.
     * @return Maintenance task.
     */
    public static MaintenanceTask toStore(List<String> cacheNames) {
        return new MaintenanceTask(
            DEFRAGMENTATION_MNTC_TASK_NAME,
            "Caches defragmentation",
            cacheNames.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(SEPARATOR))
        );
    }

    /**
     * @param rawTask Task from maintenance storage.
     * @return Defragmentation parameters.
     */
    public static DefragmentationParameters fromStore(MaintenanceTask rawTask) {
        if (rawTask.parameters() == null)
            return new DefragmentationParameters(Collections.emptyList());

        return new DefragmentationParameters(Arrays.stream(rawTask.parameters()
            .split(SEPARATOR))
            .collect(Collectors.toList())
        );
    }

    /**
     * @return Cache names.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public List<String> cacheNames() {
        return cacheNames;
    }
}
