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

package org.apache.ignite.internal.visor.cache.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.management.cache.CacheScheduleIndexesRebuildCommandArg;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.mergeTasks;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.toMaintenanceTask;

/**
 * Task that schedules indexes rebuild for specified caches via the maintenance mode.
 */
@GridInternal
public class ScheduleIndexRebuildTask
    extends VisorMultiNodeTask<CacheScheduleIndexesRebuildCommandArg, ScheduleIndexRebuildTaskRes, ScheduleIndexRebuildJobRes> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected ScheduleIndexRebuildJob job(CacheScheduleIndexesRebuildCommandArg arg) {
        return new ScheduleIndexRebuildJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable ScheduleIndexRebuildTaskRes reduce0(List<ComputeJobResult> results) throws IgniteException {
        Map<UUID, ScheduleIndexRebuildJobRes> taskResultMap = results.stream()
            .collect(Collectors.toMap(res -> res.getNode().id(), ComputeJobResult::getData));

        return new ScheduleIndexRebuildTaskRes(taskResultMap);
    }

    /** Job that schedules index rebuild (via maintenance mode) on a specific node. */
    private static class ScheduleIndexRebuildJob extends VisorJob<CacheScheduleIndexesRebuildCommandArg, ScheduleIndexRebuildJobRes> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected ScheduleIndexRebuildJob(@Nullable CacheScheduleIndexesRebuildCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected ScheduleIndexRebuildJobRes run(@Nullable CacheScheduleIndexesRebuildCommandArg arg) throws IgniteException {
            Set<String> argCacheGroups = arg.groupNames() == null
                ? null
                : new HashSet<>(Arrays.asList(arg.groupNames()));

            assert (arg.cacheToIndexes() != null && !arg.cacheToIndexes().isEmpty())
                || (argCacheGroups != null && !argCacheGroups.isEmpty()) : "Cache to indexes map or cache groups must be specified.";

            Map<String, Set<String>> argCacheToIndexes = arg.cacheToIndexes() != null ? arg.cacheToIndexes() : new HashMap<>();

            Set<String> notFoundCaches = new HashSet<>();
            Set<String> notFoundGroups = new HashSet<>();

            GridCacheProcessor cacheProcessor = ignite.context().cache();

            Map<String, Set<String>> cacheToIndexes = new HashMap<>();
            Map<String, Set<String>> cacheToMissedIndexes = new HashMap<>();

            if (argCacheGroups != null) {
                argCacheGroups.forEach(groupName -> {
                    CacheGroupContext grpCtx = cacheProcessor.cacheGroup(CU.cacheId(groupName));

                    if (grpCtx == null) {
                        notFoundGroups.add(groupName);
                        return;
                    }

                    grpCtx.caches().stream().map(GridCacheContext::name).forEach(cache -> {
                        argCacheToIndexes.put(cache, Collections.emptySet());
                    });
                });
            }

            for (Entry<String, Set<String>> indexesByCache : argCacheToIndexes.entrySet()) {
                String cache = indexesByCache.getKey();
                Set<String> indexesArg = indexesByCache.getValue();
                int cacheId = CU.cacheId(cache);

                GridCacheContext<?, ?> cacheCtx = cacheProcessor.context().cacheContext(cacheId);

                if (cacheCtx == null) {
                    notFoundCaches.add(cache);
                    continue;
                }

                Set<String> existingIndexes = indexes(cache);
                Set<String> indexesToRebuild = cacheToIndexes.computeIfAbsent(cache, s -> new HashSet<>());
                Set<String> missedIndexes = cacheToMissedIndexes.computeIfAbsent(cache, s -> new HashSet<>());

                if (indexesArg.isEmpty())
                    indexesToRebuild.addAll(existingIndexes);
                else {
                    indexesArg.forEach(index -> {
                        if (!existingIndexes.contains(index)) {
                            missedIndexes.add(index);
                            return;
                        }

                        indexesToRebuild.add(index);
                    });
                }
            }

            if (hasAtLeastOneIndex(cacheToIndexes)) {
                MaintenanceRegistry maintenanceRegistry = ignite.context().maintenanceRegistry();

                MaintenanceTask task = toMaintenanceTask(
                    cacheToIndexes.entrySet().stream().collect(Collectors.toMap(
                        (e) -> CU.cacheId(e.getKey()),
                        Entry::getValue
                    ))
                );

                try {
                    maintenanceRegistry.registerMaintenanceTask(
                        task,
                        oldTask -> mergeTasks(oldTask, task)
                    );
                }
                catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }
            }

            return new ScheduleIndexRebuildJobRes(
                cacheToIndexes,
                cacheToMissedIndexes,
                notFoundCaches,
                notFoundGroups
            );
        }

        /**
         * @param cache Cache name.
         * @return Indexes of the cache.
         */
        private Set<String> indexes(String cache) {
            return ignite.context().indexProcessor().treeIndexes(cache, false).stream()
                .map(Index::name)
                .collect(Collectors.toSet());
        }
    }

    /**
     * @param cacheToIndexes Cache name -> indexes map.
     * @return {@code true} if has at least one index in the map, {@code false} otherwise.
     */
    private static boolean hasAtLeastOneIndex(Map<String, Set<String>> cacheToIndexes) {
        return cacheToIndexes.values().stream()
            .anyMatch(indexes -> !indexes.isEmpty());
    }
}
