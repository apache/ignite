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

package org.apache.ignite.internal.management.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.cache.index.ScheduleIndexRebuildTask;
import org.apache.ignite.internal.visor.cache.index.ScheduleIndexRebuildTaskRes;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;

/** Index rebuild via the maintenance mode. */
public class CacheScheduleIndexesRebuildCommand
    implements ComputeCommand<CacheScheduleIndexesRebuildCommandArg, ScheduleIndexRebuildTaskRes> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Schedules rebuild of the indexes for specified caches via the Maintenance Mode. " +
            "Schedules rebuild of specified caches and cache-groups";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheScheduleIndexesRebuildCommandArg> argClass() {
        return CacheScheduleIndexesRebuildCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<ScheduleIndexRebuildTask> taskClass() {
        return ScheduleIndexRebuildTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(
        Map<UUID, T3<Boolean, Object, Long>> nodes,
        CacheScheduleIndexesRebuildCommandArg arg
    ) {
        return arg.nodeId() != null
            ? Collections.singleton(arg.nodeId())
            : nodes.keySet();
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        CacheScheduleIndexesRebuildCommandArg arg,
        ScheduleIndexRebuildTaskRes res0,
        Consumer<String> printer
    ) {
        res0.results().forEach((nodeId, res) -> {
            printMissed(printer, "WARNING: These caches were not found:", res.notFoundCacheNames());
            printMissed(printer, "WARNING: These cache groups were not found:", res.notFoundGroupNames());

            if (!F.isEmpty(res.notFoundIndexes()) && hasAtLeastOneIndex(res.notFoundIndexes())) {
                String warning = "WARNING: These indexes were not found:";

                printer.accept(warning);

                printCachesAndIndexes(res.notFoundIndexes(), printer);
            }

            if (!F.isEmpty(res.cacheToIndexes()) && hasAtLeastOneIndex(res.cacheToIndexes())) {
                printer.accept("Indexes rebuild was scheduled for these caches:");

                printCachesAndIndexes(res.cacheToIndexes(), printer);
            }
            else
                printer.accept("WARNING: Indexes rebuild was not scheduled for any cache. Check command input.");

            printer.accept("");
        });
    }

    /**
     * Prints missed caches' or cache groups' names.
     *
     * @param printer Printer.
     * @param message Message.
     * @param missed Missed caches or cache groups' names.
     */
    private void printMissed(Consumer<String> printer, String message, Set<String> missed) {
        if (F.isEmpty(missed))
            return;

        printer.accept(message);

        missed.stream()
            .sorted()
            .forEach(name -> printer.accept(INDENT + name));

        printer.accept("");
    }

    /**
     * Prints caches and their indexes.
     *
     * @param cachesToIndexes Cache -> indexes map.
     * @param printer Printer.
     */
    private static void printCachesAndIndexes(Map<String, Set<String>> cachesToIndexes, Consumer<String> printer) {
        cachesToIndexes.forEach((cacheName, indexes) -> {
            printer.accept(INDENT + cacheName + ":");
            indexes.forEach(index -> printer.accept(INDENT + INDENT + index));
        });
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
