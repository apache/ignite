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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.nodeOrAll;
import static org.apache.ignite.internal.management.cache.CacheIndexesForceRebuildCommand.PREF_CACHES_NOT_FOUND;
import static org.apache.ignite.internal.management.cache.CacheIndexesForceRebuildCommand.PREF_GROUPS_NOT_FOUND;
import static org.apache.ignite.internal.management.cache.CacheIndexesForceRebuildCommand.PREF_SCHEDULED;
import static org.apache.ignite.internal.management.cache.CacheIndexesForceRebuildCommand.nodeIdsString;
import static org.apache.ignite.internal.management.cache.CacheIndexesForceRebuildCommand.printBlock;
import static org.apache.ignite.internal.management.cache.CacheIndexesForceRebuildCommand.printEntryNewLine;
import static org.apache.ignite.internal.management.cache.CacheIndexesForceRebuildCommand.printHeader;
import static org.apache.ignite.internal.management.cache.CacheIndexesForceRebuildCommand.storeEntryToNodesResults;

/** Index rebuild via the maintenance mode. */
public class CacheScheduleIndexesRebuildCommand
    implements ComputeCommand<CacheScheduleIndexesRebuildCommandArg, ScheduleIndexRebuildTaskRes> {
    /** */
    public static final String PREF_INDEXES_NOT_FOUND = "WARNING: These indexes were not found:";

    /** */
    public static final String PREF_REBUILD_NOT_SCHEDULED = "WARNING: Indexes rebuild was not scheduled for any cache. " +
        "Check command input.";

    /** */
    public static final String PREF_REBUILD_NOT_SCHEDULED_MULTI = "WARNING: Indexes rebuild was not scheduled for " +
        "any cache on the following nodes. Check command input:";

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
    @Override public Collection<GridClientNode> nodes(Collection<GridClientNode> nodes, CacheScheduleIndexesRebuildCommandArg arg) {
        if (arg.allNodes() || F.isEmpty(arg.nodeIds()) && arg.nodeId() == null)
            return nodes;

        nodeOrAll(arg.nodeId(), nodes);

        return CommandUtils.nodes(arg.nodeId() == null ? arg.nodeIds() : new UUID[] {arg.nodeId()}, nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        CacheScheduleIndexesRebuildCommandArg arg,
        ScheduleIndexRebuildTaskRes results,
        Consumer<String> printer
    ) {
        if (arg.nodeId() != null) {
            printSingleResult(results, printer);

            return;
        }

        Map<String, Set<UUID>> missedCaches = new HashMap<>();
        Map<String, Set<UUID>> missedGrps = new HashMap<>();
        Map<String, Set<UUID>> notFoundIndexes = new HashMap<>();
        Map<String, Set<UUID>> scheduled = new HashMap<>();
        Set<UUID> notScheduled = new HashSet<>();

        results.results().forEach((nodeId, res) -> {
            storeEntryToNodesResults(missedCaches, res.notFoundCacheNames(), nodeId);

            storeEntryToNodesResults(missedGrps, res.notFoundGroupNames(), nodeId);

            storeEntryToNodesResults(notFoundIndexes, extractCacheIndexNames(res.notFoundIndexes()), nodeId);

            if (hasAtLeastOneIndex(res.cacheToIndexes()))
                storeEntryToNodesResults(scheduled, extractCacheIndexNames(res.cacheToIndexes()), nodeId);
            else
                notScheduled.add(nodeId);
        });

        SB b = new SB();

        if (!F.isEmpty(missedCaches))
            printBlock(b, PREF_CACHES_NOT_FOUND, missedCaches, GridStringBuilder::a);

        if (!F.isEmpty(missedGrps))
            printBlock(b, PREF_GROUPS_NOT_FOUND, missedGrps, GridStringBuilder::a);

        if (!F.isEmpty(notFoundIndexes))
            printBlock(b, PREF_INDEXES_NOT_FOUND, notFoundIndexes, GridStringBuilder::a);

        if (!F.isEmpty(notScheduled)) {
            printHeader(b, PREF_REBUILD_NOT_SCHEDULED_MULTI);

            printEntryNewLine(b);

            b.a(nodeIdsString(notScheduled));
        }

        if (!F.isEmpty(scheduled))
            printBlock(b, PREF_SCHEDULED, scheduled, GridStringBuilder::a);

        printer.accept(b.toString().trim());
    }

    /** */
    private static Collection<String> extractCacheIndexNames(Map<String, Set<String>> cacheIndexes) {
        return F.flatCollections(cacheIndexes.entrySet().stream().map(cIdxs -> cIdxs.getValue().stream()
            .map(idx -> indexAndCacheInfo(cIdxs.getKey(), idx)).collect(Collectors.toList())).collect(Collectors.toList()));
    }

    /** */
    private static String indexAndCacheInfo(String cache, String index) {
        return '\'' + index + "' (of cache '" + cache + "')";
    }

    /** */
    private static void printSingleResult(ScheduleIndexRebuildTaskRes result, Consumer<String> printer) {
        result.results().forEach((nodeId, res) -> {
            printMissed(printer, PREF_CACHES_NOT_FOUND, res.notFoundCacheNames());
            printMissed(printer, PREF_GROUPS_NOT_FOUND, res.notFoundGroupNames());

            if (hasAtLeastOneIndex(res.notFoundIndexes())) {
                printer.accept(PREF_INDEXES_NOT_FOUND);

                printCachesAndIndexes(res.notFoundIndexes(), printer);
            }

            if (!F.isEmpty(res.cacheToIndexes()) && hasAtLeastOneIndex(res.cacheToIndexes())) {
                printer.accept(PREF_SCHEDULED);

                printCachesAndIndexes(res.cacheToIndexes(), printer);
            }
            else
                printer.accept(PREF_REBUILD_NOT_SCHEDULED);

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
    private static void printMissed(Consumer<String> printer, String message, Set<String> missed) {
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
        return !F.isEmpty(cacheToIndexes) && cacheToIndexes.values().stream()
            .anyMatch(indexes -> !indexes.isEmpty());
    }

    /** */
    static <T> void storeCacheAndIndexResults(Map<IgnitePair<T>, Set<UUID>> to, Map<T, Set<T>> values, UUID nodeId) {
        if (F.isEmpty(values))
            return;

        values.forEach((cache, indexes) -> indexes.forEach(idx ->
            to.compute(new IgnitePair<>(cache, idx), (c0, idxs0) -> {
                if (idxs0 == null)
                    idxs0 = new HashSet<>();

                idxs0.add(nodeId);

                return idxs0;
            })));
    }
}
