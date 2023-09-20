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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;

/** Index force rebuild. */
public class CacheIndexesForceRebuildCommand
    implements ComputeCommand<CacheIndexesForceRebuildCommandArg, Map<UUID, IndexForceRebuildTaskRes>> {
    /** */
    public static final String PREF_REBUILDING = "WARNING: These caches have indexes rebuilding in progress:";

    /** */
    public static final String PREF_CACHES_NOT_FOUND = "WARNING: These caches were not found:";

    /** */
    private static final String PREF_GROUPS_NOT_FOUND = "WARNING: These cache groups were not found:";

    /** */
    public static final String PREF_REBUILD_STARTED = "Indexes rebuild was started for these caches:";

    /** */
    public static final String PREF_REBUILD_NOT_STARTED_SINGLE = "WARNING: Indexes rebuild was not started for " +
        "any cache. Check command input";

    /** */
    public static final String PREF_REBUILD_NOT_STARTED = "WARNING: Indexes rebuild was not started for " +
        "any cache on the following nodes. Check the command input:";

    /** {@inheritDoc} */
    @Override public String description() {
        return "Triggers rebuild of all indexes for specified caches or cache groups";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheIndexesForceRebuildCommandArg> argClass() {
        return CacheIndexesForceRebuildCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<IndexForceRebuildTask> taskClass() {
        return IndexForceRebuildTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> nodes(Collection<GridClientNode> nodes, CacheIndexesForceRebuildCommandArg arg) {
        Collection<GridClientNode> res;

        if (arg.allNodes())
            res = nodes.stream().filter(n -> !n.isClient()).collect(Collectors.toList());
        else {
            res = arg.nodeIds() != null
                ? CommandUtils.nodes(arg.nodeIds(), nodes)
                : CommandUtils.node(arg.nodeId(), nodes);

            if (!F.isEmpty(res)) {
                for (GridClientNode n : res) {
                    if (n != null && n.isClient())
                        throw new IllegalArgumentException("Please, specify only server node ids");
                }
            }
        }

        if (F.isEmpty(res))
            throw new IllegalArgumentException("Please, specify oat least one server node");

        return res;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        CacheIndexesForceRebuildCommandArg arg,
        Map<UUID, IndexForceRebuildTaskRes> results,
        Consumer<String> printer
    ) {
        if (arg.nodeId() != null) {
            printSingleResult(arg, results.values().iterator().next(), printer);

            return;
        }

        Map<UUID, Set<String>> notFound = null;
        Map<UUID, Set<IndexRebuildStatusInfoContainer>> rebuilding = null;
        Map<UUID, Set<IndexRebuildStatusInfoContainer>> started = null;
        Set<UUID> notStarted = null;

        for (Map.Entry<UUID, IndexForceRebuildTaskRes> e : results.entrySet()) {
            UUID nodeId = e.getKey();
            IndexForceRebuildTaskRes res = e.getValue();

            if (!F.isEmpty(res.notFoundCacheNames()))
                notFound = storeNodeResults(notFound, nodeId, res.notFoundCacheNames());

            if (!F.isEmpty(res.cachesWithRebuildInProgress()))
                rebuilding = storeNodeResults(rebuilding, nodeId, res.cachesWithRebuildInProgress());

            if (!F.isEmpty(res.cachesWithStartedRebuild()))
                started = storeNodeResults(started, nodeId, res.cachesWithStartedRebuild());
            else {
                if (notStarted == null)
                    notStarted = new HashSet<>();

                notStarted.add(nodeId);
            }
        }

        SB b = new SB();

        boolean nl = false;

        if (!F.isEmpty(notFound)) {
            nl = header(b, arg.groupNames() == null ? PREF_CACHES_NOT_FOUND : PREF_GROUPS_NOT_FOUND, nl);

            for (Map.Entry<UUID, Set<String>> e : notFound.entrySet())
                printNodeEntryPrefix(b, e.getKey()).a(e.getValue().stream().sorted().collect(Collectors.joining(","))).a('.');
        }

        if (!F.isEmpty(rebuilding)) {
            nl = header(b, PREF_REBUILDING, nl);

            for (Map.Entry<UUID, Set<IndexRebuildStatusInfoContainer>> e : rebuilding.entrySet())
                pringCachesInfos(b, e.getKey(), e.getValue());
        }

        if (!F.isEmpty(started)) {
            nl = header(b, PREF_REBUILD_STARTED, nl);

            for (Map.Entry<UUID, Set<IndexRebuildStatusInfoContainer>> e : started.entrySet())
                pringCachesInfos(b, e.getKey(), e.getValue());

            b.a(U.nl());
        }

        if (!F.isEmpty(notStarted)) {
            header(b, PREF_REBUILD_NOT_STARTED, nl);

            printListEntryNl(b).a(notStarted.stream().map(UUID::toString).collect(Collectors.joining(","))).a('.');
        }

        printer.accept(b.toString());
    }

    /**
     * Prints new header {@code text} to {@code b} and puts new lines before if required by {@code newLineReuired}.
     *
     * @return {@code True}. After any header new lines are required before a next header.
     */
    private static boolean header(SB b, String text, boolean newLineReuired) {
        if (newLineReuired)
            b.a(U.nl()).a(U.nl());

        b.a(text);

        return true;
    }

    /** */
    private static <T> Map<UUID, Set<T>> storeNodeResults(Map<UUID, Set<T>> to, UUID nodeId, Collection<T> values) {
        if (to == null)
            to = new HashMap<>();

        to.compute(nodeId, (nid, nodeValues) -> {
            if (nodeValues == null)
                nodeValues = new HashSet<>();

            nodeValues.addAll(values);

            return nodeValues;
        });

        return to;
    }

    /**
     * Prints result if only single node was requested with '--node-id' instead of '--node-ids'.
     */
    private static void printSingleResult(
        CacheIndexesForceRebuildCommandArg arg,
        IndexForceRebuildTaskRes res,
        Consumer<String> printer
    ) {
        if (!F.isEmpty(res.notFoundCacheNames())) {
            String warning = arg.groupNames() == null ? PREF_CACHES_NOT_FOUND : PREF_GROUPS_NOT_FOUND;

            printer.accept(warning);

            res.notFoundCacheNames()
                .stream()
                .sorted()
                .forEach(name -> printer.accept(INDENT + name));

            printer.accept("");
        }

        if (!F.isEmpty(res.cachesWithRebuildInProgress())) {
            printer.accept(PREF_REBUILDING);

            printInfos(res.cachesWithRebuildInProgress(), printer);

            printer.accept("");
        }

        if (!F.isEmpty(res.cachesWithStartedRebuild())) {
            printer.accept(PREF_REBUILD_STARTED);

            printInfos(res.cachesWithStartedRebuild(), printer);
        }
        else
            printer.accept(PREF_REBUILD_NOT_STARTED_SINGLE);

        printer.accept("");
    }

    /** */
    private static GridStringBuilder printNodeEntryPrefix(SB b, UUID nodeId) {
        return printListEntryKeySeparator(printListEntryNl(b).a("Node ").a(nodeId));
    }

    /** */
    private static GridStringBuilder printListEntryKeySeparator(GridStringBuilder b) {
        return b.a(':').a(INDENT);
    }

    /** */
    private static GridStringBuilder printListEntryNl(SB b) {
        return b.a(U.nl()).a(INDENT);
    }

    /** */
    private static void pringCachesInfos(SB b, UUID node, Set<IndexRebuildStatusInfoContainer> infos) {
        printNodeEntryPrefix(b, node).a(infos.stream().sorted(IndexRebuildStatusInfoContainer.comparator())
            .map(IndexRebuildStatusInfoContainer::toString)
            .collect(Collectors.joining("; "))).a('.');
    }

    /** */
    private static void printInfos(Collection<IndexRebuildStatusInfoContainer> infos, Consumer<String> printer) {
        infos.stream()
            .sorted(IndexRebuildStatusInfoContainer.comparator())
            .forEach(rebuildStatusInfo -> printer.accept(INDENT + rebuildStatusInfo.toString()));
    }
}
