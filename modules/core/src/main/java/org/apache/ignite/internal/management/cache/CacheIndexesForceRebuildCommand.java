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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
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
    public static final String PREF_GROUPS_NOT_FOUND = "WARNING: These cache groups were not found:";

    /** */
    public static final String PREF_REBUILD_STARTED = "Indexes rebuild was started for these caches:";

    /** */
    public static final String PREF_REBUILD_NOT_STARTED_SINGLE = "WARNING: Indexes rebuild was not started for " +
        "any cache. Check command input";

    /** */
    public static final String PREF_REBUILD_NOT_STARTED = "WARNING: Indexes rebuild was not started for " +
        "any cache on the following nodes. Check the command input:";

    /** */
    public static final String PREF_SCHEDULED = "Indexes rebuild was scheduled for these caches:";

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
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, CacheIndexesForceRebuildCommandArg arg) {
        Collection<ClusterNode> res;

        if (arg.allNodes())
            res = nodes.stream().filter(n -> !n.isClient()).collect(Collectors.toList());
        else {
            res = arg.nodeIds() != null
                ? CommandUtils.nodes(arg.nodeIds(), nodes)
                : CommandUtils.node(arg.nodeId(), nodes);

            if (!F.isEmpty(res)) {
                for (ClusterNode n : res) {
                    if (n != null && n.isClient())
                        throw new IllegalArgumentException("Please, specify only server node ids");
                }
            }
        }

        if (F.isEmpty(res))
            throw new IllegalArgumentException("Please, specify at least one server node");

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

        Map<String, Set<UUID>> notFound = new HashMap<>();
        Map<IndexRebuildStatusInfoContainer, Set<UUID>> rebuilding = new HashMap<>();
        Map<IndexRebuildStatusInfoContainer, Set<UUID>> started = new HashMap<>();
        Set<UUID> notStarted = new HashSet<>();

        results.forEach((nodeId, res) -> {
            storeEntryToNodesResults(notFound, res.notFoundCacheNames(), nodeId);

            storeEntryToNodesResults(rebuilding, res.cachesWithRebuildInProgress(), nodeId);

            if (!F.isEmpty(res.cachesWithStartedRebuild()))
                storeEntryToNodesResults(started, res.cachesWithStartedRebuild(), nodeId);
            else
                notStarted.add(nodeId);
        });

        SB b = new SB();

        if (!F.isEmpty(notFound))
            printBlock(b, arg.groupNames() == null ? PREF_CACHES_NOT_FOUND : PREF_GROUPS_NOT_FOUND, notFound);

        if (!F.isEmpty(notStarted)) {
            printHeader(b, PREF_REBUILD_NOT_STARTED);

            printEntryNewLine(b);

            b.a(nodeIdsString(notStarted));
        }

        if (!F.isEmpty(rebuilding))
            printBlock(b, PREF_REBUILDING, rebuilding);

        if (!F.isEmpty(started))
            printBlock(b, PREF_REBUILD_STARTED, started);

        printer.accept(b.toString().trim());
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
    static <T> void storeEntryToNodesResults(Map<T, Set<UUID>> to, Collection<T> keys, UUID nodeId) {
        if (F.isEmpty(keys))
            return;

        for (T kv : keys) {
            to.compute(kv, (kv0, nodeIds0) -> {
                if (nodeIds0 == null)
                    nodeIds0 = new HashSet<>();

                nodeIds0.add(nodeId);

                return nodeIds0;
            });
        }
    }

    /** */
    private static void printInfos(Collection<IndexRebuildStatusInfoContainer> infos, Consumer<String> printer) {
        infos.stream()
            .sorted(IndexRebuildStatusInfoContainer.comparator())
            .forEach(rebuildStatusInfo -> printer.accept(INDENT + rebuildStatusInfo.toString()));
    }

    /** */
    static void printBlock(SB b, String header, Map<?, ? extends Collection<UUID>> data, BiConsumer<SB, Object> cacheInfoPrinter) {
        printHeader(b, header);

        data.forEach((cacheInfo, nodes) -> {
            printEntryNewLine(b);

            cacheInfoPrinter.accept(b, cacheInfo);

            b.a(" on nodes ").a(nodeIdsString(nodes)).a('.');
        });
    }

    /** */
    static void printBlock(SB b, String header, Map<?, ? extends Collection<UUID>> data) {
        printBlock(b, header, data, CacheIndexesForceRebuildCommand::printCacheInfo);
    }

    /** */
    static void printEntryNewLine(SB b) {
        b.a(U.nl()).a(INDENT);
    }

    /** */
    static String nodeIdsString(Collection<UUID> nodes) {
        return nodes.stream().map(uuid -> '\'' + uuid.toString() + '\'').collect(Collectors.joining(", "));
    }

    /** */
    static void printHeader(SB b, String header) {
        b.a(U.nl()).a(U.nl()).a(header);
    }

    /** */
    private static void printCacheInfo(SB b, Object info) {
        if (info.getClass() == String.class)
            b.a('\'').a(info).a('\'');
        else if (info instanceof IndexRebuildStatusInfoContainer) {
            IndexRebuildStatusInfoContainer status = (IndexRebuildStatusInfoContainer)info;

            b.a('\'').a(status.cacheName()).a('\'');

            if (!F.isEmpty(status.groupName()))
                b.a(" (groupName='").a(status.groupName()).a("')");
        }
    }
}
