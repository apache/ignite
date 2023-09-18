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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;

/** Index force rebuild. */
public class CacheIndexesForceRebuildCommand implements ComputeCommand<CacheIndexesForceRebuildCommandArg, Map<UUID, IndexForceRebuildTaskRes>> {
    /** */
    private static final String PREF_REBUILDING = "WARNING: These caches have indexes rebuilding in progress:";

    /** */
    private static final String PREF_CACHES_NOT_FOUND = "WARNING: These caches were not found:";

    /** */
    private static final String PREF_GROUPS_NOT_FOUND = "WARNING: These cache groups were not found:";

    /** */
    private static final String PREF_REBUILD_STARTED = "Indexes rebuild was started for these caches:";

    /** */
    private static final String PREF_REBUILD_NOT_STARTED = "WARNING: Indexes rebuild was not started for any cache. " +
        "Check command input";

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
        Collection<GridClientNode> res = arg.nodeIds() != null
            ? CommandUtils.nodes(arg.nodeIds(), nodes)
            : CommandUtils.nodeOrNull(arg.nodeId(), nodes);

        if (!F.isEmpty(res)) {
            for (GridClientNode n : res) {
                if (n != null && n.isClient())
                    throw new IllegalArgumentException("Please, specify server node id");
            }

            return res;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        CacheIndexesForceRebuildCommandArg arg,
        Map<UUID, IndexForceRebuildTaskRes> results,
        Consumer<String> printer
    ) {
        /**
         * @param results Rebuild task results.
         * @param logger  IgniteLogger to print to.
         */
        if (results.size() == 1) {
            printSingleResult(arg, results.values().iterator().next(), printer);

            return;
        }

        StringBuilder notFound = new StringBuilder();
        StringBuilder rebuilding = new StringBuilder();
        StringBuilder started = new StringBuilder();
        StringBuilder notStarted = new StringBuilder();

        results.forEach((node, res) -> {
            if (!F.isEmpty(res.notFoundCacheNames())) {
                if (notFound.length() == 0)
                    notFound.append(arg.groupNames() == null ? PREF_CACHES_NOT_FOUND : PREF_GROUPS_NOT_FOUND);

                newNodeLog(notFound, node);

                notFound.append(res.notFoundCacheNames().stream().sorted().collect(Collectors.joining(",")));
            }

            if (!F.isEmpty(res.cachesWithRebuildInProgress())) {
                if (rebuilding.length() == 0)
                    rebuilding.append(PREF_REBUILDING);

                printInfos(rebuilding, node, res.cachesWithRebuildInProgress());
            }

            if (F.isEmpty(res.cachesWithStartedRebuild())) {
                if (notStarted.length() == 0)
                    notStarted.append(PREF_REBUILD_NOT_STARTED).append(':');

                notStarted.append(node).append(',');
            }
            else {
                if (started.length() == 0)
                    started.append(PREF_REBUILD_STARTED);

                printInfos(started, node, res.cachesWithStartedRebuild());
            }
        });

        if (notStarted.length() > 0)
            notStarted.delete(notStarted.length() - 1, notStarted.length());

        StringBuilder res = new StringBuilder();

        if (notFound.length() > 0)
            res.append(notFound).append(U.nl());

        if (rebuilding.length() > 0)
            res.append(rebuilding).append(U.nl());

        if (notStarted.length() > 0)
            res.append(notStarted).append(U.nl());

        if (started.length() > 0)
            res.append(started).append(U.nl());

        printer.accept(res.toString());
    }

    /** */
    private void printSingleResult(
        CacheIndexesForceRebuildCommandArg arg,
        IndexForceRebuildTaskRes res,
        Consumer<String> printer
    ) {
        if (!F.isEmpty(res.notFoundCacheNames())) {
            String warning = arg.groupNames() == null ?
                PREF_CACHES_NOT_FOUND : PREF_GROUPS_NOT_FOUND;

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
            printer.accept(PREF_REBUILD_NOT_STARTED);

        printer.accept("");
    }

    /** */
    private static StringBuilder newNodeLog(StringBuilder b, UUID nodeId) {
        return b.append(U.nl()).append(INDENT).append("Node ").append(nodeId).append(':').append(INDENT);
    }

    /** */
    private static void printInfos(StringBuilder b, UUID node, Set<IndexRebuildStatusInfoContainer> infos) {
        infos.stream()
            .sorted(IndexRebuildStatusInfoContainer.comparator())
            .forEach(rebuildStatusInfo -> newNodeLog(b, node).append(rebuildStatusInfo.toString()).append("; "));

        if (!infos.isEmpty()) {
            b.delete(b.length() - 2, b.length());

            b.append('.');
        }
    }

    /** */
    private void printInfos(Collection<IndexRebuildStatusInfoContainer> infos, Consumer<String> printer) {
        infos.stream()
            .sorted(IndexRebuildStatusInfoContainer.comparator())
            .forEach(rebuildStatusInfo -> printer.accept(INDENT + rebuildStatusInfo.toString()));
    }
}
