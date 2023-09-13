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

package org.apache.ignite.internal.commandline.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.IndexForceRebuildCommandArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.index.IndexForceRebuildTask;
import org.apache.ignite.internal.visor.cache.index.IndexForceRebuildTaskArg;
import org.apache.ignite.internal.visor.cache.index.IndexForceRebuildTaskRes;
import org.apache.ignite.internal.visor.cache.index.IndexRebuildStatusInfoContainer;

import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.argument.IndexForceRebuildCommandArg.CACHE_NAMES;
import static org.apache.ignite.internal.commandline.cache.argument.IndexForceRebuildCommandArg.GROUP_NAMES;
import static org.apache.ignite.internal.commandline.cache.argument.IndexForceRebuildCommandArg.NODE_ID;

/**
 * Cache subcommand that triggers indexes force rebuild.
 */
public class CacheIndexesForceRebuild extends AbstractCommand<CacheIndexesForceRebuild.Arguments> {
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

    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        String desc = "Triggers rebuild of all indexes for specified caches or cache groups.";

        Map<String, String> map = U.newLinkedHashMap(16);

        map.put(NODE_ID.argName(), "Specify node for indexes rebuild.");
        map.put(CACHE_NAMES.argName(), "Comma-separated list of cache names for which indexes should be rebuilt.");
        map.put(GROUP_NAMES.argName(), "Comma-separated list of cache group names for which indexes should be rebuilt.");

        usageCache(
            logger,
            CacheSubcommands.INDEX_FORCE_REBUILD,
            desc,
            map,
            NODE_ID.argName() + " nodeId[,nodeId,...]",
            or("--cache-names cacheName1,...cacheNameN", "--group-names groupName1,...groupNameN")
        );
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
        Map<UUID, IndexForceRebuildTaskRes> taskRes;

        IndexForceRebuildTaskArg taskArg = new IndexForceRebuildTaskArg(args.cacheGrps, args.cacheNames);

        try (GridClient client = Command.startClient(clientCfg)) {
            taskRes = TaskExecutor.executeTaskByName(
                client,
                IndexForceRebuildTask.class.getName(),
                taskArg,
                args.nodeIds,
                clientCfg
            );
        }

        printResult(taskRes, logger);

        return taskRes;
    }

    /**
     * @param results Rebuild task results.
     * @param logger  IgniteLogger to print to.
     */
    private void printResult(Map<UUID, IndexForceRebuildTaskRes> results, IgniteLogger logger) {
        if (results.size() == 1) {
            printSingleResult(results.values().iterator().next(), logger);

            return;
        }

        StringBuilder notFound = new StringBuilder();
        StringBuilder rebuilding = new StringBuilder();
        StringBuilder started = new StringBuilder();
        StringBuilder notStarted = new StringBuilder();

        results.forEach((node, res) -> {
            if (!F.isEmpty(res.notFoundCacheNames())) {
                if (notFound.length() == 0)
                    notFound.append(args.cacheGrps == null ? PREF_CACHES_NOT_FOUND : PREF_GROUPS_NOT_FOUND);

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
                    notStarted.append(PREF_REBUILD_STARTED).append(':');

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

        logger.info(res.toString());
    }

    /**
     * @param res    Rebuild task result.
     * @param logger IgniteLogger to print to.
     */
    private void printSingleResult(IndexForceRebuildTaskRes res, IgniteLogger logger) {
        if (!F.isEmpty(res.notFoundCacheNames())) {
            String warning = args.cacheGrps == null ? PREF_CACHES_NOT_FOUND : PREF_GROUPS_NOT_FOUND;

            logger.info(warning);

            res.notFoundCacheNames()
                .stream()
                .sorted()
                .forEach(name -> logger.info(INDENT + name));

            logger.info("");
        }

        if (!F.isEmpty(res.cachesWithRebuildInProgress())) {
            logger.info(PREF_REBUILDING);

            printInfos(res.cachesWithRebuildInProgress(), logger);

            logger.info("");
        }

        if (!F.isEmpty(res.cachesWithStartedRebuild())) {
            logger.info(PREF_REBUILD_STARTED);

            printInfos(res.cachesWithStartedRebuild(), logger);
        }
        else
            logger.info(PREF_REBUILD_NOT_STARTED + '.');

        logger.info("");
    }

    /** */
    private static void printInfos(Collection<IndexRebuildStatusInfoContainer> infos, IgniteLogger logger) {
        infos.stream()
            .sorted(IndexRebuildStatusInfoContainer.comparator())
            .forEach(rebuildStatusInfo -> logger.info(INDENT + rebuildStatusInfo.toString()));
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
    private static StringBuilder newNodeLog(StringBuilder b, UUID nodeId) {
        return b.append(U.nl()).append(INDENT).append("Node ").append(nodeId).append(':').append(INDENT);
    }

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CacheSubcommands.INDEX_FORCE_REBUILD.text().toUpperCase();
    }

    /** Container for command arguments. */
    public static class Arguments {
        /** Node id. */
        private Collection<UUID> nodeIds;

        /** Cache group name. */
        private Set<String> cacheGrps;

        /** Cache name. */
        private Set<String> cacheNames;

        /** */
        public Arguments(Collection<UUID> nodeIds, Set<String> cacheGrps, Set<String> cacheNames) {
            this.nodeIds = nodeIds;
            this.cacheGrps = cacheGrps;
            this.cacheNames = cacheNames;
        }

        /** @return Node ids. */
        public Collection<UUID> nodeIds() {
            return nodeIds;
        }

        /** @return Cache group to scan for, null means scanning all groups. */
        public Set<String> cacheGrps() {
            return cacheGrps;
        }

        /** @return List of caches names. */
        public Set<String> cacheNames() {
            return cacheNames;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Arguments.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIterator) {
        Set<UUID> nodeIds = null;
        Set<String> cacheGrps = null;
        Set<String> cacheNames = null;

        while (argIterator.hasNextSubArg()) {
            String nextArg = argIterator.nextArg("");

            IndexForceRebuildCommandArg arg = CommandArgUtils.of(nextArg, IndexForceRebuildCommandArg.class);

            if (arg == null)
                throw new IllegalArgumentException("Unknown argument: " + nextArg);

            switch (arg) {
                case NODE_ID:
                    if (nodeIds != null)
                        throw new IllegalArgumentException(arg.argName() + " arg specified twice.");

                    nodeIds = Arrays.stream(argIterator.nextArg("Failed to read node id.").split(","))
                        .map(UUID::fromString).collect(Collectors.toSet());

                    break;

                case GROUP_NAMES:
                    if (cacheGrps != null)
                        throw new IllegalArgumentException(arg.argName() + " arg specified twice.");

                    cacheGrps = argIterator.nextStringSet("comma-separated list of cache group names.");

                    if (cacheGrps.equals(Collections.emptySet()))
                        throw new IllegalArgumentException(arg.argName() + " not specified.");

                    break;

                case CACHE_NAMES:
                    if (cacheNames != null)
                        throw new IllegalArgumentException(arg.argName() + " arg specified twice.");

                    cacheNames = argIterator.nextStringSet("comma-separated list of cache names.");

                    if (cacheNames.equals(Collections.emptySet()))
                        throw new IllegalArgumentException(arg.argName() + " not specified.");

                    break;

                default:
                    throw new IllegalArgumentException("Unknown argument: " + arg.argName());
            }
        }

        args = new Arguments(nodeIds, cacheGrps, cacheNames);

        validateArguments();
    }

    /** */
    private void validateArguments() {
        if (F.isEmpty(args.nodeIds))
            throw new IllegalArgumentException(NODE_ID + " must be specified.");

        if ((args.cacheGrps == null) == (args.cacheNames() == null))
            throw new IllegalArgumentException("Either " + GROUP_NAMES + " or " + CACHE_NAMES + " must be specified.");
    }
}
