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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
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
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.argument.IndexForceRebuildCommandArg.CACHE_NAMES;
import static org.apache.ignite.internal.commandline.cache.argument.IndexForceRebuildCommandArg.GROUP_NAMES;
import static org.apache.ignite.internal.commandline.cache.argument.IndexForceRebuildCommandArg.NODE_ID;
import static org.apache.ignite.internal.commandline.cache.argument.IndexListCommandArg.CACHE_NAME;
import static org.apache.ignite.internal.commandline.cache.argument.IndexListCommandArg.GRP_NAME;

/**
 * Cache subcommand that triggers indexes force rebuild.
 */
public class CacheIndexesForceRebuild extends AbstractCommand<CacheIndexesForceRebuild.Arguments> {
    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
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
            NODE_ID.argName() + " nodeId",
            or(CACHE_NAME + " cacheName1,...cacheNameN", GRP_NAME + " groupName1,...groupNameN")
        );
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        IndexForceRebuildTaskRes taskRes;

        IndexForceRebuildTaskArg taskArg = new IndexForceRebuildTaskArg(args.cacheGrps, args.cacheNames);

        final UUID nodeId = args.nodeId;

        try (GridClient client = Command.startClient(clientCfg)) {
            taskRes = TaskExecutor.executeTaskByNameOnNode(
                client,
                IndexForceRebuildTask.class.getName(),
                taskArg,
                nodeId,
                clientCfg
            );
        }

        printResult(taskRes, logger);

        return taskRes;
    }

    /**
     * @param res Rebuild task result.
     * @param logger Logger to print to.
     */
    private void printResult(IndexForceRebuildTaskRes res, Logger logger) {
        if (!F.isEmpty(res.notFoundCacheNames())) {
            String warning = args.cacheGrps == null ?
                "WARNING: These caches were not found:" : "WARNING: These cache groups were not found:";

            logger.info(warning);

            res.notFoundCacheNames()
                .stream()
                .sorted()
                .forEach(name -> logger.info(INDENT + name));

            logger.info("");
        }

        if (!F.isEmpty(res.cachesWithRebuildInProgress())) {
            logger.info("WARNING: These caches have indexes rebuilding in progress:");

            printInfos(res.cachesWithRebuildInProgress(), logger);

            logger.info("");
        }

        if (!F.isEmpty(res.cachesWithStartedRebuild())) {
            logger.info("Indexes rebuild was started for these caches:");

            printInfos(res.cachesWithStartedRebuild(), logger);
        }
        else
            logger.info("WARNING: Indexes rebuild was not started for any cache. Check command input.");

        logger.info("");
    }

    /** */
    private void printInfos(Collection<IndexRebuildStatusInfoContainer> infos, Logger logger) {
        infos.stream()
            .sorted(IndexRebuildStatusInfoContainer.comparator())
            .forEach(rebuildStatusInfo -> logger.info(INDENT + rebuildStatusInfo.toString()));
    }

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CacheSubcommands.INDEX_FORCE_REBUILD.text().toUpperCase();
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Node id. */
        private UUID nodeId;

        /** Cache group name. */
        private Set<String> cacheGrps;

        /** Cache name. */
        private Set<String> cacheNames;

        /** */
        public Arguments(UUID nodeId, Set<String> cacheGrps, Set<String> cacheNames) {
            this.nodeId = nodeId;
            this.cacheGrps = cacheGrps;
            this.cacheNames = cacheNames;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Cache group to scan for, null means scanning all groups.
         */
        public Set<String> cacheGrps() {
            return cacheGrps;
        }

        /**
         * @return List of caches names.
         */
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
        UUID nodeId = null;
        Set<String> cacheGrps = null;
        Set<String> cacheNames = null;

        while (argIterator.hasNextSubArg()) {
            String nextArg = argIterator.nextArg("");

            IndexForceRebuildCommandArg arg = CommandArgUtils.of(nextArg, IndexForceRebuildCommandArg.class);

            if (arg == null)
                throw new IllegalArgumentException("Unknown argument: " + nextArg);

            switch (arg) {
                case NODE_ID:
                    if (nodeId != null)
                        throw new IllegalArgumentException(arg.argName() + " arg specified twice.");

                    nodeId = UUID.fromString(argIterator.nextArg("Failed to read node id."));

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

        args = new Arguments(nodeId, cacheGrps, cacheNames);

        validateArguments();
    }

    /** */
    private void validateArguments() {
        if (args.nodeId == null)
            throw new IllegalArgumentException(NODE_ID + " must be specified.");

        if ((args.cacheGrps == null) == (args.cacheNames() == null))
            throw new IllegalArgumentException("Either " + GROUP_NAMES + " or " + CACHE_NAMES + " must be specified.");
    }
}
