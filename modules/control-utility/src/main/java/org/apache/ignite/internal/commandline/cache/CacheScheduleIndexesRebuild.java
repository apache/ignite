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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.IndexRebuildCommandArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.cache.index.ScheduleIndexRebuildTask;
import org.apache.ignite.internal.visor.cache.index.ScheduleIndexRebuildTaskArg;
import org.apache.ignite.internal.visor.cache.index.ScheduleIndexRebuildTaskRes;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.cache.argument.IndexRebuildCommandArg.CACHE_GROUPS_TARGET;
import static org.apache.ignite.internal.commandline.cache.argument.IndexRebuildCommandArg.CACHE_NAMES_TARGET;
import static org.apache.ignite.internal.commandline.cache.argument.IndexRebuildCommandArg.NODE_ID;

/**
 * Cache subcommand that schedules indexes rebuild via the maintenance mode.
 */
public class CacheScheduleIndexesRebuild extends AbstractCommand<CacheScheduleIndexesRebuild.Arguments> {
    /** --cache-names parameter format. */
    private static final String CACHE_NAMES_FORMAT = "cacheName[index1,...indexN],cacheName2,cacheName3[index1]";

    /** --group-names parameter format. */
    private static final String CACHE_GROUPS_FORMAT = "groupName1,groupName2,...groupNameN";

    /** Command's parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        String desc = "Schedules rebuild of the indexes for specified caches via the Maintenance Mode. Schedules rebuild of specified "
            + "caches and cache-groups";

        Map<String, String> map = new LinkedHashMap<>(2);

        map.put(NODE_ID.argName(), "(Optional) Specify node for indexes rebuild. If not specified, schedules rebuild on all nodes.");

        map.put(
            CACHE_NAMES_TARGET.argName(),
            "Comma-separated list of cache names with optionally specified indexes. If indexes are not specified then all indexes "
            + "of the cache will be scheduled for the rebuild operation. Can be used simultaneously with cache group names."
        );

        map.put(CACHE_GROUPS_TARGET.argName(), "Comma-separated list of cache group names for which indexes should be scheduled for the "
            + "rebuild. Can be used simultaneously with cache names.");

        usageCache(
            logger,
            CacheSubcommands.INDEX_REBUILD,
            desc,
            map,
            NODE_ID.argName() + " nodeId",
            CACHE_NAMES_TARGET + " " + CACHE_NAMES_FORMAT,
            CACHE_GROUPS_TARGET + " " + CACHE_GROUPS_FORMAT
        );
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
        ScheduleIndexRebuildTaskRes taskRes;

        try (GridClient client = Command.startClient(clientCfg)) {
            UUID nodeId = args.nodeId;

            if (nodeId == null)
                nodeId = TaskExecutor.BROADCAST_UUID;

            taskRes = TaskExecutor.executeTaskByNameOnNode(
                client,
                ScheduleIndexRebuildTask.class.getName(),
                new ScheduleIndexRebuildTaskArg(args.cacheToIndexes, args.cacheGroups),
                nodeId,
                clientCfg
            );
        }

        printResult(taskRes, logger);

        return taskRes;
    }

    /**
     * @param taskRes Rebuild task result.
     * @param logger IgniteLogger to print to.
     */
    private void printResult(ScheduleIndexRebuildTaskRes taskRes, IgniteLogger logger) {
        taskRes.results().forEach((nodeId, res) -> {
            printMissed(logger, "WARNING: These caches were not found:", res.notFoundCacheNames());
            printMissed(logger, "WARNING: These cache groups were not found:", res.notFoundGroupNames());

            if (!F.isEmpty(res.notFoundIndexes()) && hasAtLeastOneIndex(res.notFoundIndexes())) {
                String warning = "WARNING: These indexes were not found:";

                logger.info(warning);

                printCachesAndIndexes(res.notFoundIndexes(), logger);
            }

            if (!F.isEmpty(res.cacheToIndexes()) && hasAtLeastOneIndex(res.cacheToIndexes())) {
                logger.info("Indexes rebuild was scheduled for these caches:");

                printCachesAndIndexes(res.cacheToIndexes(), logger);
            }
            else
                logger.info("WARNING: Indexes rebuild was not scheduled for any cache. Check command input.");

            logger.info("");
        });
    }

    /**
     * Prints missed caches' or cache groups' names.
     *
     * @param logger Logger.
     * @param message Message.
     * @param missed Missed caches or cache groups' names.
     */
    private void printMissed(IgniteLogger logger, String message, Set<String> missed) {
        if (!F.isEmpty(missed)) {
            logger.info(message);

            missed.stream()
                .sorted()
                .forEach(name -> logger.info(INDENT + name));

            logger.info("");
        }
    }

    /**
     * Prints caches and their indexes.
     *
     * @param cachesToIndexes Cache -> indexes map.
     * @param logger Logger.
     */
    private static void printCachesAndIndexes(Map<String, Set<String>> cachesToIndexes, IgniteLogger logger) {
        cachesToIndexes.forEach((cacheName, indexes) -> {
            logger.info(INDENT + cacheName + ":");
            indexes.forEach(index -> logger.info(INDENT + INDENT + index));
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

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CacheSubcommands.INDEX_REBUILD.text().toUpperCase();
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Node id. */
        @Nullable
        private final UUID nodeId;

        /** Cache name -> indexes. */
        @Nullable
        private final Map<String, Set<String>> cacheToIndexes;

        /** Cache groups' names. */
        @Nullable
        private final Set<String> cacheGroups;

        /** */
        private Arguments(@Nullable UUID nodeId, @Nullable Map<String, Set<String>> cacheToIndexes, @Nullable Set<String> cacheGroups) {
            this.nodeId = nodeId;
            this.cacheToIndexes = cacheToIndexes;
            this.cacheGroups = cacheGroups;
        }

        /**
         * @return Cache -> indexes map.
         */
        @Nullable
        public Map<String, Set<String>> cacheToIndexes() {
            return cacheToIndexes;
        }

        /**
         * @return Cache groups.
         */
        @Nullable
        public Set<String> cacheGroups() {
            return cacheGroups;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Arguments.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIterator) {
        UUID nodeId = null;
        Map<String, Set<String>> cacheToIndexes = null;
        Set<String> cacheGroups = null;

        while (argIterator.hasNextSubArg()) {
            String nextArg = argIterator.nextArg("");

            IndexRebuildCommandArg arg = CommandArgUtils.of(nextArg, IndexRebuildCommandArg.class);

            if (arg == null)
                throw new IllegalArgumentException("Unknown argument: " + nextArg);

            switch (arg) {
                case NODE_ID:
                    if (nodeId != null)
                        throw new IllegalArgumentException(arg.argName() + " arg specified twice.");

                    nodeId = UUID.fromString(argIterator.nextArg("Failed to read node id."));

                    break;

                case CACHE_NAMES_TARGET:
                    if (cacheToIndexes != null)
                        throw new IllegalArgumentException(arg.argName() + " arg specified twice.");

                    cacheToIndexes = new HashMap<>();

                    String cacheNamesArg = argIterator.nextArg("Expected a comma-separated cache names (and optionally a"
                        + " comma-separated list of index names in square brackets).");

                    Pattern cacheNamesPattern = Pattern.compile("([^,\\[\\]]+)(\\[(.*?)])?");
                    Matcher matcher = cacheNamesPattern.matcher(cacheNamesArg);

                    boolean found = false;

                    while (matcher.find()) {
                        found = true;

                        String cacheName = matcher.group(1);
                        boolean specifiedIndexes = matcher.group(2) != null;
                        String commaSeparatedIndexes = matcher.group(3);

                        if (!specifiedIndexes) {
                            cacheToIndexes.put(cacheName, Collections.emptySet());

                            continue;
                        }

                        if (F.isEmpty(commaSeparatedIndexes)) {
                            throw new IllegalArgumentException("Square brackets must contain comma-separated indexes or not be used "
                                + "at all.");
                        }

                        Set<String> indexes = Arrays.stream(commaSeparatedIndexes.split(",")).collect(toSet());
                        cacheToIndexes.put(cacheName, indexes);
                    }

                    if (!found)
                        throw new IllegalArgumentException("Wrong format for --cache-names, should be: " + CACHE_NAMES_FORMAT);

                    break;

                case CACHE_GROUPS_TARGET:
                    if (cacheGroups != null)
                        throw new IllegalArgumentException(arg.argName() + " arg specified twice.");

                    String cacheGroupsArg = argIterator.nextArg("Expected comma-separated cache group names");

                    cacheGroups = Arrays.stream(cacheGroupsArg.split(",")).collect(toSet());

                    break;

                default:
                    throw new IllegalArgumentException("Unknown argument: " + arg.argName());
            }
        }

        args = new Arguments(nodeId, cacheToIndexes, cacheGroups);

        validateArguments();
    }

    /** */
    private void validateArguments() {
        Set<String> cacheGroups = args.cacheGroups;
        Map<String, Set<String>> cacheToIndexes = args.cacheToIndexes;

        if ((cacheGroups == null || cacheGroups.isEmpty()) && (cacheToIndexes == null || cacheToIndexes.isEmpty()))
            throw new IllegalArgumentException(CACHE_NAMES_TARGET + " or " + CACHE_GROUPS_TARGET + " must be specified.");
    }
}
