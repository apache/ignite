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

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.IndexListCommandArg;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.index.IndexRebuildStatusInfoContainer;
import org.apache.ignite.internal.visor.cache.index.IndexRebuildStatusTask;
import org.apache.ignite.internal.visor.cache.index.IndexRebuildStatusTaskArg;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.argument.IndexListCommandArg.NODE_ID;

/**
 * Cache subcommand that allows to show caches that have
 */
public class CacheIndexesRebuildStatus implements Command<CacheIndexesRebuildStatus.Arguments> {
    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String desc = "List all indexes that have index rebuild in progress.";

        Map<String, String> map = U.newLinkedHashMap(8);

        map.put(NODE_ID.argName() + " nodeId",
            "Specify node for job execution. If not specified explicitly, info will be gathered from all nodes");

        usageCache(
            logger,
            CacheSubcommands.INDEX_REBUILD_STATUS,
            desc,
            map,
            optional(NODE_ID + " nodeId")
        );
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        Map<UUID, Set<IndexRebuildStatusInfoContainer>> taskRes;

        final UUID nodeId = args.nodeId;

        IndexRebuildStatusTaskArg taskArg = new IndexRebuildStatusTaskArg(nodeId);

        try (GridClient client = Command.startClient(clientCfg)) {
            taskRes = TaskExecutor.executeTaskByNameOnNode(client, IndexRebuildStatusTask.class.getName(), taskArg,
                nodeId, clientCfg);
        }

        printStatus(taskRes, logger);

        return taskRes;
    }

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CacheSubcommands.INDEX_REBUILD_STATUS.text().toUpperCase();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIterator) {
        UUID nodeId = null;

        while (argIterator.hasNextSubArg()) {
            String nextArg = argIterator.nextArg("");

            IndexListCommandArg arg = CommandArgUtils.of(nextArg, IndexListCommandArg.class);

            if (arg == IndexListCommandArg.NODE_ID) {
                if (nodeId != null)
                    throw new IllegalArgumentException(arg.argName() + " arg specified twice.");

                nodeId = UUID.fromString(argIterator.nextArg("Failed to read node id"));
            }
            else
                throw new IllegalArgumentException("Unknown argument: " + nextArg);
        }

        args = new Arguments(nodeId);
    }

    /**
     * Prints caches infos grouped by node id.
     *
     * @param res Task result.
     * @param logger Logger to use.
     */
    private void printStatus(Map<UUID, Set<IndexRebuildStatusInfoContainer>> res, Logger logger) {
        if (!res.isEmpty())
            logger.info("Caches that have index rebuilding in progress:");
        else {
            logger.info("There are no caches that have index rebuilding in progress.");
            logger.info("");

            return;
        }

        for (Map.Entry<UUID, Set<IndexRebuildStatusInfoContainer>> entry: res.entrySet()) {
            logger.info("");

            entry.getValue().stream()
                .sorted(IndexRebuildStatusInfoContainer.comparator())
                .forEach(container -> logger.info(constructCacheOutputString(entry.getKey(), container)));
        }

        logger.info("");
    }

    /** */
    private String constructCacheOutputString(UUID nodeId, IndexRebuildStatusInfoContainer container) {
        return "node_id=" + nodeId + ", " + container.toString();
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Node id. */
        private UUID nodeId;

        /** */
        public Arguments(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheIndexesRebuildStatus.Arguments.class, this);
        }
    }
}
