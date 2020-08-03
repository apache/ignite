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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceJobResult;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTask;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTaskArg;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.OP_NODE_ID;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.FIND_AND_DELETE_GARBAGE;

/**
 * Command to find and delete garbage which could left after destroying caches in shared group.
 */
public class FindAndDeleteGarbage implements Command<FindAndDeleteGarbage.Arguments> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String GROUPS = "groupName1,...,groupNameN";
        String description = "Find and optionally delete garbage from shared cache groups which could be left " +
            "after cache destroy.";

        usageCache(logger, FIND_AND_DELETE_GARBAGE, description, null,
            optional(GROUPS), OP_NODE_ID, optional(FindAndDeleteGarbageArg.DELETE));
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Groups. */
        private Set<String> groups;

        /** Node id. */
        private UUID nodeId;

        /** Delete garbage flag. */
        private boolean delete;

        /**
         *
         */
        public Arguments(Set<String> groups, UUID nodeId, boolean delete) {
            this.groups = groups;
            this.nodeId = nodeId;
            this.delete = delete;
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
        public Set<String> groups() {
            return groups;
        }

        /**
         * @return True if it is needed to delete found garbage.
         */
        public boolean delete() {
            return delete;
        }
    }

    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        VisorFindAndDeleteGarbageInPersistenceTaskArg taskArg = new VisorFindAndDeleteGarbageInPersistenceTaskArg(
            args.groups(),
            args.delete(),
            args.nodeId() != null ? Collections.singleton(args.nodeId()) : null
        );

        try (GridClient client = Command.startClient(clientCfg)) {
            VisorFindAndDeleteGarbageInPersistenceTaskResult taskRes = executeTask(
                client, VisorFindAndDeleteGarbageInPersistenceTask.class, taskArg, clientCfg);

            CommandLogger.printErrors(taskRes.exceptions(), "Scanning for garbage failed on nodes:", logger);

            for (Map.Entry<UUID, VisorFindAndDeleteGarbageInPersistenceJobResult> nodeEntry : taskRes.result().entrySet()) {
                if (!nodeEntry.getValue().hasGarbage()) {
                    logger.info("Node " + nodeEntry.getKey() + " - garbage not found.");

                    continue;
                }

                logger.info("Garbage found on node " + nodeEntry.getKey() + ":");

                VisorFindAndDeleteGarbageInPersistenceJobResult value = nodeEntry.getValue();

                Map<Integer, Map<Integer, Long>> grpPartErrorsCount = value.checkResult();

                if (!grpPartErrorsCount.isEmpty()) {
                    for (Map.Entry<Integer, Map<Integer, Long>> entry : grpPartErrorsCount.entrySet()) {
                        for (Map.Entry<Integer, Long> e : entry.getValue().entrySet()) {
                            logger.info(INDENT + "Group=" + entry.getKey() +
                                ", partition=" + e.getKey() +
                                ", count of keys=" + e.getValue());
                        }
                    }
                }

                logger.info("");
            }

            return taskRes;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        boolean delete = false;
        UUID nodeId = null;
        Set<String> groups = null;

        int argsCnt = 0;

        while (argIter.hasNextSubArg() && argsCnt++ < 3) {
            String nextArg = argIter.nextArg("");

            FindAndDeleteGarbageArg arg = CommandArgUtils.of(nextArg, FindAndDeleteGarbageArg.class);

            if (arg == FindAndDeleteGarbageArg.DELETE) {
                delete = true;

                continue;
            }

            try {
                nodeId = UUID.fromString(nextArg);

                continue;
            }
            catch (IllegalArgumentException ignored) {
                //No-op.
            }

            groups = argIter.parseStringSet(nextArg);
        }

        args = new Arguments(groups, nodeId, delete);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return FIND_AND_DELETE_GARBAGE.text().toUpperCase();
    }
}
