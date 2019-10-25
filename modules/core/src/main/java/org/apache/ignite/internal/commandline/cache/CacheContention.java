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

import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.processors.cache.verify.ContentionInfo;
import org.apache.ignite.internal.visor.verify.VisorContentionTask;
import org.apache.ignite.internal.visor.verify.VisorContentionTaskArg;
import org.apache.ignite.internal.visor.verify.VisorContentionTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.OP_NODE_ID;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.CONTENTION;

/**
 * Cache contention detection subcommand.
 */
public class CacheContention implements Command<CacheContention.Arguments> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String description = "Show the keys that are point of contention for multiple transactions.";

        usageCache(logger, CONTENTION, description, null, "minQueueSize",
            OP_NODE_ID, optional("maxPrint"));
    }

    /**
     * Container for command arguments.
     */
    public class Arguments {
        /** Node id. */
        private UUID nodeId;

        /** Min queue size. */
        private int minQueueSize;

        /** Max print. */
        private int maxPrint;

        /**
         *
         */
        public Arguments(UUID nodeId, int minQueueSize, int maxPrint) {
            this.nodeId = nodeId;
            this.minQueueSize = minQueueSize;
            this.maxPrint = maxPrint;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Min queue size.
         */
        public int minQueueSize() {
            return minQueueSize;
        }

        /**
         * @return Max print.
         */
        public int maxPrint() {
            return maxPrint;
        }
    }

    /**
     * Command parsed arguments.
     */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        VisorContentionTaskArg taskArg = new VisorContentionTaskArg(args.minQueueSize(), args.maxPrint());

        UUID nodeId = args.nodeId() == null ? BROADCAST_UUID : args.nodeId();

        VisorContentionTaskResult res;

        try (GridClient client = Command.startClient(clientCfg);) {
            res = executeTaskByNameOnNode(client, VisorContentionTask.class.getName(), taskArg, nodeId, clientCfg);
        }

        CommandLogger.printErrors(res.exceptions(), "Contention check failed on nodes:", logger);

        for (ContentionInfo info : res.getInfos())
            info.print();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        int minQueueSize = Integer.parseInt(argIter.nextArg("Min queue size expected"));

        UUID nodeId = null;

        if (argIter.hasNextSubArg())
            nodeId = UUID.fromString(argIter.nextArg(""));

        int maxPrint = 10;

        if (argIter.hasNextSubArg())
            maxPrint = Integer.parseInt(argIter.nextArg(""));

        args = new Arguments(nodeId, minQueueSize, maxPrint);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CONTENTION.text().toUpperCase();
    }
}
