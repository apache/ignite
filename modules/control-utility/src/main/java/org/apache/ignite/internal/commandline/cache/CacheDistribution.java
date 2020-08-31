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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTask;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskArg;
import org.apache.ignite.internal.commandline.cache.distribution.CacheDistributionTaskResult;

import static org.apache.ignite.internal.commandline.CommandHandler.NULL;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.NODE_ID;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.DISTRIBUTION;
import static org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg.USER_ATTRIBUTES;

/**
 * Would collect and print info about how data is spread between nodes and partitions.
 */
public class CacheDistribution implements Command<CacheDistribution.Arguments> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String CACHES = "cacheName1,...,cacheNameN";
        String description = "Prints the information about partition distribution.";

        usageCache(logger, DISTRIBUTION, description, null,
            or(NODE_ID, CommandHandler.NULL), optional(CACHES), optional(USER_ATTRIBUTES, "attrName1,...,attrNameN"));
    }

    /**
     * Container for command arguments.
     */
    public class Arguments {
        /** Caches. */
        private Set<String> caches;

        /** Node id. */
        private UUID nodeId;

        /** Additional user attributes in result. Set of attribute names whose values will be searched in ClusterNode.attributes(). */
        private Set<String> userAttributes;

        /**
         *
         */
        public Arguments(Set<String> caches, UUID nodeId, Set<String> userAttributes) {
            this.caches = caches;
            this.nodeId = nodeId;
            this.userAttributes = userAttributes;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Additional user attributes in result. Set of attribute names whose values will be searched in ClusterNode.attributes().
         */
        public Set<String> getUserAttributes() {
            return userAttributes;
        }
    }

    /** Command parsed arguments */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        CacheDistributionTaskArg taskArg = new CacheDistributionTaskArg(args.caches(), args.getUserAttributes());

        UUID nodeId = args.nodeId() == null ? BROADCAST_UUID : args.nodeId();

        CacheDistributionTaskResult res;

        try (GridClient client = Command.startClient(clientCfg)) {
            res = executeTaskByNameOnNode(client, CacheDistributionTask.class.getName(), taskArg, nodeId, clientCfg);
        }

        CommandLogger.printErrors(res.exceptions(), "Cache distrubution task failed on nodes:", logger);

        res.print(System.out);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        UUID nodeId = null;
        Set<String> caches = null;
        Set<String> userAttributes = null;

        String nodeIdStr = argIter.nextArg("Node id expected or null");

        if (!NULL.equals(nodeIdStr))
            nodeId = UUID.fromString(nodeIdStr);

        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("");

            DistributionCommandArg arg = CommandArgUtils.of(nextArg, DistributionCommandArg.class);

            if (arg == USER_ATTRIBUTES) {
                nextArg = argIter.nextArg("User attributes are expected to be separated by commas");

                userAttributes = new HashSet<>();

                for (String userAttribute : nextArg.split(","))
                    userAttributes.add(userAttribute.trim());

                nextArg = (argIter.hasNextSubArg()) ? argIter.nextArg("") : null;

            }

            if (nextArg != null)
                caches = argIter.parseStringSet(nextArg);
        }

        args = new Arguments(caches, nodeId, userAttributes);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DISTRIBUTION.text().toUpperCase();
    }
}
