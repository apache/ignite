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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.IndexListCommandArg;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.index.IndexListInfoContainer;
import org.apache.ignite.internal.visor.cache.index.IndexListTaskArg;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.argument.IndexListCommandArg.CACHE_NAME;
import static org.apache.ignite.internal.commandline.cache.argument.IndexListCommandArg.GRP_NAME;
import static org.apache.ignite.internal.commandline.cache.argument.IndexListCommandArg.IDX_NAME;
import static org.apache.ignite.internal.commandline.cache.argument.IndexListCommandArg.NODE_ID;

/**
 * Cache subcommand that allows to show indexes.
 */
public class CacheIndexesList implements Command<CacheIndexesList.Arguments> {
    /** Command parsed arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String desc = "List all indexes that match specified filters.";

        Map<String, String> map = U.newLinkedHashMap(16);

        map.put(NODE_ID.argName() + " nodeId",
            "Specify node for job execution. If not specified explicitly, node will be chosen by grid");
        map.put(GRP_NAME.argName() + " regExp",
            "Regular expression allowing filtering by cache group name");
        map.put(CACHE_NAME.argName() + " regExp",
            "Regular expression allowing filtering by cache name");
        map.put(IDX_NAME.argName() + " regExp",
            "Regular expression allowing filtering by index name");

        usageCache(
            logger,
            CacheSubcommands.INDEX_LIST,
            desc,
            map,
            optional(NODE_ID + " nodeId"),
            optional(GRP_NAME + " grpRegExp"),
            optional(CACHE_NAME + " cacheRegExp"),
            optional(IDX_NAME + " idxNameRegExp")
        );
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        Set<IndexListInfoContainer> taskRes;

        final UUID nodeId = args.nodeId;

        IndexListTaskArg taskArg = new IndexListTaskArg(args.groupsRegEx, args.cachesRegEx, args.indexesRegEx);

        try (GridClient client = Command.startClient(clientCfg)) {
            taskRes = TaskExecutor.executeTaskByNameOnNode(client,
                "org.apache.ignite.internal.visor.cache.index.IndexListTask", taskArg, nodeId, clientCfg);
        }

        printIndexes(taskRes, logger);

        return taskRes;
    }

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CacheSubcommands.INDEX_LIST.text().toUpperCase();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIterator) {
        UUID nodeId = null;
        String groupsRegEx = null;
        String cachesRegEx = null;
        String indexesRegEx = null;

        while (argIterator.hasNextSubArg()) {
            String nextArg = argIterator.nextArg("");

            IndexListCommandArg arg = CommandArgUtils.of(nextArg, IndexListCommandArg.class);

            if (arg == null)
                throw new IllegalArgumentException("Unknown argument: " + nextArg);

            switch (arg) {
                case NODE_ID:
                    if (nodeId != null)
                        throw new IllegalArgumentException(arg.argName() + " arg specified twice.");

                    nodeId = UUID.fromString(argIterator.nextArg("Failed to read node id."));

                    break;

                case GRP_NAME:
                    groupsRegEx = argIterator.nextArg("Failed to read group name regex.");

                    if (!validateRegEx(groupsRegEx))
                        throw new IllegalArgumentException("Invalid group name regex: " + groupsRegEx);

                    break;

                case CACHE_NAME:
                    cachesRegEx = argIterator.nextArg("Failed to read cache name regex.");

                    if (!validateRegEx(cachesRegEx))
                        throw new IllegalArgumentException("Invalid cache name regex: " + cachesRegEx);

                    break;

                case IDX_NAME:
                    indexesRegEx = argIterator.nextArg("Failed to read index name regex.");

                    if (!validateRegEx(indexesRegEx))
                        throw new IllegalArgumentException("Invalid index name regex: " + indexesRegEx);

                    break;

                default:
                    throw new IllegalArgumentException("Unknown argument: " + arg.argName());
            }
        }

        args = new Arguments(nodeId, groupsRegEx, cachesRegEx, indexesRegEx);
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Node id. */
        private UUID nodeId;

        /** Cache groups regex. */
        private String groupsRegEx;

        /** Cache names regex. */
        private String cachesRegEx;

        /** Indexes names regex. */
        private String indexesRegEx;

        /** */
        public Arguments(UUID nodeId, String groupsRegEx, String cachesRegEx, String indexesRegEx) {
            this.nodeId = nodeId;
            this.groupsRegEx = groupsRegEx;
            this.indexesRegEx = indexesRegEx;
            this.cachesRegEx = cachesRegEx;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Cache groups regex filter.
         */
        public String groups() {
            return groupsRegEx;
        }

        /**
         * @return Cache names regex filter.
         */
        public String cachesRegEx() {
            return cachesRegEx;
        }

        /**
         * @return Index names regex filter.
         */
        public String indexesRegEx() {
            return indexesRegEx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Arguments.class, this);
        }
    }

    /**
     * Prints indexes info.
     *
     * @param res Set of indexes info to print.
     * @param logger Logger to use.
     */
    private void printIndexes(Set<IndexListInfoContainer> res, Logger logger) {
        List<IndexListInfoContainer> sorted = new ArrayList<>(res);

        sorted.sort(IndexListInfoContainer.comparator());

        String prevGrpName = "";

        for (IndexListInfoContainer container : sorted) {
            if (!prevGrpName.equals(container.groupName())) {
                prevGrpName = container.groupName();

                logger.info("");
            }

            logger.info(container.toString());
        }

        logger.info("");
    }

    /**
     * @param regex Regex to validate
     * @return {@code True} if {@code regex} syntax is valid. {@code False} otherwise.
     */
    private boolean validateRegEx(String regex) {
        try {
            Pattern.compile(regex);

            return true;
        }
        catch (PatternSyntaxException e) {
            return false;
        }
    }
}
