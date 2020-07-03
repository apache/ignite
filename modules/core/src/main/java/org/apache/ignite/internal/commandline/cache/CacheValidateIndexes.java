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
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.IndexIntegrityCheckIssue;
import org.apache.ignite.internal.visor.verify.IndexValidationIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesCheckSizeIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesCheckSizeResult;
import org.apache.ignite.internal.visor.verify.ValidateIndexesPartitionResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskArg;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.join;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.IDLE_VERIFY;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.OP_NODE_ID;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CACHE_FILTER;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.EXCLUDE_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_CRC;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_SIZES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;

/**
 * Validate indexes command.
 */
public class CacheValidateIndexes implements Command<CacheValidateIndexes.Arguments> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String CACHES = "cacheName1,...,cacheNameN";
        String description = "Verify counters and hash sums of primary and backup partitions for the specified " +
            "caches/cache groups on an idle cluster and print out the differences, if any. " +
            "Cache filtering options configure the set of caches that will be processed by " + IDLE_VERIFY + " command. " +
            "Default value for the set of cache names (or cache group names) is all cache groups. Default value for " +
            EXCLUDE_CACHES + " is empty set. Default value for " + CACHE_FILTER + " is no filtering. Therefore, " +
            "the set of all caches is sequently filtered by cache name " +
            "regexps, by cache type and after all by exclude regexps.";

        Map<String, String> map = U.newLinkedHashMap(16);

        map.put(CHECK_FIRST + " N", "validate only the first N keys");
        map.put(CHECK_THROUGH + " K", "validate every Kth key");
        map.put(CHECK_CRC.toString(), "check the CRC-sum of pages stored on disk");
        map.put(CHECK_SIZES.toString(), "check that index size and cache size are the same");

        usageCache(
            logger,
            VALIDATE_INDEXES,
            description,
            map,
            optional(CACHES),
            OP_NODE_ID,
            optional(or(CHECK_FIRST + " N", CHECK_THROUGH + " K", CHECK_CRC, CHECK_SIZES))
        );
    }

    /**
     * Container for command arguments.
     */
    public class Arguments {
         /** Caches. */
        private final Set<String> caches;

        /** Node id. */
        private final UUID nodeId;

        /** Max number of entries to be checked. */
        private final int checkFirst;

        /** Number of entries to check through. */
        private final int checkThrough;

        /** Check CRC. */
        private final boolean checkCrc;

        /** Check that index size and cache size are same. */
        private final boolean checkSizes;

        /**
         * Constructor.
         *
         * @param caches Caches.
         * @param nodeId Node id.
         * @param checkFirst Max number of entries to be checked.
         * @param checkThrough Number of entries to check through.
         * @param checkCrc Check CRC.
         * @param checkSizes Check that index size and cache size are same.
         */
        public Arguments(
            Set<String> caches,
            UUID nodeId,
            int checkFirst,
            int checkThrough,
            boolean checkCrc,
            boolean checkSizes
        ) {
            this.caches = caches;
            this.nodeId = nodeId;
            this.checkFirst = checkFirst;
            this.checkThrough = checkThrough;
            this.checkCrc = checkCrc;
            this.checkSizes = checkSizes;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Max number of entries to be checked.
         */
        public int checkFirst() {
            return checkFirst;
        }

        /**
         * @return Number of entries to check through.
         */
        public int checkThrough() {
            return checkThrough;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Check CRC.
         */
        public boolean checkCrc() {
            return checkCrc;
        }

        /**
         * Returns whether to check that index size and cache size are same.
         *
         * @return {@code true} if need check that index size and cache size
         *      are same.
         */
        public boolean checkSizes() {
            return checkSizes;
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
        VisorValidateIndexesTaskArg taskArg = new VisorValidateIndexesTaskArg(
            args.caches(),
            args.nodeId() != null ? Collections.singleton(args.nodeId()) : null,
            args.checkFirst(),
            args.checkThrough(),
            args.checkCrc(),
            args.checkSizes()
        );

        try (GridClient client = Command.startClient(clientCfg)) {
            VisorValidateIndexesTaskResult taskRes = executeTaskByNameOnNode(
                client, "org.apache.ignite.internal.visor.verify.VisorValidateIndexesTask", taskArg, null, clientCfg);

            boolean errors = CommandLogger.printErrors(taskRes.exceptions(), "Index validation failed on nodes:", logger);

            for (Entry<UUID, VisorValidateIndexesJobResult> nodeEntry : taskRes.results().entrySet()) {
                VisorValidateIndexesJobResult jobRes = nodeEntry.getValue();

                if (!jobRes.hasIssues())
                    continue;

                errors = true;

                logger.info("Index issues found on node " + nodeEntry.getKey() + ":");

                for (IndexIntegrityCheckIssue is : jobRes.integrityCheckFailures())
                    logger.info(INDENT + is);

                for (Entry<PartitionKey, ValidateIndexesPartitionResult> e : jobRes.partitionResult().entrySet()) {
                    ValidateIndexesPartitionResult res = e.getValue();

                    if (!res.issues().isEmpty()) {
                        logger.info(INDENT + join(" ", e.getKey(), e.getValue()));

                        for (IndexValidationIssue is : res.issues())
                            logger.info(DOUBLE_INDENT + is);
                    }
                }

                for (Entry<String, ValidateIndexesPartitionResult> e : jobRes.indexResult().entrySet()) {
                    ValidateIndexesPartitionResult res = e.getValue();

                    if (!res.issues().isEmpty()) {
                        logger.info(INDENT + join(" ", "SQL Index", e.getKey(), e.getValue()));

                        for (IndexValidationIssue is : res.issues())
                            logger.info(DOUBLE_INDENT + is);
                    }
                }

                for (Entry<String, ValidateIndexesCheckSizeResult> e : jobRes.checkSizeResult().entrySet()) {
                    ValidateIndexesCheckSizeResult res = e.getValue();
                    Collection<ValidateIndexesCheckSizeIssue> issues = res.issues();

                    if (issues.isEmpty())
                        continue;

                    logger.info(INDENT + join(" ", "Size check", e.getKey(), res));

                    for (ValidateIndexesCheckSizeIssue issue : issues)
                        logger.info(DOUBLE_INDENT + issue);
                }
            }

            if (!errors)
                logger.info("no issues found.");
            else
                logger.severe("issues found (listed above).");

            logger.info("");

            return taskRes;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        int checkFirst = -1;
        int checkThrough = -1;
        UUID nodeId = null;
        Set<String> caches = null;
        boolean checkCrc = false;
        boolean checkSizes = false;

        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("");

            ValidateIndexesCommandArg arg = CommandArgUtils.of(nextArg, ValidateIndexesCommandArg.class);

            if (arg == CHECK_FIRST || arg == CHECK_THROUGH) {
                if (!argIter.hasNextSubArg())
                    throw new IllegalArgumentException("Numeric value for '" + nextArg + "' parameter expected.");

                int numVal;

                String numStr = argIter.nextArg("");

                try {
                    numVal = Integer.parseInt(numStr);
                }
                catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                        "Not numeric value was passed for '" + nextArg + "' parameter: " + numStr
                    );
                }

                if (numVal <= 0)
                    throw new IllegalArgumentException("Value for '" + nextArg + "' property should be positive.");

                if (arg == CHECK_FIRST)
                    checkFirst = numVal;
                else
                    checkThrough = numVal;

                continue;
            }
            else if (arg == CHECK_CRC) {
                checkCrc = true;
                continue;
            }
            else if (CHECK_SIZES == arg) {
                checkSizes = true;

                continue;
            }

            try {
                nodeId = UUID.fromString(nextArg);

                continue;
            }
            catch (IllegalArgumentException ignored) {
                //No-op.
            }

            caches = argIter.parseStringSet(nextArg);
        }

        args = new Arguments(caches, nodeId, checkFirst, checkThrough, checkCrc, checkSizes);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return VALIDATE_INDEXES.text().toUpperCase();
    }
}
