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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTask;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTask;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskResult;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskV2;
import org.apache.ignite.lang.IgniteProductVersion;

import static java.lang.String.format;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.IDLE_VERIFY;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CACHE_FILTER;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CHECK_CRC;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.DUMP;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.EXCLUDE_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.SKIP_ZEROS;

/**
 *
 */
public class IdleVerify implements Command<IdleVerify.Arguments> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String CACHES = "cacheName1,...,cacheNameN";
        String description = "Verify counters and hash sums of primary and backup partitions for the specified " +
            "caches/cache groups on an idle cluster and print out the differences, if any. " +
            "Cache filtering options configure the set of caches that will be processed by idle_verify command. " +
            "Default value for the set of cache names (or cache group names) is all cache groups. Default value" +
            " for " + EXCLUDE_CACHES + " is empty set. " +
            "Default value for " + CACHE_FILTER + " is no filtering. Therefore, the set of all caches is sequently " +
            "filtered by cache name " +
            "regexps, by cache type and after all by exclude regexps.";

        usageCache(
            logger,
            IDLE_VERIFY,
            description,
            Collections.singletonMap(CHECK_CRC.toString(),
                "check the CRC-sum of pages stored on disk before verifying data " +
                    "consistency in partitions between primary and backup nodes."),
            optional(DUMP), optional(SKIP_ZEROS), optional(CHECK_CRC), optional(EXCLUDE_CACHES, CACHES),
                optional(CACHE_FILTER, or(CacheFilterEnum.values())), optional(CACHES));
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Caches. */
        private Set<String> caches;

        /** Exclude caches or groups. */
        private Set<String> excludeCaches;

        /** Calculate partition hash and print into standard output. */
        private boolean dump;

        /** Skip zeros partitions. */
        private boolean skipZeros;

        /** Check CRC sum on idle verify. */
        private boolean idleCheckCrc;

        /** Cache filter. */
        private CacheFilterEnum cacheFilterEnum;

        /**
         *
         */
        public Arguments(Set<String> caches, Set<String> excludeCaches, boolean dump, boolean skipZeros,
            boolean idleCheckCrc,
            CacheFilterEnum cacheFilterEnum) {
            this.caches = caches;
            this.excludeCaches = excludeCaches;
            this.dump = dump;
            this.skipZeros = skipZeros;
            this.idleCheckCrc = idleCheckCrc;
            this.cacheFilterEnum = cacheFilterEnum;
        }

        /**
         * @return Gets filter of caches, which will by checked.
         */
        public CacheFilterEnum getCacheFilterEnum() {
            return cacheFilterEnum;
        }

        /**
         * @return Caches.
         */
        public Set<String> caches() {
            return caches;
        }

        /**
         * @return Exclude caches or groups.
         */
        public Set<String> excludeCaches() {
            return excludeCaches;
        }

        /**
         * @return Calculate partition hash and print into standard output.
         */
        public boolean dump() {
            return dump;
        }

        /**
         * @return Check page CRC sum on idle verify flag.
         */
        public boolean idleCheckCrc() {
            return idleCheckCrc;
        }

        /**
         * @return Skip zeros partitions(size == 0) in result.
         */
        public boolean isSkipZeros() {
            return skipZeros;
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
        try (GridClient client = Command.startClient(clientCfg)) {
            Collection<GridClientNode> nodes = client.compute().nodes(GridClientNode::connectable);

            boolean idleVerifyV2 = true;

            for (GridClientNode node : nodes) {
                String nodeVerStr = node.attribute(IgniteNodeAttributes.ATTR_BUILD_VER);

                IgniteProductVersion nodeVer = IgniteProductVersion.fromString(nodeVerStr);

                if (nodeVer.compareTo(VerifyBackupPartitionsTaskV2.V2_SINCE_VER) < 0) {
                    idleVerifyV2 = false;

                    break;
                }
            }

            if (args.dump())
                cacheIdleVerifyDump(client, clientCfg, logger);
            else if (idleVerifyV2)
                cacheIdleVerifyV2(client, clientCfg);
            else
                legacyCacheIdleVerify(client, clientCfg, logger);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> cacheNames = null;
        boolean dump = false;
        boolean skipZeros = false;
        boolean idleCheckCrc = false;
        CacheFilterEnum cacheFilterEnum = CacheFilterEnum.DEFAULT;
        Set<String> excludeCaches = null;

        int idleVerifyArgsCnt = 5;

        while (argIter.hasNextSubArg() && idleVerifyArgsCnt-- > 0) {
            String nextArg = argIter.nextArg("");

            IdleVerifyCommandArg arg = CommandArgUtils.of(nextArg, IdleVerifyCommandArg.class);

            if (arg == null) {
                cacheNames = argIter.parseStringSet(nextArg);

                validateRegexes(cacheNames);
            }
            else {
                switch (arg) {
                    case DUMP:
                        dump = true;

                        break;

                    case SKIP_ZEROS:
                        skipZeros = true;

                        break;

                    case CHECK_CRC:
                        idleCheckCrc = true;

                        break;

                    case CACHE_FILTER:
                        String filter = argIter.nextArg("The cache filter should be specified. The following " +
                            "values can be used: " + Arrays.toString(CacheFilterEnum.values()) + '.');

                        cacheFilterEnum = CacheFilterEnum.valueOf(filter.toUpperCase());

                        break;

                    case EXCLUDE_CACHES:
                        excludeCaches = argIter.nextStringSet("caches, which will be excluded.");

                        validateRegexes(excludeCaches);

                        break;
                }
            }
        }

        args = new Arguments(cacheNames, excludeCaches, dump, skipZeros, idleCheckCrc, cacheFilterEnum);
    }

    /**
     * @param string To validate that given name is valed regex.
     */
    private void validateRegexes(Set<String> string) {
        string.forEach(s -> {
            try {
                Pattern.compile(s);
            }
            catch (PatternSyntaxException e) {
                throw new IgniteException(format("Invalid cache name regexp '%s': %s", s, e.getMessage()));
            }
        });
    }

    /**
     * @param client Client.
     * @param clientCfg Client configuration.
     */
    private void cacheIdleVerifyDump(
        GridClient client,
        GridClientConfiguration clientCfg,
        Logger logger
    ) throws GridClientException {
        VisorIdleVerifyDumpTaskArg arg = new VisorIdleVerifyDumpTaskArg(
            args.caches(),
            args.excludeCaches(),
            args.isSkipZeros(),
            args.getCacheFilterEnum(),
            args.idleCheckCrc()
        );

        String path = executeTask(client, VisorIdleVerifyDumpTask.class, arg, clientCfg);

        logger.info("VisorIdleVerifyDumpTask successfully written output to '" + path + "'");
        logParsedArgs(arg, logger::info);
    }

    /**
     * @param client Client.
     * @param clientCfg Client configuration.
     */
    private void cacheIdleVerifyV2(
        GridClient client,
        GridClientConfiguration clientCfg
    ) throws GridClientException {
        VisorIdleVerifyTaskArg taskArg = new VisorIdleVerifyTaskArg(
            args.caches(),
            args.excludeCaches(),
            args.isSkipZeros(),
            args.getCacheFilterEnum(),
            args.idleCheckCrc()
        );

        IdleVerifyResultV2 res = executeTask(client, VisorIdleVerifyTaskV2.class, taskArg, clientCfg);

        logParsedArgs(taskArg, System.out::print);

        res.print(System.out::print);
    }

    /**
     * Passes idle_verify parsed arguments to given log consumer.
     *
     * @param args idle_verify arguments.
     * @param logConsumer Logger.
     */
    public static void logParsedArgs(VisorIdleVerifyTaskArg args, Consumer<String> logConsumer) {
        SB options = new SB("idle_verify task was executed with the following args: ");

        options
            .a("caches=[")
            .a(args.caches() == null ? "" : String.join(", ", args.caches()))
            .a("], excluded=[")
            .a(args.excludeCaches() == null ? "" : String.join(", ", args.excludeCaches()))
            .a("]")
            .a(", cacheFilter=[")
            .a(args.cacheFilterEnum().toString())
            .a("]\n");

        logConsumer.accept(options.toString());
    }

    /**
     * @param client Client.
     * @param clientCfg Client configuration.
     */
    private void legacyCacheIdleVerify(
        GridClient client,
        GridClientConfiguration clientCfg,
        Logger logger
    ) throws GridClientException {
        VisorIdleVerifyTaskResult res = executeTask(
            client,
            VisorIdleVerifyTask.class,
            new VisorIdleVerifyTaskArg(
                args.caches(),
                args.excludeCaches(),
                args.isSkipZeros(),
                args.getCacheFilterEnum(),
                args.idleCheckCrc()
            ),
            clientCfg);

        Map<PartitionKey, List<PartitionHashRecord>> conflicts = res.getConflicts();

        if (conflicts.isEmpty()) {
            logger.info("idle_verify check has finished, no conflicts have been found.");
            logger.info("");
        }
        else {
            logger.info("idle_verify check has finished, found " + conflicts.size() + " conflict partitions.");
            logger.info("");

            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : conflicts.entrySet()) {
                logger.info("Conflict partition: " + entry.getKey());

                logger.info("Partition instances: " + entry.getValue());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return IDLE_VERIFY.text().toUpperCase();
    }
}
