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
import java.util.Comparator;
import java.util.Map;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommonArgParser.getCommonOptions;
import static org.apache.ignite.internal.commandline.CommandLogger.j;
import static org.apache.ignite.internal.commandline.CommandLogger.op;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.Commands.CACHE;
import static org.apache.ignite.internal.commandline.OutputFormat.MULTI_LINE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.CONTENTION;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.HELP;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.LIST;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CACHE_FILTER;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CHECK_CRC;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.EXCLUDE_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.CONFIG;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.GROUP;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.OUTPUT_FORMAT;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.SEQUENCE;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder.DELIM;

/**
 * High-level "cache" command realization.
 */
public class CacheCommands extends Command<CacheSubcommands> {
    /** One cache filter option should used message. */
    public static final String ONE_CACHE_FILTER_OPT_SHOULD_USED_MSG = "Should use only one of option: " +
        EXCLUDE_CACHES + ", " + CACHE_FILTER + " or pass caches explicitly"; //TODO

    /** */
    protected static final String NODE_ID = "nodeId";

    /** */
    protected static final String OP_NODE_ID = op(NODE_ID);

    /** */
    private CommandLogger logger;

    /** */
    private CacheSubcommands subcommand;

    /** {@inheritDoc} */
    @Override public void printUsage(CommandLogger logger) {
        logger.logWithIndent("View caches information in a cluster. For more details type:");
        logger.logWithIndent(j(" ", UTILITY_NAME, CACHE, HELP), 2);
        logger.nl();
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        this.logger = logger;

        if (subcommand == CacheSubcommands.HELP) {
            printCacheHelp();

            return null;
        }

        Command command = subcommand.subcommand();

        if (command == null)
            throw new IllegalStateException("Unknown command " + subcommand);

        return command.execute(clientCfg, logger);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            throw new IllegalArgumentException("Arguments are expected for --cache subcommand, " +
                "run '--cache help' for more info.");
        }

        String str = argIter.nextArg("").toLowerCase();

        CacheSubcommands cmd = CacheSubcommands.of(str);

        if (cmd == null)
            cmd = CacheSubcommands.HELP;

        switch (cmd) {
            case HELP:
                break;

            case RESET_LOST_PARTITIONS:
            case LIST:
            case IDLE_VERIFY:
            case VALIDATE_INDEXES:
            case FIND_AND_DELETE_GARBAGE:
            case CONTENTION:
            case DISTRIBUTION:
                cmd.subcommand().parseArguments(argIter);

                break;

            default:
                throw new IllegalArgumentException("Unknown --cache subcommand " + cmd);
        }

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected argument of --cache subcommand: " + argIter.peekNextArg());

        this.subcommand = cmd;
    }


    /** */
    private void printCacheHelp() {
        logger.logWithIndent("The '" + CACHE + " subcommand' is used to get information about and perform actions with caches. The command has the following syntax:");
        logger.nl();
        logger.logWithIndent(CommandLogger.j(" ", UTILITY_NAME, j(" ", getCommonOptions())) + " " + CACHE + " [subcommand] <subcommand_parameters>");
        logger.nl();
        logger.logWithIndent("The subcommands that take " + OP_NODE_ID + " as an argument ('" + LIST + "', '" + CONTENTION + "' and '" + VALIDATE_INDEXES + "') will be executed on the given node or on all server nodes if the option is not specified. Other commands will run on a random server node.");
        logger.nl();
        logger.nl();
        logger.logWithIndent("Subcommands:");

        Arrays.stream(CacheCommandList.values()).forEach(c -> c.subcommand().printUsage(logger));

        logger.nl();
    }


    /**
     * Print cache command usage with default indention.
     *
     * @param cmd Cache command.
     * @param args Cache command arguments.
     */
    protected static void usageCache(CommandLogger logger, CacheSubcommands cmd, String... args) {
        usageCache(logger, 1, cmd, args);
    }

    /**
     * Print cache command usage.
     *
     * @param indentsNum Number of indents.
     * @param cmd Cache command.
     * @param args Cache command arguments.
     */
    protected static void usageCache(CommandLogger logger, int indentsNum, CacheSubcommands cmd, String... args) {
        logger.logWithIndent(DELIM, indentsNum);
        logger.nl();
        logger.logWithIndent(j(" ", CACHE, cmd, j(" ", args)), indentsNum++);
        logger.nl();
        logger.logWithIndent(cmd.description(), indentsNum);
        logger.nl();

        Map<String, String> paramsDesc = createCacheArgsDesc(cmd);

        if (!paramsDesc.isEmpty()) {
            logger.logWithIndent("Parameters:", indentsNum);

            usageCacheParams(logger, paramsDesc, indentsNum + 1);

            logger.nl();
        }
    }

    /**
     * Print cache command arguments usage.
     *
     * @param paramsDesc Cache command arguments description.
     * @param indentsNum Number of indents.
     */
    private static void usageCacheParams(CommandLogger logger, Map<String, String> paramsDesc, int indentsNum) {
        int maxParamLen = paramsDesc.keySet().stream().max(Comparator.comparingInt(String::length)).get().length();

        for (Map.Entry<String, String> param : paramsDesc.entrySet())
            logger.logWithIndent(extendToLen(param.getKey(), maxParamLen) + "  " + "- " + param.getValue(), indentsNum);
    }

    /**
     * Appends spaces to end of input string for extending to needed length.
     *
     * @param s Input string.
     * @param targetLen Needed length.
     * @return String with appended spaces on the end.
     */
    private static String extendToLen(String s, int targetLen) {
        assert targetLen >= 0;
        assert s.length() <= targetLen;

        if (s.length() == targetLen)
            return s;

        SB sb = new SB(targetLen);

        sb.a(s);

        for (int i = 0; i < targetLen - s.length(); i++)
            sb.a(" ");

        return sb.toString();
    }


    /**
     * Gets cache command arguments description by cache command.
     *
     * @param cmd Cache command.
     * @return Cache command arguments description.
     */
    private static Map<String, String> createCacheArgsDesc(CacheSubcommands cmd) {
        Map<String, String> map = U.newLinkedHashMap(16);
        switch (cmd) {
            case LIST:
                map.put(CONFIG.toString(), "print all configuration parameters for each cache.");
                map.put(OUTPUT_FORMAT + " " + MULTI_LINE, "print configuration parameters per line. This option has effect only when used with " + CONFIG + " and without " + op(or(GROUP, SEQUENCE)) + ".");
                map.put(GROUP.toString(), "print information about groups.");
                map.put(SEQUENCE.toString(), "print information about sequences.");

                break;
            case VALIDATE_INDEXES:
                map.put(CHECK_FIRST + " N", "validate only the first N keys");
                map.put(CHECK_THROUGH + " K", "validate every Kth key");

                break;

            case IDLE_VERIFY:
                map.put(CHECK_CRC.toString(), "check the CRC-sum of pages stored on disk before verifying data consistency in partitions between primary and backup nodes.");

                break;
        }

        return map;
    }

    /** {@inheritDoc} */
    @Override public CacheSubcommands arg() {
        return subcommand;
    }
}
