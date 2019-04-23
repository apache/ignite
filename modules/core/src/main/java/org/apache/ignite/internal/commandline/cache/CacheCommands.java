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

import java.util.Comparator;
import java.util.Map;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;

import static org.apache.ignite.internal.commandline.CommandArgParser.getCommonOptions;
import static org.apache.ignite.internal.commandline.CommandLogger.j;
import static org.apache.ignite.internal.commandline.CommandLogger.op;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.Commands.CACHE;
import static org.apache.ignite.internal.commandline.OutputFormat.MULTI_LINE;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.CONTENTION;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.DISTRIBUTION;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.IDLE_VERIFY;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.LIST;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.RESET_LOST_PARTITIONS;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.DistributionCommandArg.USER_ATTRIBUTES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CACHE_FILTER;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.CHECK_CRC;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.DUMP;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.EXCLUDE_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.IdleVerifyCommandArg.SKIP_ZEROS;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.CONFIG;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.GROUP;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.OUTPUT_FORMAT;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.SEQUENCE;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder.DELIM;

public class CacheCommands extends Command<CacheCommandList> {
    /** */
    private static final String NODE_ID = "nodeId";

    /** */
    private static final String OP_NODE_ID = op(NODE_ID);

    private CommandLogger logger;

    /** */
    private CacheCommandList subcommand;

    /**
     * Executes --cache subcommand.
     *
     * @param clientCfg Client configuration.
     */

    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        this.logger = logger;

        if (subcommand == CacheCommandList.HELP) {
            printCacheHelp();

            return null;
        }

        Command command = subcommand.subcommand();

        if (command == null)
            throw new IllegalStateException("Unknown command " + subcommand);

        return command.execute(clientCfg, logger);
    }

    /**
     * Parses and validates cache arguments.
     *
     * @return --cache subcommand arguments in case validation is successful.
     * @param argIter Argument iterator.
     */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            throw new IllegalArgumentException("Arguments are expected for --cache subcommand, " +
                "run '--cache help' for more info.");
        }

        String str = argIter.nextArg("").toLowerCase();

        CacheCommandList cmd = CacheCommandList.of(str);

        if (cmd == null)
            cmd = CacheCommandList.HELP;


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
        logger.logWithIndent(CommandLogger.j(" ", CommandHandler.UTILITY_NAME, j(" ", getCommonOptions())) + " " + CACHE + " [subcommand] <subcommand_parameters>");
        logger.nl();
        logger.logWithIndent("The subcommands that take " + OP_NODE_ID + " as an argument ('" + LIST + "', '" + CONTENTION + "' and '" + VALIDATE_INDEXES + "') will be executed on the given node or on all server nodes if the option is not specified. Other commands will run on a random server node.");
        logger.nl();
        logger.nl();
        logger.logWithIndent("Subcommands:");

        String CACHES = "cacheName1,...,cacheNameN";

        String GROUPS = "groupName1,...,groupNameN";

        usageCache(LIST, "regexPattern", op(or(GROUP, SEQUENCE)), OP_NODE_ID, op(CONFIG), op(OUTPUT_FORMAT, MULTI_LINE));
        usageCache(CONTENTION, "minQueueSize", OP_NODE_ID, op("maxPrint"));
        usageCache(IDLE_VERIFY, op(DUMP), op(SKIP_ZEROS), op(CHECK_CRC),
            op(EXCLUDE_CACHES, CACHES), op(CACHE_FILTER, or(CacheFilterEnum.values())), op(CACHES));
        usageCache(VALIDATE_INDEXES, op(CACHES), OP_NODE_ID, op(or(CHECK_FIRST + " N", CHECK_THROUGH + " K")));
        usageCache(DISTRIBUTION, or(NODE_ID, CommandHandler.NULL), op(CACHES), op(USER_ATTRIBUTES, "attrName1,...,attrNameN"));
        usageCache(RESET_LOST_PARTITIONS, CACHES);
        usageCache(FIND_AND_DELETE_GARBAGE, op(GROUPS), OP_NODE_ID, op(FindAndDeleteGarbageArg.DELETE));

        logger.nl();
    }


    /**
     * Print cache command usage with default indention.
     *
     * @param cmd Cache command.
     * @param args Cache command arguments.
     */
    private void usageCache(CacheCommandList cmd, String... args) {
        usageCache(1, cmd, args);
    }

    /**
     * Print cache command usage.
     *
     * @param indentsNum Number of indents.
     * @param cmd Cache command.
     * @param args Cache command arguments.
     */
    private void usageCache(int indentsNum, CacheCommandList cmd, String... args) {
        logger.logWithIndent(DELIM, indentsNum);
        logger.nl();
        logger.logWithIndent(j(" ", CACHE, cmd, j(" ", args)), indentsNum++);
        logger.nl();
        logger.logWithIndent(cmd.description(), indentsNum);
        logger.nl();

        Map<String, String> paramsDesc = createCacheArgsDesc(cmd);

        if (!paramsDesc.isEmpty()) {
            logger.logWithIndent("Parameters:", indentsNum);

            usageCacheParams(paramsDesc, indentsNum + 1);

            logger.nl();
        }
    }

    /**
     * Print cache command arguments usage.
     *
     * @param paramsDesc Cache command arguments description.
     * @param indentsNum Number of indents.
     */
    private void usageCacheParams(Map<String, String> paramsDesc, int indentsNum) {
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
    private String extendToLen(String s, int targetLen) {
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
    private Map<String, String> createCacheArgsDesc(CacheCommandList cmd) {
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

    @Override public CacheCommandList arg() {
        return subcommand;
    }
}
