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
import java.util.Map;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.commandline.Command.usageParams;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.getCommonOptions;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.CONTENTION;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.HELP;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.LIST;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;

/**
 * High-level "cache" command implementation.
 */
public class CacheCommands implements Command<CacheSubcommands> {
    /** */
    protected static final String NODE_ID = "nodeId";

    /** */
    protected static final String OP_NODE_ID = optional(NODE_ID);

    /** */
    private CacheSubcommands subcommand;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        logger.info("");
        logger.info(INDENT + "View caches information in a cluster. For more details type:");
        logger.info(DOUBLE_INDENT + CommandLogger.join(" ", UTILITY_NAME, CACHE, HELP));
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        if (subcommand == CacheSubcommands.HELP) {
            printCacheHelp(logger);

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
            case CHECK_INDEX_INLINE_SIZES:
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
    private void printCacheHelp(Logger logger) {
        logger.info(INDENT + "The '" + CACHE + " subcommand' is used to get information about and perform actions" +
            " with caches. The command has the following syntax:");
        logger.info("");
        logger.info(INDENT + CommandLogger.join(" ", UTILITY_NAME, CommandLogger.join(" ", getCommonOptions())) + " " +
            CACHE + " [subcommand] <subcommand_parameters>");
        logger.info("");
        logger.info(INDENT + "The subcommands that take " + OP_NODE_ID + " as an argument ('" + LIST + "', '"
            + FIND_AND_DELETE_GARBAGE + "', '" + CONTENTION + "' and '" + VALIDATE_INDEXES +
            "') will be executed on the given node or on all server nodes" +
            " if the option is not specified. Other commands will run on a random server node.");
        logger.info("");
        logger.info("");
        logger.info(INDENT + "Subcommands:");

        Arrays.stream(CacheCommandList.values()).forEach(c -> {
            if (c.subcommand() != null) c.subcommand().printUsage(logger);
        });

        logger.info("");
    }

    /**
     * Print cache command usage with default indention.
     *
     * @param logger Logger to use.
     * @param cmd Cache command.
     * @param description Command description.
     * @param paramsDesc Parameter desciptors.
     * @param args Cache command arguments.
     */
    protected static void usageCache(
        Logger logger,
        CacheSubcommands cmd,
        String description,
        Map<String, String> paramsDesc,
        String... args
    ) {
        logger.info("");
        logger.info(INDENT + CommandLogger.join(" ", CACHE, cmd, CommandLogger.join(" ", args)));
        logger.info(DOUBLE_INDENT + description);

        if (!F.isEmpty(paramsDesc)) {
            logger.info("");
            logger.info(DOUBLE_INDENT + "Parameters:");

            usageParams(paramsDesc, DOUBLE_INDENT + INDENT, logger);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheSubcommands arg() {
        return subcommand;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CACHE.toCommandName();
    }
}
