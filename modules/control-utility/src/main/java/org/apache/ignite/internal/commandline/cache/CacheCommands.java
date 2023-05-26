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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;

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
public class CacheCommands extends AbstractCommand<CacheSubcommands> {
    /** Empty group name. */
    public static final String EMPTY_GROUP_NAME = "no_group";

    /** */
    protected static final String NODE_ID = "nodeId";

    /** */
    protected static final String OP_NODE_ID = optional(NODE_ID);

    /** */
    private CacheSubcommands subcommand;

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        logger.info("");
        logger.info(INDENT + "View caches information in a cluster. For more details type:");
        logger.info(DOUBLE_INDENT + CommandLogger.join(" ", UTILITY_NAME, CACHE, HELP));
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
        if (subcommand == CacheSubcommands.HELP) {
            printCacheHelp(logger);

            return null;
        }

        Command command = subcommand.subcommand();

        if (command == null)
            throw new IllegalStateException("Unknown command " + subcommand);

        return command.execute(clientCfg, logger, verbose);
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

        if (cmd != HELP)
            cmd.subcommand().parseArguments(argIter);

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected argument of --cache subcommand: " + argIter.peekNextArg());

        subcommand = cmd;
    }

    /** */
    private void printCacheHelp(IgniteLogger logger) {
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

        Arrays.stream(CacheSubcommands.values()).forEach(c -> {
            if (c.subcommand() != null) c.subcommand().printUsage(logger);
        });

        logger.info("");
    }

    /** {@inheritDoc} */
    @Override public void prepareConfirmation(GridClientConfiguration clientCfg, IgniteLogger logger) throws Exception {
        if (subcommand != null && subcommand.subcommand() != null)
            subcommand.subcommand().prepareConfirmation(clientCfg, logger);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (subcommand != null && subcommand.subcommand() != null)
            return subcommand.subcommand().confirmationPrompt();

        return null;
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
