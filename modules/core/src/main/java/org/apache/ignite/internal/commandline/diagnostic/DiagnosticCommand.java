/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.diagnostic;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;

import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandList.DIAGNOSTIC;
import static org.apache.ignite.internal.commandline.CommandLogger.join;
import static org.apache.ignite.internal.commandline.diagnostic.DiagnosticSubCommand.HELP;
import static org.apache.ignite.internal.commandline.diagnostic.DiagnosticSubCommand.PAGE_LOCKS;

/**
 *
 */
public class DiagnosticCommand implements Command<DiagnosticSubCommand> {
    /**
     *
     */
    private DiagnosticSubCommand subcommand;

    /**
     *
     */
    private Logger logger;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        this.logger = logger;

        if (subcommand == HELP) {
            printDiagnosticHelp();

            return null;
        }

        Command command = subcommand.subcommand();

        if (command == null)
            throw new IllegalStateException("Unknown command " + subcommand);

        return command.execute(clientCfg, logger);
    }

    /** {@inheritDoc} */
    @Override public DiagnosticSubCommand arg() {
        return subcommand;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            subcommand = HELP;

            return;
        }

        String str = argIter.nextArg("").toLowerCase();

        DiagnosticSubCommand cmd = DiagnosticSubCommand.of(str);

        if (cmd == null)
            cmd = HELP;

        switch (cmd) {
            case HELP:
                break;

            case PAGE_LOCKS:
                cmd.subcommand().parseArguments(argIter);

                break;

            default:
                throw new IllegalArgumentException("Unknown diagnostic subcommand " + cmd);
        }

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected argument of diagnostic subcommand: " + argIter.peekNextArg());

        subcommand = cmd;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "Diagnostic command";
    }

    /** {@inheritDoc} */
    @Override public void printUsage() {
        logger.info("View diagnostic information in a cluster. For more details type:");
        logger.info(join(" ", UTILITY_NAME, DIAGNOSTIC, HELP));
        logger.info("\n");
    }

    /**
     *
     */
    private void printDiagnosticHelp() {
        logger.info(join(" ", UTILITY_NAME, DIAGNOSTIC, PAGE_LOCKS + " - dump page locks info."));
    }
}
