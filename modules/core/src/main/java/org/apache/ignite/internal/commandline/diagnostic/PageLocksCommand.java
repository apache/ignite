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

package org.apache.ignite.internal.commandline.diagnostic;

import java.io.File;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksResult;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTrackerArgs;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTask;

import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandList.DIAGNOSTIC;
import static org.apache.ignite.internal.commandline.diagnostic.DiagnosticSubCommand.PAGE_LOCKS;

/**
 *
 */
public class PageLocksCommand implements Command<PageLocksCommand.Args> {
    /**
     *
     */
    public static final String COMMAND = "dump";

    /**
     *
     */
    private Args args;

    /**
     *
     */
    private CommandLogger logger;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        this.logger = logger;

        VisorPageLocksTrackerArgs taskArg = new VisorPageLocksTrackerArgs(args.op, args.type, args.filePath);

        VisorPageLocksResult res;

        try (GridClient client = Command.startClient(clientCfg)) {
            res = TaskExecutor.executeTask(
                client,
                VisorPageLocksTask.class,
                taskArg,
                clientCfg
            );
        }

        printResult(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Args arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (argIter.hasNextSubArg()) {
            String cmd = argIter.nextArg("").toLowerCase();

            if (COMMAND.equals(cmd)) {
                String type = null;
                String filePath = null;

                if (argIter.hasNextSubArg()) {
                    String nextArg = argIter.nextArg("").toLowerCase();

                    if ("log".equals(nextArg))
                        type = nextArg;
                    else if (new File(nextArg).isDirectory())
                        filePath = nextArg;
                }

                args = new Args(COMMAND, type, filePath);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(CommandLogger logger) {
        logger.log("View pages locks state information in a node");
        logger.log(CommandLogger.join(" ",
            UTILITY_NAME, DIAGNOSTIC, PAGE_LOCKS, COMMAND, "// Save page locks dump to file generated in IGNITE_HOME directory."));
        logger.log(CommandLogger.join(" ",
            UTILITY_NAME, DIAGNOSTIC, PAGE_LOCKS, COMMAND + " log", "// Pring page locks dump to console on node"));
        logger.log(CommandLogger.join(" ",
            UTILITY_NAME, DIAGNOSTIC, PAGE_LOCKS, COMMAND + " {path}", "// Save page locks dump to specific path."));
        logger.nl();
    }

    /**
     * @param res Result.
     */
    private void printResult(VisorPageLocksResult res) {
        logger.log(res.result());
    }

    /**
     *
     */
    public static class Args {
        /**
         *
         */
        private final String op;
        /**
         *
         */
        private final String type;
        /**
         *
         */
        private final String filePath;

        /**
         * @param op Operation.
         * @param type Type.
         * @param filePath File path.
         */
        public Args(String op, String type, String filePath) {
            this.op = op;
            this.type = type;
            this.filePath = filePath;
        }
    }
}
