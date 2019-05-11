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

import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTrackerArgs;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTrackerResult;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTrackerTask;

/**
 *
 */
public class PageLocksTrackerCommand implements Command<PageLocksTrackerCommand.Args> {
    /** */
    private Args args;

    /** */
    private CommandLogger logger;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        this.logger = logger;

        VisorPageLocksTrackerArgs taskArg = new VisorPageLocksTrackerArgs(args.op, args.type, args.filePath);

        VisorPageLocksTrackerResult res;

        try (GridClient client = Command.startClient(clientCfg)) {
            res = TaskExecutor.executeTask(
                client,
                VisorPageLocksTrackerTask.class,
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

            if ("dump".equals(cmd)) {
                String type = null;
                String filePath = null;

                if (argIter.hasNextSubArg()) {
                    type = argIter.nextArg("").toLowerCase();

                    if (!("log".equals(type) || "file".equals(type)))
                        throw new IllegalArgumentException("Unsupported dump operation type:" + type);
                }

                if (argIter.hasNextSubArg())
                    filePath = argIter.nextArg("").toLowerCase();

                args = new Args("dump", type, filePath);
            }
            else
                args = new Args(cmd, null, null);

        }
        else
            args = new Args("status", null, null);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(CommandLogger logger) {

    }

    /**
     * @param res Result.
     */
    private void printResult(VisorPageLocksTrackerResult res) {
        logger.log(res.result());
    }

    /** */
    public static class Args {
        /** */
        private final String op;
        /** */
        private final String type;
        /** */
        private final String filePath;

        /**
         * @param op
         * @param type
         * @param filePath
         */
        public Args(String op, String type, String filePath) {
            this.op = op;
            this.type = type;
            this.filePath = filePath;
        }
    }
}
