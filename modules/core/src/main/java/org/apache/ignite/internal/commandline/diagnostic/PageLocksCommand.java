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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.visor.diagnostic.Operation;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksResult;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTask;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTrackerArgs;

import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandList.DIAGNOSTIC;
import static org.apache.ignite.internal.commandline.CommandLogger.join;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.diagnostic.DiagnosticSubCommand.PAGE_LOCKS;
import static org.apache.ignite.internal.commandline.diagnostic.PageLocksCommand.PageLocksCommandArg.ALL;
import static org.apache.ignite.internal.commandline.diagnostic.PageLocksCommand.PageLocksCommandArg.DUMP;
import static org.apache.ignite.internal.commandline.diagnostic.PageLocksCommand.PageLocksCommandArg.DUMP_LOG;
import static org.apache.ignite.internal.commandline.diagnostic.PageLocksCommand.PageLocksCommandArg.NODES;
import static org.apache.ignite.internal.commandline.diagnostic.PageLocksCommand.PageLocksCommandArg.PATH;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;

/**
 *
 */
public class PageLocksCommand implements Command<PageLocksCommand.Arguments> {
    /** */
    private Arguments arguments;

    /** */
    private Logger logger;


    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        this.logger = logger;

        Set<String> nodeIds = arguments.nodeIds;

        Map<ClusterNode, VisorPageLocksResult> res;

        try (GridClient client = Command.startClient(clientCfg)) {
            if (arguments.allNodes) {
                client.compute().nodes().forEach(n -> {
                    nodeIds.add(String.valueOf(n.consistentId()));
                    nodeIds.add(n.nodeId().toString());
                });
            }

            VisorPageLocksTrackerArgs taskArg = new VisorPageLocksTrackerArgs(arguments.op, arguments.filePath, nodeIds);

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
    @Override public Arguments arg() {
        return arguments;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Operation op = Operation.DUMP_LOG;

        String path = null;
        boolean allNodes = false;
        Set<String> nodeIds = new HashSet<>();

        while (argIter.hasNextArg()) {
            String nextArg = argIter.nextArg("");

            PageLocksCommandArg arg = CommandArgUtils.of(nextArg, PageLocksCommandArg.class);

            if (arg == null)
                break;

            switch (arg) {
                case DUMP:
                    op = Operation.DUMP_FILE;

                    break;
                case DUMP_LOG:
                    op = Operation.DUMP_LOG;

                    break;
                case ALL:
                    allNodes = true;

                    break;
                case NODES:
                    nodeIds.addAll(argIter.nextStringSet(""));

                    break;
                case PATH:
                    path = argIter.nextArg("");

                    break;
                default:
                    throw new IllegalArgumentException(
                        "Unexpected argumetn:" + arg + ", supported:" + Arrays.toString(PageLocksCommandArg.values())
                    );
            }
        }

        arguments = new Arguments(op, path, allNodes, nodeIds);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        logger.info("View pages locks state information on the node or nodes.");
        logger.info(join(" ",
            UTILITY_NAME, DIAGNOSTIC, PAGE_LOCKS, DUMP,
            optional(PATH, "path_to_directory"),
            optional(ALL),
            optional(CommandLogger.or(NODES, "nodeId1,nodeId2,..")),
            optional(CommandLogger.or(NODES, "consistentId1,consistentId2,..")),
            "// Save page locks dump to file generated in IGNITE_HOME" +
                File.separatorChar + "work" + File.separatorChar + DEFAULT_TARGET_FOLDER + " directory."));
        logger.info(join(" ",
            UTILITY_NAME, DIAGNOSTIC, PAGE_LOCKS, DUMP_LOG,
            optional(ALL),
            optional(CommandLogger.or(NODES, "nodeId1,nodeId2,..")),
            optional(CommandLogger.or(NODES, "consistentId1,consistentId2,..")),
            "// Pring page locks dump to console on the node or nodes."));
        logger.info("");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PAGE_LOCKS.toString();
    }

    /**
     * @param res Result.
     */
    private void printResult(Map<ClusterNode, VisorPageLocksResult> res) {
        res.forEach((n, res0) -> {
            logger.info(n.id() + " (" + n.consistentId() + ") " + res0.result());
        });
    }

    /** */
    public static class Arguments {
        /** */
        private final Operation op;

        /** */
        private final String filePath;

        /** */
        private final boolean allNodes;

        /** */
        private final Set<String> nodeIds;

        /**
         * @param op Operation.
         * @param filePath File path.
         * @param allNodes If {@code True} include all available nodes for command. If {@code False} include only subset.
         * @param nodeIds Node ids.
         */
        public Arguments(
            Operation op,
            String filePath,
            boolean allNodes,
            Set<String> nodeIds
        ) {
            this.op = op;
            this.filePath = filePath;
            this.allNodes = allNodes;
            this.nodeIds = nodeIds;
        }
    }

    enum PageLocksCommandArg implements CommandArg {
        /** */
        DUMP("dump"),

        /** */
        DUMP_LOG("dump_log"),

        /** */
        PATH("--path"),

        /** */
        NODES("--nodes"),

        /** */
        ALL("--all");

        /** Option name. */
        private final String name;

        /** */
        PageLocksCommandArg(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String argName() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return name;
        }
    }
}
