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

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksResult;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTask;
import org.apache.ignite.internal.visor.diagnostic.VisorPageLocksTrackerArgs;

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
    public static final String DUMP = "dump";
    /**
     *
     */
    public static final String DUMP_LOG = "dump_log";

    /**
     *
     */
    private Args args;

    /**
     *
     */
    private CommandLogger logger;

    /**
     *
     */
    private boolean help;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, CommandLogger logger) throws Exception {
        this.logger = logger;

        if (help) {
            help = false;

            printUsage(logger);

            return null;
        }

        Set<String> nodeIds = args.nodeIds;

        try (GridClient client = Command.startClient(clientCfg)) {
            if (args.allNodes) {
                client.compute().nodes().forEach(n -> {
                    nodeIds.add(String.valueOf(n.consistentId()));
                    nodeIds.add(n.nodeId().toString());
                });
            }
        }

        VisorPageLocksTrackerArgs taskArg = new VisorPageLocksTrackerArgs(args.op, args.type, args.filePath, nodeIds);

        Map<ClusterNode, VisorPageLocksResult> res;

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

            if (DUMP.equals(cmd) || DUMP_LOG.equals(cmd)) {
                boolean allNodes = false;
                String filePath = null;

                Set<String> nodeIds = new TreeSet<>();

                while (argIter.hasNextArg()){
                    String nextArg = argIter.nextArg("").toLowerCase();

                    if ("--all".equals(nextArg))
                        allNodes = true;
                    else if ("--nodes".equals(nextArg)) {
                        while (argIter.hasNextArg()){
                            nextArg = argIter.nextArg("").toLowerCase();

                            nodeIds.add(nextArg);
                        }
                    }
                    else {
                        if (new File(nextArg).isDirectory())
                            filePath = nextArg;
                    }
                }

                args = new Args(DUMP, cmd, filePath, allNodes, nodeIds);
            }
            else
                help = true;
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(CommandLogger logger) {
        logger.log("View pages locks state information on the node or nodes.");
        logger.log(CommandLogger.join(" ",
            UTILITY_NAME, DIAGNOSTIC, PAGE_LOCKS, DUMP,
            "[--path path_to_directory] [--all|--nodes nodeId1,nodeId2,..|--nodes consistentId1,consistentId2,..]",
            "// Save page locks dump to file generated in IGNITE_HOME/work directory."));
        logger.log(CommandLogger.join(" ",
            UTILITY_NAME, DIAGNOSTIC, PAGE_LOCKS, DUMP_LOG,
            "[--all|--nodes nodeId1,nodeId2,..|--nodes consistentId1,consistentId2,..]",
            "// Pring page locks dump to console on the node or nodes."));
        logger.nl();
    }

    /**
     * @param res Result.
     */
    private void printResult(Map<ClusterNode, VisorPageLocksResult> res) {
        res.forEach((n, res0) -> {
            logger.log(n.id() + " (" + n.consistentId() + ") " + res0.result());
        });
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
         *
         */
        private final boolean allNodes;
        /**
         *
         */
        private final Set<String> nodeIds;

        /**
         * @param op Operation.
         * @param type Type.
         * @param filePath File path.
         * @param nodeIds Node ids.
         */
        public Args(String op, String type, String filePath, boolean allNodes, Set<String> nodeIds) {
            this.op = op;
            this.type = type;
            this.filePath = filePath;
            this.allNodes = allNodes;
            this.nodeIds = nodeIds;
        }
    }
}
