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

package org.apache.ignite.cli.spec;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;
import javax.inject.Inject;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.IgnitePaths;
import org.apache.ignite.cli.Table;
import org.apache.ignite.cli.builtins.node.NodeManager;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.ColorScheme;

/**
 * Commands for start/stop/list Ignite nodes on the current machine.
 */
@CommandLine.Command(
    name = "node",
    description = "Manages locally running Ignite nodes.",
    subcommands = {
        NodeCommandSpec.StartNodeCommandSpec.class,
        NodeCommandSpec.StopNodeCommandSpec.class,
        NodeCommandSpec.NodesClasspathCommandSpec.class,
        NodeCommandSpec.ListNodesCommandSpec.class
    }
)
public class NodeCommandSpec extends CategorySpec {
    /**
     * Starts Ignite node command.
     */
    @CommandLine.Command(name = "start", description = "Starts an Ignite node locally.")
    public static class StartNodeCommandSpec extends CommandSpec {

        /** Loader for Ignite distributive paths. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** Consistent id, which will be used by new node. */
        @CommandLine.Parameters(paramLabel = "consistent-id", description = "Consistent ID of the new node")
        public String consistentId;

        /** Path to node config. */
        @CommandLine.Option(names = "--config", description = "Configuration file to start the node with")
        private Path configPath;

        /** {@inheritDoc} */
        @Override public void run() {
            IgnitePaths ignitePaths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            out.println("Starting a new Ignite node...");

            NodeManager.RunningNode node = nodeMgr.start(consistentId, ignitePaths.workDir,
                ignitePaths.cliPidsDir(),
                configPath,
                out);

            out.println();
            out.println("Node is successfully started. To stop, type " +
                cs.commandText("ignite node stop ") + cs.parameterText(node.consistentId));
            out.println();

            Table tbl = new Table(0, cs);

            tbl.addRow("@|bold Consistent ID|@", node.consistentId);
            tbl.addRow("@|bold PID|@", node.pid);
            tbl.addRow("@|bold Log File|@", node.logFile);

            out.println(tbl);
        }
    }

    /**
     * Command for stopping Ignite node on the current machine.
     */
    @CommandLine.Command(name = "stop", description = "Stops a locally running Ignite node.")
    public static class StopNodeCommandSpec extends CommandSpec {
        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** Loader for Ignite distributive paths. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** Consistent ids of nodes to stop. */
        @CommandLine.Parameters(
            arity = "1..*",
            paramLabel = "consistent-ids",
            description = "Consistent IDs of the nodes to stop (space separated list)"
        )
        private List<String> consistentIds;

        /** {@inheritDoc} */
        @Override public void run() {
            IgnitePaths ignitePaths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            consistentIds.forEach(p -> {
                out.print("Stopping locally running node with consistent ID " + cs.parameterText(p) + "... ");

                if (nodeMgr.stopWait(p, ignitePaths.cliPidsDir()))
                    out.println(cs.text("@|bold,green Done!|@"));
                else
                    out.println(cs.text("@|bold,red Failed|@"));
            });
        }
    }

    /**
     * Command for listing the running nodes.
     */
    @CommandLine.Command(name = "list", description = "Shows the list of currently running local Ignite nodes.")
    public static class ListNodesCommandSpec extends CommandSpec {
        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** Loader for Ignite distributive paths. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** {@inheritDoc} */
        @Override public void run() {
            IgnitePaths paths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            List<NodeManager.RunningNode> nodes = nodeMgr.getRunningNodes(paths.workDir, paths.cliPidsDir());

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            if (nodes.isEmpty()) {
                out.println("Currently, there are no locally running nodes.");
                out.println();
                out.println("Use the " + cs.commandText("ignite node start")
                    + " command to start a new node.");
            }
            else {
                out.println("Currently, there are " +
                    cs.text("@|bold " + nodes.size() + "|@") + " locally running nodes.");
                out.println();

                Table tbl = new Table(0, cs);

                tbl.addRow("@|bold Consistent ID|@", "@|bold PID|@", "@|bold Log File|@");

                for (NodeManager.RunningNode node : nodes) {
                    tbl.addRow(node.consistentId, node.pid, node.logFile);
                }

                out.println(tbl);
            }
        }
    }

    /**
     * Command for reading the current classpath of Ignite nodes.
     */
    @CommandLine.Command(name = "classpath", description = "Shows the current classpath used by the Ignite nodes.")
    public static class NodesClasspathCommandSpec extends CommandSpec {
        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                List<String> items = nodeMgr.classpathItems();

                PrintWriter out = spec.commandLine().getOut();

                out.println(Ansi.AUTO.string("@|bold Current Ignite node classpath:|@"));

                for (String item : items) {
                    out.println("    " + item);
                }
            }
            catch (IOException e) {
                throw new IgniteCLIException("Can't get current classpath", e);
            }
        }
    }

}
