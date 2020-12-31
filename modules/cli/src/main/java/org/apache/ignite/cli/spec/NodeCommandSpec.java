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
    @CommandLine.Command(name = "start", description = "Starts an Ignite node locally.")
    public static class StartNodeCommandSpec extends CommandSpec {

        @Inject private CliPathsConfigLoader cliPathsConfigLoader;

        @Inject private NodeManager nodeManager;

        @CommandLine.Parameters(paramLabel = "consistent-id", description = "Consistent ID of the new node")
        public String consistentId;

        @CommandLine.Option(names = "--config", description = "Configuration file to start the node with")
        public Path configPath;

        @Override public void run() {
            IgnitePaths ignitePaths = cliPathsConfigLoader.loadIgnitePathsOrThrowError();

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            out.println("Starting a new Ignite node...");

            NodeManager.RunningNode node = nodeManager.start(consistentId, ignitePaths.workDir,
                ignitePaths.cliPidsDir(),
                configPath,
                out);

            out.println();
            out.println("Node is successfully started. To stop, type " +
                cs.commandText("ignite node stop ") + cs.parameterText(node.consistentId));
            out.println();

            Table table = new Table(0, cs);

            table.addRow("@|bold Consistent ID|@", node.consistentId);
            table.addRow("@|bold PID|@", node.pid);
            table.addRow("@|bold Log File|@", node.logFile);

            out.println(table);
        }
    }

    @CommandLine.Command(name = "stop", description = "Stops a locally running Ignite node.")
    public static class StopNodeCommandSpec extends CommandSpec {

        @Inject private NodeManager nodeManager;
        @Inject private CliPathsConfigLoader cliPathsConfigLoader;

        @CommandLine.Parameters(
            arity = "1..*",
            paramLabel = "consistent-ids",
            description = "Consistent IDs of the nodes to stop (space separated list)"
        )
        public List<String> consistentIds;

        @Override public void run() {
            IgnitePaths ignitePaths = cliPathsConfigLoader.loadIgnitePathsOrThrowError();

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            consistentIds.forEach(p -> {
                out.print("Stopping locally running node with consistent ID " + cs.parameterText(p) + "... ");

                if (nodeManager.stopWait(p, ignitePaths.cliPidsDir()))
                    out.println(cs.text("@|bold,green Done!|@"));
                else
                    out.println(cs.text("@|bold,red Failed|@"));
            });
        }
    }

    @CommandLine.Command(name = "list", description = "Shows the list of currently running local Ignite nodes.")
    public static class ListNodesCommandSpec extends CommandSpec {

        @Inject private NodeManager nodeManager;
        @Inject private CliPathsConfigLoader cliPathsConfigLoader;

        @Override public void run() {
            IgnitePaths paths = cliPathsConfigLoader.loadIgnitePathsOrThrowError();

            List<NodeManager.RunningNode> nodes = nodeManager.getRunningNodes(paths.workDir, paths.cliPidsDir());

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

                Table table = new Table(0, cs);

                table.addRow("@|bold Consistent ID|@", "@|bold PID|@", "@|bold Log File|@");

                for (NodeManager.RunningNode node : nodes) {
                    table.addRow(node.consistentId, node.pid, node.logFile);
                }

                out.println(table);
            }
        }
    }

    @CommandLine.Command(name = "classpath", description = "Shows the current classpath used by the Ignite nodes.")
    public static class NodesClasspathCommandSpec extends CommandSpec {

        @Inject private NodeManager nodeManager;

        @Override public void run() {
            try {
                List<String> items = nodeManager.classpathItems();

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
