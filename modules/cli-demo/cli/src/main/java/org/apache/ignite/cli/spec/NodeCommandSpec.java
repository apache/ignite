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
import java.nio.file.Path;
import java.util.List;
import javax.inject.Inject;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.IgnitePaths;
import org.apache.ignite.cli.Table;
import org.apache.ignite.cli.builtins.node.NodeManager;
import picocli.CommandLine;

@CommandLine.Command(
    name = "node",
    description = "Start, stop and manage locally running Ignite nodes.",
    subcommands = {
        NodeCommandSpec.StartNodeCommandSpec.class,
        NodeCommandSpec.StopNodeCommandSpec.class,
        NodeCommandSpec.NodesClasspathCommandSpec.class,
        NodeCommandSpec.ListNodesCommandSpec.class
    }
)
public class NodeCommandSpec extends CategorySpec {
    @CommandLine.Command(name = "start", description = "Start an Ignite node locally.")
    public static class StartNodeCommandSpec extends CommandSpec {

        @Inject private CliPathsConfigLoader cliPathsConfigLoader;

        @Inject private NodeManager nodeManager;

        @CommandLine.Parameters(paramLabel = "consistent-id", description = "ConsistentId for new node")
        public String consistentId;

        @CommandLine.Option(names = {"--config"},
            description = "path to configuration file")
        public Path configPath;

        @Override public void run() {
            IgnitePaths ignitePaths = cliPathsConfigLoader.loadIgnitePathsOrThrowError();

            NodeManager.RunningNode node = nodeManager.start(consistentId, ignitePaths.workDir,
                ignitePaths.cliPidsDir(),
                configPath);

            spec.commandLine().getOut().println("Started ignite node.\nPID: " + node.pid +
                "\nConsistent Id: " + node.consistentId + "\nLog file: " + node.logFile);
        }
    }

    @CommandLine.Command(name = "stop", description = "Stop a locally running Ignite node.")
    public static class StopNodeCommandSpec extends CommandSpec {

        @Inject private NodeManager nodeManager;
        @Inject private CliPathsConfigLoader cliPathsConfigLoader;

        @CommandLine.Parameters(arity = "1..*", paramLabel = "consistent-ids",
            description = "consistent ids of nodes to start")
        public List<String> consistentIds;

        @Override public void run() {
            IgnitePaths ignitePaths = cliPathsConfigLoader.loadIgnitePathsOrThrowError();

            consistentIds.forEach(p -> {
                if (nodeManager.stopWait(p, ignitePaths.cliPidsDir()))
                    spec.commandLine().getOut().println("Node with consistent id " + p + " was stopped");
                else
                    spec.commandLine().getOut().println("Stop of node " + p + " was failed");
            });
        }
    }

    @CommandLine.Command(name = "list", description = "Show the list of currently running local Ignite nodes.")
    public static class ListNodesCommandSpec extends CommandSpec {

        @Inject private NodeManager nodeManager;
        @Inject private CliPathsConfigLoader cliPathsConfigLoader;

        @Override public void run() {
            IgnitePaths paths = cliPathsConfigLoader.loadIgnitePathsOrThrowError();

            List<NodeManager.RunningNode> nodes = nodeManager
                .getRunningNodes(paths.workDir, paths.cliPidsDir());

            if (nodes.isEmpty())
                spec.commandLine().getOut().println("No running nodes");
            else {
                Table table = new Table(0, spec.commandLine().getColorScheme());

                table.addRow("@|bold PID|@", "@|bold Consistent ID|@", "@|bold Log|@");

                for (NodeManager.RunningNode node : nodes) {
                    table.addRow(node.pid, node.consistentId, node.logFile);
                }

                spec.commandLine().getOut().println(table);
            }
        }
    }

    @CommandLine.Command(name = "classpath", description = "Show the current classpath used by the Ignite nodes.")
    public static class NodesClasspathCommandSpec extends CommandSpec {

        @Inject private NodeManager nodeManager;

        @Override public void run() {
            try {
                spec.commandLine().getOut().println(nodeManager.classpath());
            }
            catch (IOException e) {
                throw new IgniteCLIException("Can't get current classpath", e);
            }
        }
    }

}
