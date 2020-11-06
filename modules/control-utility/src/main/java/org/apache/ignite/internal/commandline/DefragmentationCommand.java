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

package org.apache.ignite.internal.commandline;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.defragmentation.DefragmentationArguments;
import org.apache.ignite.internal.commandline.defragmentation.DefragmentationSubcommands;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.defragmentation.VisorDefragmentationOperation;
import org.apache.ignite.internal.visor.defragmentation.VisorDefragmentationTask;
import org.apache.ignite.internal.visor.defragmentation.VisorDefragmentationTaskArg;
import org.apache.ignite.internal.visor.defragmentation.VisorDefragmentationTaskResult;

/** */
public class DefragmentationCommand implements Command<DefragmentationArguments> {
    /** */
    private DefragmentationArguments args;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Optional<GridClientNode> firstNodeOpt = client.compute().nodes().stream().filter(GridClientNode::connectable).findFirst();

            if (firstNodeOpt.isPresent()) {
                UUID connectableNodeId = firstNodeOpt.get().nodeId();

                VisorTaskArgument<?> visorArg;

                if (args.nodeIds() == null) {
                    visorArg = new VisorTaskArgument<>(
                        connectableNodeId,
                        convertArguments(),
                        false
                    );
                }
                else {
                    visorArg = new VisorTaskArgument<>(
                        client.compute().nodes().stream().filter(
                            node -> args.nodeIds().contains(node.consistentId().toString())
                        ).map(GridClientNode::nodeId).collect(Collectors.toList()),
                        convertArguments(),
                        false
                    );
                }

                VisorDefragmentationTaskResult res = client
                    .compute()
                    .projection(firstNodeOpt.get())
                    .execute(
                        VisorDefragmentationTask.class.getName(),
                        visorArg
                    );

//                printResult(res, log);
            }
            else
                log.warning("No nodes found in topology, command won't be executed.");
        }
        catch (Throwable t) {
            log.severe("Failed to execute defragmentation command='" + args.subcommand().text() + "'");
            log.severe(CommandLogger.errorMessage(t));

            throw t;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            args = new DefragmentationArguments(DefragmentationSubcommands.INFO);

            return;
        }

        DefragmentationSubcommands cmd = DefragmentationSubcommands.of(argIter.nextArg("Expected defragmentation maintenance action"));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct defragmentation maintenance action");

        args = new DefragmentationArguments(cmd);

        switch (cmd) {
            case INFO:
                break;

            case SCHEDULE:
                List<String> consistentIds = null;
                List<String> cacheNames = null;

                String subarg;

                do {
                    subarg = argIter.nextArg("Expected one of subcommand arguments.").toLowerCase(Locale.ENGLISH);

                    switch (subarg) {
                        case "--nodes": {
                            Set<String> ids = argIter.nextStringSet("--nodes");

                            if (ids.isEmpty())
                                throw new IllegalArgumentException("Consistent ids list is empty.");

                            consistentIds = new ArrayList<>(ids);

                            break;
                        }

                        case "--caches": {
                            Set<String> ids = argIter.nextStringSet("--caches");

                            if (ids.isEmpty())
                                throw new IllegalArgumentException("Caches list is empty.");

                            cacheNames = new ArrayList<>(ids);

                            break;
                        }

                        default:
                            subarg = null;
                    }
                }
                while (subarg != null);

                if (consistentIds == null)
                    throw new IllegalArgumentException("--nodes argument is missing.");

                args.setNodeIds(consistentIds);
                args.setCacheNames(cacheNames);

                break;

            case STATUS:
            case CANCEL:
                // No-op.
        }
    }

    /** {@inheritDoc} */
    @Override public DefragmentationArguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {

    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CommandList.DEFRAGMENTATION.toCommandName();
    }

    /** */
    private VisorDefragmentationTaskArg convertArguments() {
        return new VisorDefragmentationTaskArg(
            convertSubcommand(args.subcommand()),
            args.nodeIds(),
            args.cacheNames()
        );
    }

    /** */
    private static VisorDefragmentationOperation convertSubcommand(DefragmentationSubcommands subcmd) {
        switch (subcmd) {
            case SCHEDULE:
                return VisorDefragmentationOperation.SCHEDULE;

            case STATUS:
                return VisorDefragmentationOperation.STATUS;

            case CANCEL:
                return VisorDefragmentationOperation.CANCEL;

            default:
                throw new IllegalArgumentException(subcmd.name());
        }
    }
}
