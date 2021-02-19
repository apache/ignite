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

import static org.apache.ignite.internal.commandline.Command.usage;
import static org.apache.ignite.internal.commandline.CommandList.DEFRAGMENTATION;
import static org.apache.ignite.internal.commandline.defragmentation.DefragmentationSubcommands.CANCEL;
import static org.apache.ignite.internal.commandline.defragmentation.DefragmentationSubcommands.SCHEDULE;

/** */
public class DefragmentationCommand implements Command<DefragmentationArguments> {
    /** */
    private static final String NODES_ARG = "--nodes";

    /** */
    private static final String CACHES_ARG = "--caches";

    /** */
    private DefragmentationArguments args;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Optional<GridClientNode> firstNodeOpt = client.compute().nodes().stream().filter(GridClientNode::connectable).findFirst();

            if (firstNodeOpt.isPresent()) {
                VisorDefragmentationTaskResult res;

                if (args.nodeIds() == null) {
                    res = TaskExecutor.executeTaskByNameOnNode(
                        client,
                        VisorDefragmentationTask.class.getName(),
                        convertArguments(),
                        null, // Use node from clientCfg.
                        clientCfg
                    );
                }
                else {
                    VisorTaskArgument<?> visorArg = new VisorTaskArgument<>(
                        client.compute().nodes().stream().filter(
                            node -> args.nodeIds().contains(node.consistentId().toString())
                        ).map(GridClientNode::nodeId).collect(Collectors.toList()),
                        convertArguments(),
                        false
                    );

                    res = client.compute()
                        .projection(firstNodeOpt.get())
                        .execute(
                            VisorDefragmentationTask.class.getName(),
                            visorArg
                        );
                }

                printResult(res, log);
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

    /** */
    private void printResult(VisorDefragmentationTaskResult res, Logger log) {
        assert res != null;

        log.info(res.getMessage());
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        DefragmentationSubcommands cmd = DefragmentationSubcommands.of(argIter.nextArg("Expected defragmentation subcommand."));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct defragmentation subcommand.");

        args = new DefragmentationArguments(cmd);

        switch (cmd) {
            case SCHEDULE:
                List<String> consistentIds = null;
                List<String> cacheNames = null;

                String subarg;

                do {
                    subarg = argIter.peekNextArg();

                    if (subarg == null)
                        break;

                    subarg = subarg.toLowerCase(Locale.ENGLISH);

                    switch (subarg) {
                        case NODES_ARG: {
                            argIter.nextArg("");

                            Set<String> ids = argIter.nextStringSet(NODES_ARG);

                            if (ids.isEmpty())
                                throw new IllegalArgumentException("Consistent ids list is empty.");

                            consistentIds = new ArrayList<>(ids);

                            break;
                        }

                        case CACHES_ARG: {
                            argIter.nextArg("");

                            Set<String> ids = argIter.nextStringSet(CACHES_ARG);

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
        String consistentIds = "consistentId0,consistentId1";

        String cacheNames = "cache1,cache2,cache3";

        usage(
            log,
            "Schedule PDS defragmentation on given nodes for all caches:",
            DEFRAGMENTATION,
            SCHEDULE.text(),
            NODES_ARG,
            consistentIds
        );

        usage(
            log,
            "Schedule PDS defragmentation on given nodes but only for given caches:",
            DEFRAGMENTATION,
            SCHEDULE.text(),
            NODES_ARG,
            consistentIds,
            CACHES_ARG,
            cacheNames
        );

        usage(
            log,
            "Cancel scheduled or active PDS defragmentation on underlying node:",
            DEFRAGMENTATION,
            CANCEL.text()
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DEFRAGMENTATION.toCommandName();
    }

    /** */
    private VisorDefragmentationTaskArg convertArguments() {
        return new VisorDefragmentationTaskArg(
            convertSubcommand(args.subcommand()),
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
