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

package org.apache.ignite.internal.commandline.dr.subcommands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.IgniteFeatures.DR_CONTROL_UTILITY;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;

/** */
public abstract class DrAbstractRemoteSubCommand<
    VisorArgsDto extends IgniteDataTransferObject,
    VisorResultDto extends IgniteDataTransferObject,
    DrArgs extends DrAbstractRemoteSubCommand.Arguments<VisorArgsDto>
> implements Command<DrArgs> {
    /** */
    protected static boolean drControlUtilitySupported(GridClientNode node) {
        return nodeSupports(node.attribute(ATTR_IGNITE_FEATURES), DR_CONTROL_UTILITY);
    }

    /** */
    private DrArgs args;

    /** */
    private final List<GridClientNode> nodesWithoutDrTasks = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public final void printUsage(Logger log) {
        throw new UnsupportedOperationException("printUsage");
    }

    /** {@inheritDoc} */
    @Override public final void parseArguments(CommandArgIterator argIter) {
        args = parseArguments0(argIter);
    }

    /** {@inheritDoc} */
    @Override public final Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorResultDto res = execute0(clientCfg, client);

            printResult(res, log);
        }
        catch (Throwable e) {
            log.severe("Failed to execute dr command='" + name() + "'");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }

        return null;
    }

    /** */
    protected VisorResultDto execute0(
        GridClientConfiguration clientCfg,
        GridClient client
    ) throws Exception {
        GridClientCompute compute = client.compute();

        Collection<GridClientNode> nodes = compute.nodes();

        nodes.stream()
            .filter(node -> !drControlUtilitySupported(node))
            .collect(toCollection(() -> nodesWithoutDrTasks));

        List<UUID> nodeIds = nodes.stream()
            .filter(DrAbstractRemoteSubCommand::drControlUtilitySupported)
            .map(GridClientNode::nodeId)
            .collect(toList());

        if (F.isEmpty(nodeIds))
            throw new GridClientDisconnectedException("Connectable nodes not found", null);

        return compute.projection(DrAbstractRemoteSubCommand::drControlUtilitySupported)
            .execute(visorTaskName(), new VisorTaskArgument<>(nodeIds, args.toVisorArgs(), false));
    }

    /** */
    protected void printUnrecognizedNodesMessage(Logger log, boolean verbose) {
        if (!nodesWithoutDrTasks.isEmpty()) {
            log.warning("Unrecognized nodes found that have no DR API for control utility: " + nodesWithoutDrTasks.size());

            if (verbose) {
                for (GridClientNode node : nodesWithoutDrTasks) {
                    boolean clientNode = node.attribute(IgniteNodeAttributes.ATTR_CLIENT_MODE);

                    log.warning(String.format(INDENT + "nodeId=%s, Mode=%s", node.nodeId(), clientNode ? "Client" : "Server"));
                }
            }
            else
                log.warning("Please use \"--dr topology\" command to see full list.");
        }
    }

    /** {@inheritDoc} */
    @Override public final DrArgs arg() {
        return args;
    }

    /** */
    protected abstract String visorTaskName();

    /** */
    protected abstract DrArgs parseArguments0(CommandArgIterator argIter);

    /** */
    protected abstract void printResult(VisorResultDto res, Logger log);

    /** */
    @SuppressWarnings("PublicInnerClass")
    public interface Arguments<ArgsDto extends IgniteDataTransferObject> {
        /** */
        ArgsDto toVisorArgs();
    }
}
