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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
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

/** */
public abstract class DrAbstractRemoteSubCommand<
    VisorArgsDto extends IgniteDataTransferObject,
    VisorResultDto extends IgniteDataTransferObject,
    DrArgs extends DrAbstractRemoteSubCommand.Arguments<VisorArgsDto>
> implements Command<DrArgs> {
    /** */
    private DrArgs args;

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

        if (F.isEmpty(nodes))
            throw new GridClientDisconnectedException("Connectable nodes not found", null);

        List<UUID> nodeIds = nodes.stream()
            .map(GridClientNode::nodeId)
            .collect(Collectors.toList());

        return compute.execute(visorTaskName(), new VisorTaskArgument<>(nodeIds, args.toVisorArgs(), false));
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
