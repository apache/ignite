/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.property.subcommands;

import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;

/** */
public abstract class PropertyAbstractSubCommand<
    MetadataArgsDto extends IgniteDataTransferObject,
    MetadataResultDto extends IgniteDataTransferObject>
    extends AbstractCommand<MetadataArgsDto> {
    /** */
    private MetadataArgsDto args;

    /** {@inheritDoc} */
    @Override public final void printUsage(IgniteLogger log) {
        throw new UnsupportedOperationException("printUsage");
    }

    /** {@inheritDoc} */
    @Override public final void parseArguments(CommandArgIterator argIter) {
        args = parseArguments0(argIter);
    }

    /** {@inheritDoc} */
    @Override public final Object execute(ClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try (IgniteClient client = Command.startClient(clientCfg)) {
            // Try to find connectable server nodes.
            Collection<ClusterNode> nodes = client.cluster().forServers().nodes();

            if (F.isEmpty(nodes)) {
                nodes = client.cluster().nodes();

                if (F.isEmpty(nodes))
                    throw new ClientException("Connectable nodes not found", null);
            }

            ClusterNode node = nodes.stream()
                .findAny().orElse(null);

            if (node == null)
                node = client.cluster().forOldest().node();

            MetadataResultDto res = client.compute(client.cluster().forNodes(nodes)).execute(
                taskName(),
                new VisorTaskArgument<>(node.id(), arg(), false)
            );

            printResult(res, log);
        }
        catch (Throwable e) {
            log.error("Failed to execute metadata command='" + name() + "'");
            log.error(CommandLogger.errorMessage(e));

            throw e;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public final MetadataArgsDto arg() {
        return args;
    }

    /** */
    protected abstract String taskName();

    /** */
    protected MetadataArgsDto parseArguments0(CommandArgIterator argIter) {
        return null;
    }

    /** */
    protected abstract void printResult(MetadataResultDto res, IgniteLogger log);
}
