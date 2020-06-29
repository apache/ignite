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

package org.apache.ignite.internal.commandline.meta.subcommands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Collection;
import java.util.logging.Logger;
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
public abstract class MetadataAbstractSubCommand<
    MetadataArgsDto extends IgniteDataTransferObject,
    MetadataResultDto extends IgniteDataTransferObject>
    implements Command<MetadataArgsDto> {
    /** Filesystem. */
    protected static final FileSystem FS = FileSystems.getDefault();

    /** */
    private MetadataArgsDto args;

    /** {@inheritDoc} */
    @Override public boolean experimental() {
        return true;
    }

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
            GridClientCompute compute = client.compute();

            // Try to find connectable server nodes.
            Collection<GridClientNode> nodes = compute.nodes((n) -> n.connectable() && !n.isClient());

            if (F.isEmpty(nodes)) {
                nodes = compute.nodes(GridClientNode::connectable);

                if (F.isEmpty(nodes))
                    throw new GridClientDisconnectedException("Connectable nodes not found", null);
            }

            GridClientNode node = nodes.stream()
                .findAny().orElse(null);

            if (node == null)
                node = compute.balancer().balancedNode(nodes);

            MetadataResultDto res = compute.projection(node).execute(
                taskName(),
                new VisorTaskArgument<>(node.nodeId(), arg(), false)
            );

            printResult(res, log);
        }
        catch (Throwable e) {
            log.severe("Failed to execute metadata command='" + name() + "'");
            log.severe(CommandLogger.errorMessage(e));

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
    protected abstract void printResult(MetadataResultDto res, Logger log);

    /**
     * @param val Integer value.
     * @return String.
     */
    protected String printInt(int val) {
        return "0x" + Integer.toHexString(val).toUpperCase() + " (" + val + ')';
    }

    /**
     *
     */
    public static class VoidDto extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in)
            throws IOException, ClassNotFoundException {
            // No-op.
        }
    }
}
