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

package org.apache.ignite.internal.commandline.cdc;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.cdc.VisorCdcDeleteLostSegmentsTask;

import static org.apache.ignite.internal.commandline.CommandList.CDC;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;

/**
 * CDC command.
 */
public class CdcCommand extends AbstractCommand<String> {
    /** Command to delete lost segment links. */
    public static final String DELETE_LOST_SEGMENT_LINKS = "delete_lost_segment_links";

    /** */
    public static final String NODE_ID = "--node-id";

    /** Node ID. */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            executeTaskByNameOnNode(
                client,
                VisorCdcDeleteLostSegmentsTask.class.getName(),
                null,
                nodeId,
                clientCfg
            );

            Collection<UUID> nodeIds = nodeId != null ? Collections.singletonList(nodeId) :
                client.compute().nodes(node -> !node.isClient()).stream().map(GridClientNode::nodeId)
                    .collect(Collectors.toSet());

            client.compute().execute(VisorCdcDeleteLostSegmentsTask.class.getName(),
                new VisorTaskArgument<>(nodeIds, false));

            String res = "Lost segment CDC links successfully removed.";

            log.info(res);

            return res;
        }
        catch (Throwable e) {
            log.error("Failed to perform operation.");
            log.error(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        nodeId = null;

        String cmd = argIter.nextArg("Expected command: " + DELETE_LOST_SEGMENT_LINKS);

        if (!DELETE_LOST_SEGMENT_LINKS.equalsIgnoreCase(cmd))
            throw new IllegalArgumentException("Unexpected command: " + cmd);

        while (argIter.hasNextSubArg()) {
            String opt = argIter.nextArg("Failed to read command argument.");

            if (NODE_ID.equalsIgnoreCase(opt))
                nodeId = argIter.nextUuidArg(NODE_ID);
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: The command will fix WAL segments gap in case CDC link creation was stopped by distributed " +
            "property or excess of maximum CDC directory size. Gap will be fixed by deletion of WAL segment links" +
            "previous to the last gap." + U.nl() +
            "All changes in deleted segment links will be lost!" + U.nl() +
            "Make sure you need to sync data before restarting the CDC application. You can synchronize caches " +
            "using snapshot or other methods.";
    }

    /** {@inheritDoc} */
    @Override public String arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        Map<String, String> params = new LinkedHashMap<>();

        params.put(NODE_ID + " node_id", "ID of the node to delete lost segment links from. " +
            "If not set, the command will affect all server nodes.");

        usage(logger, "Delete lost segment CDC links:", CDC, params, DELETE_LOST_SEGMENT_LINKS,
            optional(NODE_ID, "node_id"), optional(CMD_AUTO_CONFIRMATION));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "cdc";
    }

    /** {@inheritDoc} */
    @Override public boolean experimental() {
        return true;
    }
}
