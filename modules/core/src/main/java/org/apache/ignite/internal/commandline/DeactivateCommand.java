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

package org.apache.ignite.internal.commandline;

import java.util.logging.Logger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;

import static org.apache.ignite.internal.commandline.ClusterStateChangeCommand.FORCE_COMMAND;
import static org.apache.ignite.internal.commandline.CommandList.DEACTIVATE;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;

/**
 * Command to deactivate cluster.
 * @deprecated Use {@link ClusterStateChangeCommand} instead.
 */
@Deprecated
public class DeactivateCommand implements Command<Void> {
    /** Cluster name. */
    private String clusterName;

    /** If {@code true}, cluster deactivation will be forced. */
    private boolean forceDeactivation;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Deactivate cluster (deprecated. Use " + SET_STATE.toString() + " instead):", DEACTIVATE,
            optional(FORCE_COMMAND), optional(CMD_AUTO_CONFIRMATION));
    }

    /** {@inheritDoc} */
    @Override public void prepareConfirmation(GridClientConfiguration clientCfg) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            clusterName = client.state().clusterName();
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: the command will deactivate a cluster \"" + clusterName + "\".";
    }

    /**
     * Deactivate cluster.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to deactivate.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        logger.warning("Command deprecated. Use " + SET_STATE.toString() + " instead.");

        try (GridClient client = Command.startClient(clientCfg)) {
            client.state().state(ClusterState.INACTIVE, forceDeactivation);

            logger.info("Cluster deactivated");
        }
        catch (Exception e) {
            logger.severe("Failed to deactivate cluster.");

            throw e;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        forceDeactivation = false;

        if (argIter.hasNextArg()) {
            String arg = argIter.peekNextArg();

            if (FORCE_COMMAND.equalsIgnoreCase(arg)) {
                forceDeactivation = true;

                argIter.nextArg("");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DEACTIVATE.toCommandName();
    }
}
