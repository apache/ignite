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
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.visor.cluster.VisorCheckDeactivationTask;

import static org.apache.ignite.internal.commandline.CommandList.DEACTIVATE;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.DATA_LOST_ON_DEACTIVATION_WARNING;
import static org.apache.ignite.internal.commandline.ClusterStateChangeCommand.FORCE_COMMAND;

/**
 * Command to deactivate cluster.
 * @deprecated Use {@link ClusterStateChangeCommand} instead.
 */
@Deprecated
public class DeactivateCommand implements Command<Void> {
    /** Cluster name. */
    private String clusterName;

    /** Force cluster deactivation even it might have in-mem caches. */
    private boolean force;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Deactivate cluster (deprecated. Use " + SET_STATE.toString() + " instead):", DEACTIVATE,
            optional(FORCE_COMMAND, CMD_AUTO_CONFIRMATION));
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
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        log.warning("Command deprecated. Use " + SET_STATE.toString() + " instead.");

        try (GridClient client = Command.startClient(clientCfg)) {

            // Search for in-memory-only caches. Fail if possible data loss.
            if (!force) {
                Boolean readyToDeactivate = executeTask(client, VisorCheckDeactivationTask.class,
                    null, clientCfg);

                if (!readyToDeactivate) {
                    throw new IllegalStateException(DATA_LOST_ON_DEACTIVATION_WARNING
                        + " Please, add " + FORCE_COMMAND + " to deactivate cluster.");
                }
            }

            GridClientClusterState state = client.state();

            state.active(false);

            log.info("Cluster deactivated.");
        }
        catch (Exception e) {
            log.severe("Failed to deactivate cluster.");

            throw e;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (argIter.hasNextArg()) {
            String arg = argIter.peekNextArg();

            if (FORCE_COMMAND.equalsIgnoreCase(arg)) {
                force = true;

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
