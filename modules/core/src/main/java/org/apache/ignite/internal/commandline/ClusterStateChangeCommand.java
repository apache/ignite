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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;

/**
 * Command to change cluster state.
 */
public class ClusterStateChangeCommand implements Command<ClusterState> {
    /** Flag of forced cluster deactivation. */
    static final String FORCE_COMMAND = "--force";

    /** New cluster state */
    private ClusterState state;

    /** Cluster name. */
    private String clusterName;

    /** If {@code true}, cluster deactivation will be forced. */
    private boolean forceDeactivation;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = new LinkedHashMap<>();

        params.put(ACTIVE.toString(), "Activate cluster. Cache updates are allowed.");
        params.put(INACTIVE.toString(), "Deactivate cluster.");
        params.put(ACTIVE_READ_ONLY.toString(), "Activate cluster. Cache updates are denied.");

        Command.usage(log, "Change cluster state:", SET_STATE, params, or((Object[])ClusterState.values()),
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
        return "Warning: the command will change state of cluster with name \"" + clusterName + "\" to " + state + ".";
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            client.state().state(state, forceDeactivation);

            log.info("Cluster state changed to " + state);

            return null;
        }
        catch (Throwable e) {
            log.info("Failed to change cluster state to " + state);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String s = argIter.nextArg("New cluster state not found.");

        try {
            state = ClusterState.valueOf(s.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Can't parse new cluster state. State: " + s, e);
        }

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
    @Override public ClusterState arg() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SET_STATE.toCommandName();
    }
}
