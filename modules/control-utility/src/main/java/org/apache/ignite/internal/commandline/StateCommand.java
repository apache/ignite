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

import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;

import static org.apache.ignite.internal.commandline.CommandList.STATE;

/**
 * Command to print cluster state.
 */
public class StateCommand implements Command<Void> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Print current cluster state:", STATE);
    }

    /**
     * Print cluster state.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to print state.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            GridClientClusterState state = client.state();

            UUID id = state.id();
            String tag = state.tag();

            log.info("Cluster  ID: " + id);
            log.info("Cluster tag: " + tag);

            log.info(CommandHandler.DELIM);

            ClusterState clusterState = state.state();

            switch (clusterState) {
                case ACTIVE:
                    log.info("Cluster is active");

                    break;

                case INACTIVE:
                    log.info("Cluster is inactive");

                    break;

                case ACTIVE_READ_ONLY:
                    log.info("Cluster is active (read-only)");

                    break;

                default:
                    throw new IllegalStateException("Unknown state: " + clusterState);
            }
        }
        catch (Throwable e) {
            if (!CommandHandler.isAuthError(e))
                log.severe("Failed to get cluster state.");

            throw e;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return STATE.toCommandName();
    }
}
