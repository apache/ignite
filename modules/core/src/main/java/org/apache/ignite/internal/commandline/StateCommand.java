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

package org.apache.ignite.internal.commandline;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;

import static org.apache.ignite.internal.commandline.CommandList.STATE;

/**
 * Command to print cluster state.
 */
public class StateCommand implements Command<Void> {
    /** {@inheritDoc} */
    @Override public void printUsage() {
        Command.usage("Print current cluster state:", STATE);
    }

    /**
     * Print cluster state.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to print state.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)){
            GridClientClusterState state = client.state();

            logger.info("Cluster is " + (state.active() ? "active" : "inactive"));
        }
        catch (Throwable e) {
            logger.severe("Failed to get cluster state.");

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
