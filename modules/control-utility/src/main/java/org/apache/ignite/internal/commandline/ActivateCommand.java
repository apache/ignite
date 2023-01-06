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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandList.ACTIVATE;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;

/**
 * Activate cluster command.
 * @deprecated Use {@link ClusterStateChangeCommand} instead.
 */
@Deprecated
public class ActivateCommand extends AbstractCommand<Void> {
    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        usage(logger, "Activate cluster (deprecated. Use " + SET_STATE.toString() + " instead):", ACTIVATE);
    }

    /**
     * Activate cluster.
     *
     * @param cfg Client configuration.
     * @throws GridClientException If failed to activate.
     */
    @Override public Object execute(ClientConfiguration cfg, IgniteLogger logger) throws Exception {
        logger.warning("Command deprecated. Use " + SET_STATE + " instead.");

        try (IgniteClient client = Command.startClient(cfg)) {
            client.cluster().state(ACTIVE);

            logger.info("Cluster activated");
        }
        catch (Throwable e) {
            logger.error("Failed to activate cluster.");

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
        return ACTIVATE.toCommandName();
    }
}
