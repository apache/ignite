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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.visor.misc.VisorIdAndTagViewTask;
import org.apache.ignite.internal.visor.misc.VisorIdAndTagViewTaskResult;

import static org.apache.ignite.internal.commandline.CommandList.STATE;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;

/**
 * Command to print cluster state.
 */
public class StateCommand extends AbstractCommand<Void> {
    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        usage(logger, "Print current cluster state:", STATE);
    }

    /**
     * Print cluster state.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to print state.
     */
    @Override public Object execute(ClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try (IgniteClient client = Command.startClient(clientCfg)) {
            ClusterState state = client.cluster().state();

            VisorIdAndTagViewTaskResult idAndTag = executeTask(client, VisorIdAndTagViewTask.class, null, clientCfg);

            log.info("Cluster  ID: " + idAndTag.id());
            log.info("Cluster tag: " + idAndTag.tag());

            log.info(CommandHandler.DELIM);

            switch (state) {
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
                    throw new IllegalStateException("Unknown state: " + state);
            }
        }
        catch (Throwable e) {
            if (!CommandHandler.isAuthError(e))
                log.error("Failed to get cluster state.");

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
