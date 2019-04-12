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

import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;

public class StateCommand implements Command<Void> {

    @Override public String confirmationPrompt(Void args) {
        return null;
    }

    /**
     * Print cluster state.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to print state.
     */
    @Override public Object execute(Void args, GridClientConfiguration clientCfg,
        CommandLogger logger) throws Exception {
        try (GridClient client = GridClientFactory.start(clientCfg)){
            GridClientClusterState state = client.state();

            logger.log("Cluster is " + (state.active() ? "active" : "inactive"));
        }
        catch (Throwable e) {
            logger.log("Failed to get cluster state.");

            throw e;
        }

        return null;
    }

    @Override public Void init(CommandArgIterator argIterator) {
        return null;
    }
}
