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
import org.apache.ignite.internal.client.GridClientConfiguration;

import static org.apache.ignite.internal.commandline.CommandList.READ_ONLY_ENABLE;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;

/**
 * Command to enable cluster read-only mode.
 */
public class ClusterReadOnlyModeEnableCommand implements Command<Void> {
    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            client.state().readOnly(true);

            log.info("Cluster read-only mode enabled");
        }
        catch (Throwable e) {
            log.info("Failed to enable read-only mode");

            throw e;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: the command will enable read-only mode on a cluster.";
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(
            log,
            "Enable read-only mode on active cluster:",
            READ_ONLY_ENABLE,
            optional(CMD_AUTO_CONFIRMATION)
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return READ_ONLY_ENABLE.toCommandName();
    }
}
