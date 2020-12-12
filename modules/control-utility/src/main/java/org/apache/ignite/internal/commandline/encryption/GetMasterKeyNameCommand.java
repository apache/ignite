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

package org.apache.ignite.internal.commandline.encryption;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.encryption.VisorGetMasterKeyNameTask;

import static org.apache.ignite.internal.commandline.CommandList.ENCRYPTION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.GET_MASTER_KEY_NAME;

/**
 * Get master key name encryption subcommand.
 */
public class GetMasterKeyNameCommand extends AbstractCommand<Void> {
    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            String masterKeyName = executeTaskByNameOnNode(
                client,
                VisorGetMasterKeyNameTask.class.getName(),
                null,
                null,
                clientCfg
            );

            log.info(masterKeyName);

            return masterKeyName;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Print the current master key name:", ENCRYPTION, GET_MASTER_KEY_NAME.toString());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return GET_MASTER_KEY_NAME.text().toUpperCase();
    }
}
