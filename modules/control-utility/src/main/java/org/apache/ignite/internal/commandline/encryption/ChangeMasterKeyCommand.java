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
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.encryption.VisorChangeMasterKeyTask;

import static org.apache.ignite.internal.commandline.CommandList.ENCRYPTION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.CHANGE_MASTER_KEY;

/**
 * Change master key encryption subcommand.
 */
public class ChangeMasterKeyCommand extends AbstractCommand<String> {
    /** New master key name. */
    private String argMasterKeyName;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            String resMsg = executeTaskByNameOnNode(
                client,
                VisorChangeMasterKeyTask.class.getName(),
                argMasterKeyName,
                null,
                clientCfg
            );

            log.info(resMsg);

            return resMsg;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: the command will change the master key. Cache start and node join during the key change " +
            "process is prohibited and will be rejected.";
    }

    /** {@inheritDoc} */
    @Override public String arg() {
        return argMasterKeyName;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        argMasterKeyName = argIter.nextArg("Expected master key name.");
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Change the master key:", ENCRYPTION, CHANGE_MASTER_KEY.toString(), "newMasterKeyName");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CHANGE_MASTER_KEY.text().toUpperCase();
    }
}
