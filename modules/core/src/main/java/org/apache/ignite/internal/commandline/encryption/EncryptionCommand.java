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
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.encryption.VisorEncryptionArgs;
import org.apache.ignite.internal.visor.encryption.VisorEncryptionTask;
import org.apache.ignite.internal.visor.encryption.VisorEncryptionTaskResult;

import static org.apache.ignite.internal.commandline.CommandList.ENCRYPTION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommand.CHANGE_MASTER_KEY;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommand.GET_MASTER_KEY;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommand.of;

/**
 * Commands assosiated with encryption functionality.
 */
public class EncryptionCommand implements Command<VisorEncryptionArgs> {
    /** Arguments. */
    private VisorEncryptionArgs encryptionArgs;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorEncryptionTaskResult res = executeTask(
                client,
                VisorEncryptionTask.class,
                encryptionArgs,
                clientCfg
            );

            return res.result();
        }
        catch (Throwable e) {
            logger.severe("Failed to perform operation.");
            logger.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (encryptionArgs != null && CHANGE_MASTER_KEY == encryptionArgs.getCmd())
            return "Warning: the command will change the master key.";

        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        EncryptionSubcommand cmd = of(argIter.nextArg("Expected encryption action."));

        if (cmd == null)
            throw new IllegalArgumentException("Expected correct encryption action.");

        switch (cmd) {
            case GET_MASTER_KEY:
                encryptionArgs = new VisorEncryptionArgs(GET_MASTER_KEY);

                break;

            case CHANGE_MASTER_KEY:
                String masterKeyId = argIter.nextArg("Expected master key id.");

                encryptionArgs = new VisorEncryptionArgs(CHANGE_MASTER_KEY, masterKeyId);

                break;

            default:
                throw new IllegalArgumentException("Unknown encryption subcommand: " + cmd);
        }

    }

    /** {@inheritDoc} */
    @Override public VisorEncryptionArgs arg() {
        return encryptionArgs;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Print current master key id:", ENCRYPTION, GET_MASTER_KEY.toString());
        Command.usage(logger, "Change master key id:", ENCRYPTION, CHANGE_MASTER_KEY.toString(), "newMasterKeyId");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return ENCRYPTION.toCommandName();
    }
}
