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

import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.encryption.VisorStopReencryptionTask;

import static org.apache.ignite.internal.commandline.CommandList.ENCRYPTION;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.STOP_REENCRYPTION;

/**
 * Stop cache group reencryption subcommand.
 */
public class StopReencryptionCommand implements Command<String> {
    /** Cache group name. */
    private String grpName;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Map<UUID, Object> resErrs = executeTaskByNameOnNode(
                client,
                VisorStopReencryptionTask.class.getName(),
                CU.cacheId(grpName),
                BROADCAST_UUID,
                clientCfg
            );

            for (Map.Entry<UUID, Object> entry : resErrs.entrySet()) {
                String msg;

                if (entry.getValue() instanceof Throwable) {
                    msg = "failed to stop re-encryption of the cache group \"" + grpName +
                        "\" (" + ((Throwable)entry.getValue()).getMessage() + ").";
                }
                else {
                    msg = "re-encryption of the cache group \"" + grpName + "\" has " +
                        (((boolean)entry.getValue()) ? "" : "already ") + "been stopped.";
                }

                log.info(INDENT + "Node " + entry.getKey() + ": " + msg);
            }

            return null;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String arg() {
        return grpName;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        grpName = argIter.nextArg("Expected cache group name.");
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Stop cache group re-encryption:", ENCRYPTION,
            STOP_REENCRYPTION.toString(), "cacheGroupName");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return STOP_REENCRYPTION.name();
    }
}
