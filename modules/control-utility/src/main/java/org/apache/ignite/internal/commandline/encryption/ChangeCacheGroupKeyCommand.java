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
import org.apache.ignite.internal.visor.encryption.VisorCacheGroupEncryptionTaskArg;
import org.apache.ignite.internal.visor.encryption.VisorChangeCacheGroupKeyTask;

import static org.apache.ignite.internal.commandline.CommandList.ENCRYPTION;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.CHANGE_CACHE_GROUP_KEY;

/**
 * Change cache group key encryption subcommand.
 */
public class ChangeCacheGroupKeyCommand extends AbstractCommand<VisorCacheGroupEncryptionTaskArg> {
    /** Change cache group key task argument. */
    private VisorCacheGroupEncryptionTaskArg taskArg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            executeTaskByNameOnNode(
                client,
                VisorChangeCacheGroupKeyTask.class.getName(),
                taskArg,
                null,
                clientCfg
            );

            log.info("The encryption key has been changed for the cache group \"" + taskArg.groupName() + "\".");

            return null;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: the command will change the encryption key of the cache group. Joining a node during " +
            "the key change process is prohibited and will be rejected.";
    }

    /** {@inheritDoc} */
    @Override public VisorCacheGroupEncryptionTaskArg arg() {
        return taskArg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String argCacheGrpName = argIter.nextArg("Сache group name is expected.");

        taskArg = new VisorCacheGroupEncryptionTaskArg(argCacheGrpName);

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected command argument: " + argIter.peekNextArg());
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Change the encryption key of the cache group:", ENCRYPTION,
            CHANGE_CACHE_GROUP_KEY.toString(), "cacheGroupName");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CHANGE_CACHE_GROUP_KEY.text().toUpperCase();
    }
}
