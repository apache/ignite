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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.encryption.VisorCacheGroupKeyIdsTask;

import static org.apache.ignite.internal.commandline.CommandList.ENCRYPTION;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.TaskExecutor.BROADCAST_UUID;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.CACHE_GROUP_KEY_IDS;

/**
 * View cache group encryption key identifiers subcommand.
 */
public class CacheGroupKeysCommand implements Command<String> {
    /** Cache group name, */
    private String argCacheGrpName;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Map<UUID, List<Integer>> keyIdsMap = executeTaskByNameOnNode(
                client,
                VisorCacheGroupKeyIdsTask.class.getName(),
                argCacheGrpName,
                BROADCAST_UUID,
                clientCfg
            );

            log.info("Encryption key identifiers for cache: " + argCacheGrpName);

            for (Map.Entry<UUID, List<Integer>> entry : keyIdsMap.entrySet()) {
                log.info(INDENT + "Node: " + entry.getKey());

                List<Integer> keyIds = entry.getValue();

                if (F.isEmpty(keyIds)) {
                    log.info(DOUBLE_INDENT + "---");

                    continue;
                }

                for (int i = 0; i < keyIds.size(); i++)
                    log.info(DOUBLE_INDENT + keyIds.get(i) + (i == 0 ? " (active)" : ""));
            }

            return keyIdsMap;
        }
        catch (Throwable e) {
            log.severe("Failed to perform operation.");
            log.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String arg() {
        return argCacheGrpName;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        argCacheGrpName = argIter.nextArg("Expected cache group name.");
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "View encryption key identifiers of the cache group:", ENCRYPTION,
            CACHE_GROUP_KEY_IDS.toString(), "cacheGroupName");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CACHE_GROUP_KEY_IDS.name();
    }
}
