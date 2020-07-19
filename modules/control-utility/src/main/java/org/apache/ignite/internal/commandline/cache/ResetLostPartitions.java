/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.cache;

import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTask;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskArg;
import org.apache.ignite.internal.commandline.cache.reset_lost_partitions.CacheResetLostPartitionsTaskResult;

import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.RESET_LOST_PARTITIONS;

/**
 * Command for reseting lost partition state.
 */
public class ResetLostPartitions implements Command<Set<String>> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String CACHES = "cacheName1,...,cacheNameN";
        String description = "Reset the state of lost partitions for the specified caches.";

        usageCache(logger, RESET_LOST_PARTITIONS, description, null, CACHES);
    }

    /**
     * Command argument. Caches which lost partitions should be reseted.
     */
    private Set<String> caches;

    /** {@inheritDoc} */
    @Override public Set<String> arg() {
        return caches;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        CacheResetLostPartitionsTaskArg taskArg = new CacheResetLostPartitionsTaskArg(caches);

        try (GridClient client = Command.startClient(clientCfg)) {
            CacheResetLostPartitionsTaskResult res =
                executeTaskByNameOnNode(client, CacheResetLostPartitionsTask.class.getName(), taskArg, null, clientCfg);

            res.print(System.out);

            return res;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        caches = argIter.nextStringSet("Cache names");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return RESET_LOST_PARTITIONS.text().toUpperCase();
    }
}
