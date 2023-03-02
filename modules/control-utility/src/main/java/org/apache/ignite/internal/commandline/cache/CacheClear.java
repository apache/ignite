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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.processors.cache.ClearCachesTask;
import org.apache.ignite.internal.processors.cache.ClearCachesTaskArg;
import org.apache.ignite.internal.processors.cache.ClearCachesTaskResult;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.CLEAR;

/** Command that clears specified caches. */
public class CacheClear extends AbstractCommand<ClearCachesTaskArg> {
    /** Message that contains cleared caches. */
    public static final String CLEAR_MSG = "The following caches have been cleared: %s";

    /** Message that contains not-cleared caches (they don't exist). */
    public static final String SKIP_CLEAR_MSG = "The following caches don't exist: %s";

    /** Comma-separated list of cache names. */
    public static final String CACHES = "--caches";

    /** Command parsed arguments. */
    private ClearCachesTaskArg arg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            ClearCachesTaskResult res = TaskExecutor.executeTask(client, ClearCachesTask.class, arg(), clientCfg);

            if (!F.isEmpty(res.clearedCaches()))
                U.log(log, String.format(CLEAR_MSG, String.join(", ", res.clearedCaches())));

            if (!F.isEmpty(res.nonExistentCaches()))
                U.warn(log, String.format(SKIP_CLEAR_MSG, String.join(", ", res.nonExistentCaches())));
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public ClearCachesTaskArg arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        usageCache(
            logger,
            CacheSubcommands.CLEAR,
            "Clear specified caches.",
            F.asMap(CACHES, "specifies a comma-separated list of cache names to be cleared."));
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String cmdArg = argIter.nextArg("Command arguments are expected.");

        if (cmdArg == null)
            throw new IllegalArgumentException("Unknown argument: " + cmdArg);

        if (CACHES.equals(cmdArg)) {
            String cacheNamesArg = argIter.nextArg("Expected a comma-separated cache names.");

            List<String> caches = Arrays.stream(cacheNamesArg.split(",")).collect(Collectors.toList());

            arg = new ClearCachesTaskArg(caches);
        }
        else
            throw new IllegalArgumentException("Unknown argument: " + cmdArg);

        if (argIter.hasNextSubArg()) {
            throw new IllegalArgumentException(
                "Invalid argument \"" + argIter.peekNextArg() + "\", no more arguments are expected.");
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CLEAR.text().toUpperCase();
    }
}
