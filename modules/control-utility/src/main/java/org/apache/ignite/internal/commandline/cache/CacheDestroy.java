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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.cache.VisorCacheStopTask;
import org.apache.ignite.internal.visor.cache.VisorCacheStopTaskArg;

import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.DESTROY;

/**
 * Cache destroy command.
 */
public class CacheDestroy extends AbstractCommand<VisorCacheStopTaskArg> {
    /** Command argument to destroy all user-created caches. */
    public static final String DESTROY_ALL_ARG = "--destroy-all-caches";

    /** Command argument to specify a comma-separated list of cache names to be destroyed. */
    public static final String CACHE_NAMES_ARG = "--caches";

    /** Confirmation message format. */
    public static final String CONFIRM_MSG = "Warning! The command will destroy %d caches: %s.\n" +
        "If you continue, the cache data will be impossible to recover.";

    /** No user-created caches exists message. */
    public static final String NOOP_MSG = "No user-created caches exist.";

    /** Result message. */
    public static final String RESULT_MSG = "The following caches have been stopped: %s.";

    /** Flag to destroy all user-created caches. */
    private boolean destroyAll;

    /** Caches to destroy. */
    private Set<String> cacheNames;

    /** {@inheritDoc} */
    @Override public VisorCacheStopTaskArg arg() {
        return new VisorCacheStopTaskArg(new ArrayList<>(cacheNames));
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        String cacheNamesArgFull = CACHE_NAMES_ARG + " cache1,...,cacheN";

        usageCache(
            log,
            DESTROY,
            "Permanently destroy specified caches.",
            F.asMap(cacheNamesArgFull, "specifies a comma-separated list of cache names to be destroyed.",
                DESTROY_ALL_ARG, "permanently destroy all user-created caches."),
            or(cacheNamesArgFull, DESTROY_ALL_ARG)
        );
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cacheNames = null;
        destroyAll = false;

        String requiredArgsMsg = "One of \"" + CACHE_NAMES_ARG + "\" or \"" + DESTROY_ALL_ARG + "\" is expected.";

        String cmdArg = argIter.nextArg(requiredArgsMsg);

        if (DESTROY_ALL_ARG.equals(cmdArg))
            destroyAll = true;
        else if (CACHE_NAMES_ARG.equals(cmdArg))
            cacheNames = new TreeSet<>(argIter.nextStringSet("cache names"));
        else
            throw new IllegalArgumentException("Invalid argument \"" + cmdArg + "\". " + requiredArgsMsg);

        if (argIter.hasNextSubArg()) {
            throw new IllegalArgumentException(
                "Invalid argument \"" + argIter.peekNextArg() + "\", no more arguments is expected.");
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareConfirmation(ClientConfiguration clientCfg) throws Exception {
        if (destroyAll)
            cacheNames = collectClusterCaches(clientCfg);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (F.isEmpty(cacheNames))
            return null;

        return String.format(CONFIRM_MSG, cacheNames.size(), S.joinToString(cacheNames, ", ", "..", 80, 0));
    }

    /** {@inheritDoc} */
    @Override public Object execute(ClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        if (destroyAll && cacheNames == null)
            cacheNames = collectClusterCaches(clientCfg);

        if (cacheNames.isEmpty()) {
            assert destroyAll;

            log.info(NOOP_MSG);

            return null;
        }

        try (IgniteClient client = Command.startClient(clientCfg)) {
            TaskExecutor.executeTask(client, VisorCacheStopTask.class, arg(), clientCfg);
        }

        log.info(String.format(RESULT_MSG, F.concat(cacheNames, ", ")));

        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DESTROY.text().toUpperCase();
    }

    /**
     * @param clientCfg Client configuration.
     * @return Names of user-created caches that exist in the cluster.
     * @throws Exception If failed.
     */
    private Set<String> collectClusterCaches(ClientConfiguration clientCfg) throws Exception {
        try (IgniteClient client = Command.startClient(clientCfg)) {
            return new HashSet(client.cacheNames());
        }
    }
}
