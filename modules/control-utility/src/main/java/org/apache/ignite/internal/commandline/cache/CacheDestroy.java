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
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.cache.VisorCacheStopTask;
import org.apache.ignite.internal.visor.cache.VisorCacheStopTaskArg;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.DESTROY;

/**
 * Cache destroy command.
 */
public class CacheDestroy extends AbstractCommand<VisorCacheStopTaskArg> {
    /** Command argument to destroy all user-created caches. */
    public static final String DESTROY_ALL_ARG = "--destroy-all-caches";

    /** Confirmation message format. */
    public static final String CONFIRM_MSG = "Warning! The command will destroy %d caches: %s.\n" +
        "If you continue, the cache data will be impossible to recover.";

    /** No user-created caches exists message. */
    public static final String NOOP_MSG = "No user-created caches exist.";

    /** Result message. */
    public static final String RESULT_MSG = "The following caches have been stopped: %s.";

    /** Command arguments. */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public VisorCacheStopTaskArg arg() {
        return new VisorCacheStopTaskArg(new ArrayList<>(args.cacheNames()));
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        usageCache(
            log,
            DESTROY,
            "Permanently destroy specified caches.",
            F.asMap(DESTROY_ALL_ARG, "permanently destroy all user-created caches."),
            optional("cacheName1,...,cacheNameN"),
            optional(DESTROY_ALL_ARG)
        );
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        boolean destroyAll = false;
        Set<String> caches = null;

        do {
            String cmdArg = argIter.nextArg("At least one argument is expected.");

            if (caches != null) {
                throw new IllegalArgumentException("Unexpected argument \"" + cmdArg +
                    "\". The cache names have already been specified: " + F.concat(caches, ",") + '.');
            }

            if (DESTROY_ALL_ARG.equals(cmdArg)) {
                destroyAll = true;

                continue;
            }

            if (destroyAll) {
                throw new IllegalArgumentException(
                    "Unexpected argument \"" + cmdArg + "\". The flag for deleting all caches is already set.");
            }

            caches = argIter.parseStringSet(cmdArg);
        } while (argIter.hasNextSubArg());

        args = new Arguments(caches, destroyAll);
    }

    /** {@inheritDoc} */
    @Override public void prepareConfirmation(GridClientConfiguration clientCfg) throws Exception {
        args.collectClusterCachesIfNeeded(clientCfg);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (F.isEmpty(args.cacheNames()))
            return null;

        return String.format(CONFIRM_MSG,
            args.cacheNames().size(), S.joinToString(new TreeSet<>(args.cacheNames()), ", ", "..", 80, 0));
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        args.collectClusterCachesIfNeeded(clientCfg);

        if (args.cacheNames().isEmpty()) {
            log.info(NOOP_MSG);

            return null;
        }

        try (GridClient client = Command.startClient(clientCfg)) {
            TaskExecutor.executeTask(client, VisorCacheStopTask.class, arg(), clientCfg);
        }

        log.info(String.format(RESULT_MSG, F.concat(args.cacheNames(), ", ")));

        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DESTROY.text().toUpperCase();
    }

    /** Command arguments. */
    private static class Arguments {
        /** Flag to destroy all user-created caches. */
        private final boolean destroyAll;

        /** Caches to destroy. */
        private final Set<String> rmvCaches;

        /** User-created caches existing in the cluster. */
        private Set<String> clusterCaches;

        /**
         * @param rmvCaches Caches to destroy.
         * @param destroyAll Flag to destroy all user-created caches.
         */
        public Arguments(Set<String> rmvCaches, boolean destroyAll) {
            assert destroyAll || !F.isEmpty(rmvCaches);

            this.destroyAll = destroyAll;
            this.rmvCaches = rmvCaches;
        }

        /** @return Caches to destroy. */
        public Set<String> cacheNames() {
            return rmvCaches == null ? clusterCaches : rmvCaches;
        }

        /**
         * @param clientCfg Client configuration.
         * @throws Exception If failed.
         */
        public void collectClusterCachesIfNeeded(GridClientConfiguration clientCfg) throws Exception {
            if (!destroyAll || clusterCaches != null)
                return;

            try (GridClient client = Command.startClient(clientCfg)) {
                clusterCaches = new HashSet<>();

                for (GridClientNode node : client.compute().nodes(GridClientNode::connectable))
                    clusterCaches.addAll(node.caches().keySet());
            }
        }
    }
}
