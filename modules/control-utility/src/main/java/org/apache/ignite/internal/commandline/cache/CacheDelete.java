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
import java.util.SortedSet;
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
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.cache.VisorCacheStopTask;
import org.apache.ignite.internal.visor.cache.VisorCacheStopTaskArg;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.DELETE;

/**
 * Cache delete command.
 */
public class CacheDelete extends AbstractCommand<VisorCacheStopTaskArg> {
    /** Command argument to delete all user-created caches. */
    public static final String DELETE_ALL_ARG = "--delete-all-caches";

    /** Command argument to disable checking for the existence of caches. */
    public static final String SKIP_EXISTENCE_ARG = "--skip-existence-check";

    /** Confirmation message format. */
    public static final String CONFIRM_MSG = "Warning! The command will delete %d caches: %s.\n" +
        "If you continue, the cache data will be impossible to recover.";

    /** No user-created caches exists message. */
    public static final String NOOP_MSG = "No user-created caches exist.";

    /** Result message. */
    public static final String RESULT_MSG = "The following caches have been stopped: %s.";

    /** Cache does not exists message. */
    public static final String NOT_EXISTS_MSG = "Specified caches does not exists: %s.";

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
            DELETE,
            "Permanently delete specified caches.",
            F.asMap(DELETE_ALL_ARG, "permanently delete all user-created caches.",
                SKIP_EXISTENCE_ARG, "disable check for cache existence, otherwise, the command will fail " +
                                    "if at least one of the specified caches does not exist."),
            optional("cacheName1,...,cacheNameN"),
            optional(DELETE_ALL_ARG),
            optional(SKIP_EXISTENCE_ARG)
        );
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        boolean checkExisting = true;
        boolean deleteAll = false;
        Set<String> caches = null;

        do {
            String cmdArg = argIter.nextArg("At least one argument is expected.");

            if (SKIP_EXISTENCE_ARG.equals(cmdArg))
                checkExisting = false;
            else
            if (DELETE_ALL_ARG.equals(cmdArg)) {
                if (caches != null) {
                    throw new IllegalArgumentException(
                        "Unexpected argument \"" + cmdArg + "\". The cache names are already specified.");
                }

                deleteAll = true;
            }
            else {
                if (deleteAll) {
                    throw new IllegalArgumentException(
                        "Unexpected argument \"" + cmdArg + "\". The flag for deleting all caches is already set.");
                }

                caches = argIter.parseStringSet(cmdArg);
            }
        } while (argIter.hasNextSubArg());

        args = new Arguments(caches, deleteAll, checkExisting);
    }

    /** {@inheritDoc} */
    @Override public void prepareConfirmation(GridClientConfiguration clientCfg) throws Exception {
        args.checkExistingCachesIfNeeded(clientCfg);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        if (F.isEmpty(args.cacheNames()))
            return null;

        // Limit the number of cache names displayed to the user.
        SortedSet<String> cacheNames = new TreeSet<>(args.cacheNames());
        SB buf = new SB();
        int limit = 5;

        boolean compact = cacheNames.size() > limit;

        for (String cacheName : cacheNames) {
            if (compact && --limit == 0) {
                buf.a(",... ").a(cacheNames.last());

                break;
            }
            else if (buf.length() > 0)
                buf.a(", ");

            buf.a(cacheName);
        }

        return String.format(CONFIRM_MSG, args.cacheNames().size(), buf.toString());
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        args.checkExistingCachesIfNeeded(clientCfg);

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
        return DELETE.text().toUpperCase();
    }

    /** Command arguments. */
    private static class Arguments {
        /** Flag to delete all user-created caches. */
        private final boolean deleteAll;

        /** Flag to check the existence of caches. */
        private final boolean checkExisting;

        /** Caches tp delete. */
        private final Set<String> rmvCaches;

        /** User-created caches existing in the cluster. */
        private Set<String> clusterCaches;

        /**
         * @param rmvCaches Caches tp delete.
         * @param deleteAll Flag to delete all user-created caches.
         * @param checkExisting Flag to check the existence of caches.
         */
        public Arguments(Set<String> rmvCaches, boolean deleteAll, boolean checkExisting) {
            this.deleteAll = deleteAll;
            this.checkExisting = checkExisting;
            this.rmvCaches = rmvCaches;
        }

        /**
         * @return Caches tp delete.
         */
        public Set<String> cacheNames() {
            return rmvCaches == null ? clusterCaches : rmvCaches;
        }

        /**
         * @param clientCfg Client configuration.
         * @throws Exception If failed.
         */
        public void checkExistingCachesIfNeeded(GridClientConfiguration clientCfg) throws Exception {
            if (clusterCaches == null) {
                try (GridClient client = Command.startClient(clientCfg)) {
                    clusterCaches = new HashSet<>();

                    for (GridClientNode node : client.compute().nodes(GridClientNode::connectable))
                        clusterCaches.addAll(node.caches().keySet());
                }
            }

            if (deleteAll || !checkExisting)
                return;

            Set<String> unknownCaches = new HashSet<>(rmvCaches);

            unknownCaches.removeAll(clusterCaches);

            if (!unknownCaches.isEmpty())
                throw new IllegalArgumentException(String.format(NOT_EXISTS_MSG, F.concat(unknownCaches, ", ")));
        }
    }
}
