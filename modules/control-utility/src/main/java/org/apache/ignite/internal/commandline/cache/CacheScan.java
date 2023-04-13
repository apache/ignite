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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.cache.VisorCacheScanTask;
import org.apache.ignite.internal.visor.cache.VisorCacheScanTaskArg;
import org.apache.ignite.internal.visor.cache.VisorCacheScanTaskResult;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTask;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.SCAN;

/**
 * Command to show caches on cluster.
 */
public class CacheScan extends AbstractCommand<CacheScan.Arguments> {
    /** Default entries limit. */
    private static final int DFLT_LIMIT = 1_000;

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        String description = "Show cache content.";

        Map<String, String> map = F.asMap("--limit", "limit count of entries to scan (" + DFLT_LIMIT + " by default)");

        usageCache(log, SCAN, description, map, "cacheName", optional("--limit", "N"));
    }

    /** Command parsed arguments */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        VisorCacheScanTaskArg taskArg = new VisorCacheScanTaskArg(args.cacheName, args.limit);

        VisorCacheScanTaskResult res;

        try (GridClient client = Command.startClient(clientCfg)) {
            res = TaskExecutor.executeTask(
                client,
                VisorCacheScanTask.class,
                taskArg,
                clientCfg
            );

            List<VisorSystemViewTask.SimpleType> types = res.titles().stream()
                .map(x -> VisorSystemViewTask.SimpleType.STRING).collect(Collectors.toList());

            SystemViewCommand.printTable(res.titles(), types, res.entries(), log);

            if (res.entries().size() == args.limit)
                log.info("Result limited to " + args.limit + " rows. Limit can be changed with '--limit' argument.");
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String cacheName = argIter.nextArg("Cache name is expected");
        int limit = DFLT_LIMIT;

        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("").toLowerCase();

            if ("--limit".equals(nextArg))
                limit = argIter.nextIntArg("limit");
            else
                throw new IllegalArgumentException("Unknown argument: " + nextArg);
        }

        args = new Arguments(cacheName, limit);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SCAN.text().toUpperCase();
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Cache name. */
        private final String cacheName;

        /** Rows limit. */
        private final int limit;

        /** */
        public Arguments(String cacheName, int limit) {
            this.cacheName = cacheName;
            this.limit = limit;
        }
    }
}
