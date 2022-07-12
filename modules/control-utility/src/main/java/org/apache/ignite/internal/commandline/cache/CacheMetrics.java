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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.cache.metrics.CacheMetricsOperation;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsTask;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsTaskArg;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsTaskResult;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.METRICS;
import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricsOperation.DISABLE;
import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricsOperation.ENABLE;
import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricsOperation.STATUS;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/**
 * Cache sub-command for a cache metrics collection management. It provides an ability to enable, disable or show status.
 */
public class CacheMetrics extends AbstractCommand<VisorCacheMetricsTaskArg> {
    /** Argument for applying metrics command operation to explicitly specified caches. */
    public static final String CACHES_ARGUMENT = "--caches";

    /** Argument for applying metrics command operation to all user caches. */
    public static final String ALL_CACHES_ARGUMENT = "--all-caches";

    /** Incorrect metrics operation message. */
    public static final String INCORRECT_METRICS_OPERATION_MESSAGE = "Expected correct metrics command operation.";

    /** Incorrect cache argument message. */
    public static final String INCORRECT_CACHE_ARGUMENT_MESSAGE =
        String.format("Expected one of these arguments: '%s' or '%s'. Multiple arguments are not allowed.",
            CACHES_ARGUMENT, ALL_CACHES_ARGUMENT);

    /** Expected caches list message. */
    public static final String EXPECTED_CACHES_LIST_MESSAGE = "comma-separated list of cache names.";

    /** Task argument. */
    private VisorCacheMetricsTaskArg arg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorCacheMetricsTaskResult res = TaskExecutor.executeTaskByNameOnNode(client,
                VisorCacheMetricsTask.class.getName(), arg, null, clientCfg);

            List<List<?>> values = new ArrayList<>();

            for (Map.Entry<String, Boolean> e : res.result().entrySet())
                values.add(asList(e.getKey(), e.getValue() ? "enabled" : "disabled"));

            SystemViewCommand.printTable(asList("Cache Name", "Metrics Status"), asList(STRING, STRING), values, log);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        String desc = "Manages user cache metrics collection: enables, disables it or shows status.";

        String cachesArgDesc = CACHES_ARGUMENT + " cache1" + optional(",...,cacheN");

        Map<String, String> paramsDesc = F.asMap(
            cachesArgDesc, "specifies a comma-separated list of cache names to which operation should be applied.",
            ALL_CACHES_ARGUMENT, "applies operation to all user caches.");

        usageCache(log, METRICS, desc, paramsDesc, or(ENABLE, DISABLE, STATUS), or(cachesArgDesc, ALL_CACHES_ARGUMENT));
    }

    /** {@inheritDoc} */
    @Override public VisorCacheMetricsTaskArg arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        CacheMetricsOperation op = CacheMetricsOperation.of(argIter.nextArg(INCORRECT_METRICS_OPERATION_MESSAGE));

        if (op == null)
            throw new IllegalArgumentException(INCORRECT_METRICS_OPERATION_MESSAGE);

        Set<String> cacheNames;

        String arg = argIter.nextArg(INCORRECT_CACHE_ARGUMENT_MESSAGE);

        if (CACHES_ARGUMENT.equals(arg))
            cacheNames = new TreeSet<>(argIter.nextStringSet(EXPECTED_CACHES_LIST_MESSAGE));
        else if (ALL_CACHES_ARGUMENT.equals(arg))
            cacheNames = Collections.emptySet();
        else
            throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);

        this.arg = new VisorCacheMetricsTaskArg(op, cacheNames);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return METRICS.text().toUpperCase();
    }
}
