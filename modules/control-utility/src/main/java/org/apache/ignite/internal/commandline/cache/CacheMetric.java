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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.CacheMetricCommandArg;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.cache.metrics.CacheMetricOperation;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricTask;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricTaskArg;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricTaskResult;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.METRIC;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricCommandArg.ALL_CACHES;
import static org.apache.ignite.internal.commandline.cache.argument.CacheMetricCommandArg.CACHES;
import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricOperation.DISABLE;
import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricOperation.ENABLE;
import static org.apache.ignite.internal.visor.cache.metrics.CacheMetricOperation.STATUS;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/**
 * Cache sub-command for a cache metrics collection management. It provides an ability to enable, disable or show status.
 */
public class CacheMetric extends AbstractCommand<VisorCacheMetricTaskArg> {
    /** Incorrect metric operation message. */
    public static final String INCORRECT_METRIC_OPERATION_MESSAGE = "Expected correct metric command operation name.";

    /** Incorrect cache argument message. */
    public static final String INCORRECT_CACHE_ARGUMENT_MESSAGE =
        String.format("Expected one of these arguments: '%s' or '%s'. Multiple arguments are not allowed.", CACHES, ALL_CACHES);

    /** Invalid caches list message. */
    public static final String INVALID_CACHES_LIST_MESSAGE = "comma-separated list of cache names.";

    /** No caches processed message. */
    public static final String NONE_CACHES_PROCESSED_MESSAGE = "None of caches have been processed. " +
        "Are there any caches in cluster?";

    /** Duplicated '--caches' option message. */
    public static final String DUPLICATED_CACHES_OPTION_MESSAGE = "Duplicated '" + CACHES + "' option found.";

    /** Duplicated '--all-caches' option message. */
    public static final String DUPLICATED_ALL_CACHES_OPTION_MESSAGE = "Duplicated '" + ALL_CACHES + "' option found.";

    /** Success message. */
    public static final String SUCCESS_MESSAGE = "Command performed successfully.";

    /** Task argument. */
    private VisorCacheMetricTaskArg arg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorCacheMetricTaskResult taskResult = TaskExecutor.executeTaskByNameOnNode(client,
                VisorCacheMetricTask.class.getName(), arg, null, clientCfg);

            return processTaskResult(log, taskResult.result());
        }
    }

    /**
     * @param log Logger.
     * @param result Task result.
     */
    private String processTaskResult(Logger log, Object result) {
        Objects.requireNonNull(result);

        switch (arg.subCommand()) {
            case ENABLE:
            case DISABLE:
                String resultMsg = ((Integer)result) > 0 ? SUCCESS_MESSAGE : NONE_CACHES_PROCESSED_MESSAGE;

                log.info(resultMsg);

                break;
            case STATUS:
                Map<String, Boolean> statusTaskResult = (Map<String, Boolean>)result;

                if (statusTaskResult.isEmpty())
                    log.info(NONE_CACHES_PROCESSED_MESSAGE);
                else {
                    Collection<List<?>> values = F.viewReadOnly(statusTaskResult.entrySet(),
                        e -> asList(e.getKey(), e.getValue() ? "enabled" : "disabled"));

                    SystemViewCommand.printTable(asList("Cache Name", "Metrics Enable Status"), asList(STRING, STRING),
                        values, log);
                }

                break;
            default:
                throw new IllegalStateException("Unexpected value: " + arg.subCommand());
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        String desc = "Manages user cache metrics collection: enables, disables it or shows status.";

        String cachesArgDesc = CACHES + " cache1" + optional(",...,cacheN");

        Map<String, String> paramsDesc = F.asMap(
            cachesArgDesc, "specifies a comma-separated list of cache names to which sub-command should be applied.",
            ALL_CACHES.argName(), "applies sub-command to all user caches.");

        usageCache(log, METRIC, desc, paramsDesc, or(ENABLE, DISABLE, STATUS), or(cachesArgDesc, ALL_CACHES));
    }

    /** {@inheritDoc} */
    @Override public VisorCacheMetricTaskArg arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        CacheMetricOperation operation = CacheMetricOperation.of(argIter.nextArg(INCORRECT_METRIC_OPERATION_MESSAGE));

        if (operation == null)
            throw new IllegalArgumentException(INCORRECT_METRIC_OPERATION_MESSAGE);

        Set<String> cacheNames = null;

        boolean isAllCaches = false;

        while (argIter.hasNextSubArg()) {
            CacheMetricCommandArg cmdArg = CommandArgUtils.of(
                argIter.nextArg(INCORRECT_CACHE_ARGUMENT_MESSAGE),
                CacheMetricCommandArg.class
            );

            if (cmdArg == null)
                throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);

            switch (cmdArg) {
                case CACHES:
                    if (!F.isEmpty(cacheNames))
                        throw new IllegalArgumentException(DUPLICATED_CACHES_OPTION_MESSAGE);

                    cacheNames = argIter.nextStringSet(INVALID_CACHES_LIST_MESSAGE);

                    if (cacheNames.isEmpty())
                        throw new IllegalArgumentException("Expected " + INVALID_CACHES_LIST_MESSAGE);

                    break;
                case ALL_CACHES:
                    if (isAllCaches)
                        throw new IllegalArgumentException(DUPLICATED_ALL_CACHES_OPTION_MESSAGE);

                    isAllCaches = true;

                    break;
                default:
                    throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);
            }
        }

        // Fail if no arguments passed after subcommand
        if (F.isEmpty(cacheNames) && !isAllCaches)
            throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);

        // Fail if both '--all-caches' and '--caches' options are present (they are mutual exclusive options)
        if (!F.isEmpty(cacheNames) && isAllCaches)
            throw new IllegalArgumentException(INCORRECT_CACHE_ARGUMENT_MESSAGE);

        arg = new VisorCacheMetricTaskArg(operation, cacheNames);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return METRIC.text().toUpperCase();
    }
}
