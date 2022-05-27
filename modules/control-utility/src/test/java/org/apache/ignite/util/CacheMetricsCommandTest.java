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

package org.apache.ignite.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.commandline.cache.CacheMetrics;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.visor.cache.metrics.CacheMetricsOperation;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.cache.CacheMetrics.ALL_CACHES_ARGUMENT;
import static org.apache.ignite.internal.commandline.cache.CacheMetrics.CACHES_ARGUMENT;
import static org.apache.ignite.internal.commandline.cache.CacheMetrics.EXPECTED_CACHES_LIST_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.CacheMetrics.INCORRECT_CACHE_ARGUMENT_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.CacheMetrics.INCORRECT_METRICS_OPERATION_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.METRICS;
import static org.apache.ignite.internal.util.lang.GridFunc.asMap;

/**
 * Test for {@link CacheMetrics} command.
 */
public class CacheMetricsCommandTest extends GridCommandHandlerAbstractTest {
    /** Enable operation. */
    private static final String ENABLE_COMMAND = CacheMetricsOperation.ENABLE.toString();

    /** Disable operation. */
    private static final String DISABLE_COMMAND = CacheMetricsOperation.DISABLE.toString();

    /** Status operation. */
    private static final String STATUS_COMMAND = CacheMetricsOperation.STATUS.toString();

    /** Cache one. */
    private static final String CACHE_ONE = "cache-1";

    /** Cache two. */
    private static final String CACHE_TWO = "cache-2";

    /** Cache three. */
    private static final String CACHE_THREE = "cache-3";

    /** Non-metric cache. */
    private static final String NON_METRIC_CACHE = "non-metric-cache";

    /** Not found cache. */
    private static final String NON_EXISTENT_CACHE = "non-existent-cache";

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        injectTestSystemOut();
        persistenceEnable(false);
        autoConfirmation = false;

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests metrics enabling/disabling for some (not all) caches in a cluster.
     */
    @Test
    public void testEnableDisable() {
        createCachesWithMetrics(asMap(CACHE_ONE, false, NON_METRIC_CACHE, false));

        checkExecutionSuccess(asMap(CACHE_ONE, true), ENABLE_COMMAND, CACHES_ARGUMENT, CACHE_ONE);
        checkClusterMetrics(asMap(NON_METRIC_CACHE, false));

        createCachesWithMetrics(asMap(CACHE_TWO, true));

        checkExecutionSuccess(asMap(CACHE_TWO, false), DISABLE_COMMAND, CACHES_ARGUMENT, CACHE_TWO);
        checkClusterMetrics(asMap(NON_METRIC_CACHE, false));

        checkExecutionSuccess(asMap(CACHE_ONE, false, CACHE_TWO, false), DISABLE_COMMAND, CACHES_ARGUMENT,
            CACHE_ONE + ',' + CACHE_TWO);
        checkClusterMetrics(asMap(NON_METRIC_CACHE, false));

        // Cache list with duplicates and shuffled order
        String cacheNames = String.join(",", CACHE_TWO, CACHE_TWO, CACHE_ONE, CACHE_ONE, CACHE_TWO);

        checkExecutionSuccess(asMap(CACHE_ONE, true, CACHE_TWO, true), ENABLE_COMMAND, CACHES_ARGUMENT, cacheNames);
        checkClusterMetrics(asMap(NON_METRIC_CACHE, false));
    }

    /**
     * Tests '--all-caches' argument for 'enable' and 'disable' options.
     */
    @Test
    public void testEnableDisableAll() {
        createCachesWithMetrics(asMap(CACHE_ONE, false));

        checkExecutionSuccess(asMap(CACHE_ONE, true), ENABLE_COMMAND, ALL_CACHES_ARGUMENT);

        createCachesWithMetrics(asMap(CACHE_TWO, true, CACHE_THREE, false));

        Map<String, Boolean> expStatuses = new HashMap<>();

        expStatuses.put(CACHE_ONE, false);
        expStatuses.put(CACHE_TWO, false);
        expStatuses.put(CACHE_THREE, false);

        checkExecutionSuccess(expStatuses, DISABLE_COMMAND, ALL_CACHES_ARGUMENT);

        expStatuses.put(CACHE_ONE, true);
        expStatuses.put(CACHE_TWO, true);
        expStatuses.put(CACHE_THREE, true);

        checkExecutionSuccess(expStatuses, ENABLE_COMMAND, ALL_CACHES_ARGUMENT);
    }

    /**
     * Tests metrics enabling/disabling for a non-existing caches.
     */
    @Test
    public void testNotFoundCacheEnableDisable() {
        createCachesWithMetrics(asMap(CACHE_ONE, false));

        String cacheArg = String.join(",", CACHE_ONE, CACHE_TWO, NON_EXISTENT_CACHE);

        checkExecutionError(descriptorsNotFound(CACHE_ONE, CACHE_TWO, NON_EXISTENT_CACHE), ENABLE_COMMAND,
            CACHES_ARGUMENT, cacheArg);

        // Check that metrics statuses was not changed
        checkClusterMetrics(asMap(CACHE_ONE, false));

        createCachesWithMetrics(asMap(CACHE_TWO, true));

        checkExecutionError(descriptorsNotFound(CACHE_ONE, CACHE_TWO, NON_EXISTENT_CACHE), DISABLE_COMMAND,
            CACHES_ARGUMENT, cacheArg);

        // Check that metrics statuses was not changed
        checkClusterMetrics(asMap(CACHE_ONE, false, CACHE_TWO, true));
    }

    /**
     * Tests metrics status for some (not all) caches in a cluster.
     */
    @Test
    public void testStatus() {
        createCachesWithMetrics(asMap(CACHE_ONE, false, NON_METRIC_CACHE, false));

        checkExecutionSuccess(asMap(CACHE_ONE, false), STATUS_COMMAND, CACHES_ARGUMENT, CACHE_ONE);

        createCachesWithMetrics(asMap(CACHE_TWO, true));

        checkExecutionSuccess(asMap(CACHE_TWO, true), STATUS_COMMAND, CACHES_ARGUMENT, CACHE_TWO);

        IgniteClusterEx cluster = grid(0).cluster();
        cluster.enableStatistics(Collections.singleton(CACHE_ONE), true);
        cluster.enableStatistics(Collections.singleton(CACHE_TWO), false);

        checkExecutionSuccess(asMap(CACHE_ONE, true, CACHE_TWO, false), STATUS_COMMAND, CACHES_ARGUMENT,
            CACHE_ONE + ',' + CACHE_TWO);

        cluster.enableStatistics(Arrays.asList(CACHE_ONE, CACHE_TWO), true);

        // Cache list with duplicates and shuffled order
        String cacheNames = String.join(",", NON_METRIC_CACHE, CACHE_TWO, CACHE_ONE, CACHE_TWO, NON_METRIC_CACHE);

        Map<String, Boolean> expectedMetricsStatuses = new HashMap<>();

        expectedMetricsStatuses.put(CACHE_ONE, true);
        expectedMetricsStatuses.put(CACHE_TWO, true);
        expectedMetricsStatuses.put(NON_METRIC_CACHE, false);

        checkExecutionSuccess(expectedMetricsStatuses, STATUS_COMMAND, CACHES_ARGUMENT, cacheNames);
    }

    /**
     * Tests '--all-caches' argument with the 'status' operation.
     */
    @Test
    public void testStatusAll() {
        createCachesWithMetrics(asMap(CACHE_ONE, false));

        checkExecutionSuccess(asMap(CACHE_ONE, false), STATUS_COMMAND, ALL_CACHES_ARGUMENT);

        createCachesWithMetrics(asMap(CACHE_TWO, true, NON_METRIC_CACHE, false));

        Map<String, Boolean> expectedMetricsStatuses = new HashMap<>();

        expectedMetricsStatuses.put(CACHE_ONE, false);
        expectedMetricsStatuses.put(CACHE_TWO, true);
        expectedMetricsStatuses.put(NON_METRIC_CACHE, false);

        checkExecutionSuccess(expectedMetricsStatuses, STATUS_COMMAND, ALL_CACHES_ARGUMENT);

        IgniteClusterEx cluster = grid(0).cluster();
        cluster.enableStatistics(Collections.singleton(CACHE_ONE), true);
        cluster.enableStatistics(Collections.singleton(CACHE_TWO), false);

        expectedMetricsStatuses.put(CACHE_ONE, true);
        expectedMetricsStatuses.put(CACHE_TWO, false);

        checkExecutionSuccess(expectedMetricsStatuses, STATUS_COMMAND, ALL_CACHES_ARGUMENT);
    }

    /**
     * Tests metrics status request for a non-existing caches.
     */
    @Test
    public void testNotFoundCacheStatus() {
        createCachesWithMetrics(asMap(CACHE_ONE, false));

        String cacheArg = String.join(",", CACHE_ONE, CACHE_TWO, NON_EXISTENT_CACHE);

        checkExecutionError(doesNotExist(CACHE_TWO), STATUS_COMMAND, CACHES_ARGUMENT, cacheArg);

        createCachesWithMetrics(asMap(CACHE_TWO, true));

        checkExecutionError(doesNotExist(NON_EXISTENT_CACHE), STATUS_COMMAND, CACHES_ARGUMENT, cacheArg);
    }

    /**
     * Tests commands on an empty cluster without caches.
     */
    @Test
    public void testEmptyCluster() {
        checkExecutionSuccess(Collections.emptyMap(), ENABLE_COMMAND, ALL_CACHES_ARGUMENT);
        checkExecutionSuccess(Collections.emptyMap(), DISABLE_COMMAND, ALL_CACHES_ARGUMENT);
        checkExecutionSuccess(Collections.emptyMap(), STATUS_COMMAND, ALL_CACHES_ARGUMENT);
    }

    /** */
    @Test
    public void testInvalidArguments() {
        String checkArgs = "Check arguments. ";

        // Check when no operation passed
        checkInvalidArguments(checkArgs + INCORRECT_METRICS_OPERATION_MESSAGE);

        // Check when unknown operation passed
        checkInvalidArguments(checkArgs + INCORRECT_METRICS_OPERATION_MESSAGE, "bad-command");

        // Check when no --caches/--all-caches arguments passed
        checkInvalidArguments(checkArgs + INCORRECT_CACHE_ARGUMENT_MESSAGE, ENABLE_COMMAND);
        checkInvalidArguments(checkArgs + INCORRECT_CACHE_ARGUMENT_MESSAGE, DISABLE_COMMAND);
        checkInvalidArguments(checkArgs + INCORRECT_CACHE_ARGUMENT_MESSAGE, STATUS_COMMAND);

        String invalidCacheListFullMsg = "Check arguments. Expected " + EXPECTED_CACHES_LIST_MESSAGE;

        // Check when --caches argument passed without list of caches
        checkInvalidArguments(invalidCacheListFullMsg, ENABLE_COMMAND, CACHES_ARGUMENT);
        checkInvalidArguments(invalidCacheListFullMsg, DISABLE_COMMAND, CACHES_ARGUMENT);
        checkInvalidArguments(invalidCacheListFullMsg, STATUS_COMMAND, CACHES_ARGUMENT);

        String incorrectCacheArgFullMsg = checkArgs + INCORRECT_CACHE_ARGUMENT_MESSAGE;

        // Check when unknown argument is passed after metric operation
        checkInvalidArguments(incorrectCacheArgFullMsg, ENABLE_COMMAND, "--arg");
        checkInvalidArguments(incorrectCacheArgFullMsg, DISABLE_COMMAND, "--arg");
        checkInvalidArguments(incorrectCacheArgFullMsg, STATUS_COMMAND, "--arg");

        String unexpectedCacheArgMsg = "Unexpected argument of --cache subcommand: ";

        // Check when extra argument passed after correct command
        checkInvalidArguments(unexpectedCacheArgMsg + "param", ENABLE_COMMAND, CACHES_ARGUMENT, CACHE_ONE, "param");
        checkInvalidArguments(unexpectedCacheArgMsg + "--arg", STATUS_COMMAND, ALL_CACHES_ARGUMENT, "--arg");
        checkInvalidArguments(unexpectedCacheArgMsg + CACHES_ARGUMENT, STATUS_COMMAND, ALL_CACHES_ARGUMENT,
            CACHES_ARGUMENT);

        // Check when after --caches argument extra argument is passed instead of list of caches
        checkInvalidArguments(unexpectedCacheArgMsg + "--arg", STATUS_COMMAND, CACHES_ARGUMENT, "--arg");

        checkInvalidArguments(unexpectedCacheArgMsg + ALL_CACHES_ARGUMENT, ENABLE_COMMAND, CACHES_ARGUMENT,
            ALL_CACHES_ARGUMENT);
    }

    /**
     * Check execution of metric operation and parse resulting table.
     *
     * @param expectedMetricsStatuses Expected table entries in command output.
     * @param args Command arguments.
     */
    private void checkExecutionSuccess(Map<String, Boolean> expectedMetricsStatuses, String... args) {
        exec(EXIT_CODE_OK, args);

        checkOutput(expectedMetricsStatuses, testOut.toString());

        checkClusterMetrics(expectedMetricsStatuses);
    }

    /**
     * @param expExitCode Expected exit code.
     * @param args Command arguments.
     */
    private void exec(int expExitCode, String... args) {
        String[] fullArgs = F.concat(new String[] {CACHE.text(), METRICS.text()}, args);

        int exitCode = execute(fullArgs);
        assertEquals("Unexpected exit code", expExitCode, exitCode);
    }

    /** */
    private void checkInvalidArguments(String expOut, String... args) {
        checkExecutionAndOutput(EXIT_CODE_INVALID_ARGUMENTS, expOut, args);
    }

    /** */
    private void checkExecutionError(String expOut, String... args) {
        checkExecutionAndOutput(EXIT_CODE_UNEXPECTED_ERROR, expOut, args);
    }

    /**
     * Check command execution and results.
     *
     * @param expExitCode Expected exit code.
     * @param expOut Expected command output.
     * @param args Command arguments.
     */
    private void checkExecutionAndOutput(int expExitCode, String expOut, String... args) {
        exec(expExitCode, args);

        GridTestUtils.assertContains(log, testOut.toString(), expOut);
    }

    /**
     * @param expectedMetricsStatuses Expected metrics statuses.
     * @param testOutStr Test output.
     */
    private void checkOutput(Map<String, Boolean> expectedMetricsStatuses, String testOutStr) {
        for (Map.Entry<String, Boolean> entry : expectedMetricsStatuses.entrySet()) {
            String cacheName = entry.getKey();
            String metricStatus = entry.getValue() ? "enabled" : "disabled";

            Matcher cacheStatusMatcher = Pattern.compile(cacheName + "\\s+" + metricStatus).matcher(testOutStr);

            String msg = String.format("Expected cache metrics status not found: [cacheName=%s, metricsStatus=%s]",
                cacheName, metricStatus);

            assertTrue(msg, cacheStatusMatcher.find());
        }
    }

    /**
     * @param cacheMetricsModes Metrics modes.
     */
    private void createCachesWithMetrics(Map<String, Boolean> cacheMetricsModes) {
        for (Map.Entry<String, Boolean> nameAndState : cacheMetricsModes.entrySet()) {
            grid(0).getOrCreateCache(new CacheConfiguration<>()
                .setName(nameAndState.getKey())
                .setStatisticsEnabled(nameAndState.getValue()));
        }

        checkClusterMetrics(cacheMetricsModes);
    }

    /**
     * @param expectedMetricsModes Expected cache metrics modes.
     */
    private void checkClusterMetrics(Map<String, Boolean> expectedMetricsModes) {
        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : expectedMetricsModes.keySet()) {
                Boolean cacheMetricsEnabled = ignite.cache(cacheName).metrics().isStatisticsEnabled();

                assertEquals("Unexpected metrics mode for cache: " + cacheName, expectedMetricsModes.get(cacheName),
                    cacheMetricsEnabled);
            }
        }
    }

    /**
     * Forms expected output for 'status' command when cache does not exist.
     *
     * @param cacheName Cache name.
     */
    private String doesNotExist(String cacheName) {
        return "Cache does not exist: " + cacheName;
    }

    /**
     * Forms expected output for 'enable' and 'disable' commands when caches do not exist.
     *
     * @param cacheNames Cache names.
     */
    private String descriptorsNotFound(String... cacheNames) {
        return "One or more cache descriptors not found [caches=" + Arrays.toString(cacheNames) + ']';
    }
}
