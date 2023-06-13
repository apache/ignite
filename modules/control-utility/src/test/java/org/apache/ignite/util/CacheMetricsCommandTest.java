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

import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.management.cache.CacheMetricsCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.visor.cache.metrics.CacheMetricsOperation;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.util.lang.GridFunc.asMap;
import static org.apache.ignite.util.GridCommandHandlerIndexingCheckSizeTest.CACHE;

/**
 * Test for {@link CacheMetricsCommand} command.
 */
public class CacheMetricsCommandTest extends GridCommandHandlerAbstractTest {
    /** Enable operation. */
    private static final String ENABLE = CacheMetricsOperation.ENABLE.toString();

    /** Disable operation. */
    private static final String DISABLE = CacheMetricsOperation.DISABLE.toString();

    /** Status operation. */
    private static final String STATUS = CacheMetricsOperation.STATUS.toString();

    /** Cache one. */
    private static final String CACHE_ONE = "cache-1";

    /** Cache two. */
    private static final String CACHE_TWO = "cache-2";

    /** */
    private static final String ALL_CACHES_ARGUMENT = "--all-caches";

    /** */
    private static final String CACHES_ARGUMENT = "--caches";

    /** Incorrect metrics operation message. */
    public static final String INCORRECT_METRICS_OPERATION_MESSAGE = "Argument operation required";

    /** Expected caches list message. */
    public static final String EXPECTED_CACHES_LIST_MESSAGE = "Please specify a value for argument: --caches";

    /** Incorrect cache argument message. */
    public static final String INCORRECT_CACHE_ARGUMENT_MESSAGE = "One of [--all-caches, --caches] required";

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
     * Tests metrics enable / disable operations.
     */
    @Test
    public void testEnableDisable() {
        // Test empty cluster
        checkExecutionSuccess(Collections.emptyMap(), ENABLE, ALL_CACHES_ARGUMENT);

        createCachesWithMetrics(asMap(CACHE_ONE, false, CACHE_TWO, true));

        checkExecutionSuccess(asMap(CACHE_ONE, true), ENABLE, CACHES_ARGUMENT, CACHE_ONE);
        checkExecutionSuccess(asMap(CACHE_TWO, false), DISABLE, CACHES_ARGUMENT, CACHE_TWO);

        // Cache list with duplicates
        String cacheNames = String.join(",", CACHE_TWO, CACHE_ONE, CACHE_ONE, CACHE_TWO);

        checkExecutionSuccess(asMap(CACHE_ONE, false, CACHE_TWO, false), DISABLE, CACHES_ARGUMENT, cacheNames);

        checkExecutionSuccess(asMap(CACHE_ONE, true, CACHE_TWO, true), ENABLE, ALL_CACHES_ARGUMENT);
    }

    /**
     * Tests metrics status operation.
     */
    @Test
    public void testStatus() {
        // Test empty cluster
        checkExecutionSuccess(Collections.emptyMap(), STATUS, ALL_CACHES_ARGUMENT);

        createCachesWithMetrics(asMap(CACHE_ONE, false, CACHE_TWO, true));

        checkExecutionSuccess(asMap(CACHE_ONE, false), STATUS, CACHES_ARGUMENT, CACHE_ONE);
        checkExecutionSuccess(asMap(CACHE_TWO, true), STATUS, CACHES_ARGUMENT, CACHE_TWO);

        // Cache list with duplicates
        String cacheNames = String.join(",", CACHE_TWO, CACHE_ONE, CACHE_ONE, CACHE_TWO);

        checkExecutionSuccess(asMap(CACHE_ONE, false, CACHE_TWO, true), STATUS, CACHES_ARGUMENT, cacheNames);

        checkExecutionSuccess(asMap(CACHE_ONE, false, CACHE_TWO, true), STATUS, ALL_CACHES_ARGUMENT);
    }

    /**
     * Tests metrics operations for a non-existing cache.
     */
    @Test
    public void testNotFoundCache() {
        createCachesWithMetrics(asMap(CACHE_ONE, false));

        String descriptorsNotFoundMsg = "One or more cache descriptors not found [caches=[" + CACHE_ONE + ", " +
            CACHE_TWO + ']';

        checkExecutionError(descriptorsNotFoundMsg, ENABLE, CACHES_ARGUMENT, CACHE_ONE + ',' + CACHE_TWO);

        // Check that metrics statuses was not changed
        checkClusterMetrics(asMap(CACHE_ONE, false));

        checkExecutionError("Cache does not exist: " + CACHE_TWO, STATUS, CACHES_ARGUMENT, CACHE_ONE + ',' + CACHE_TWO);
    }

    /** */
    @Test
    public void testInvalidArguments() {
        String checkArgs = "Check arguments. ";

        // Check when no operation passed
        checkInvalidArguments(checkArgs + INCORRECT_METRICS_OPERATION_MESSAGE);

        // Check when unknown operation passed
        checkInvalidArguments(checkArgs + "Can't parse value 'bad-command'", "bad-command");

        // Check when no --caches/--all-caches arguments passed
        checkInvalidArguments(checkArgs + INCORRECT_CACHE_ARGUMENT_MESSAGE, ENABLE);
        checkInvalidArguments(checkArgs + INCORRECT_CACHE_ARGUMENT_MESSAGE, STATUS);

        String invalidCacheListFullMsg = checkArgs + EXPECTED_CACHES_LIST_MESSAGE;

        // Check when --caches argument passed without list of caches
        checkInvalidArguments(invalidCacheListFullMsg, ENABLE, CACHES_ARGUMENT);
        checkInvalidArguments(invalidCacheListFullMsg, STATUS, CACHES_ARGUMENT);

        String incorrectCacheArgFullMsg = checkArgs + "Unexpected argument: --arg";

        // Check when unknown argument is passed after metric operation
        checkInvalidArguments(incorrectCacheArgFullMsg, ENABLE, "--arg");
        checkInvalidArguments(incorrectCacheArgFullMsg, STATUS, "--arg");

        String unexpectedCacheArgMsg = checkArgs + "Only one of [--all-caches, --caches] allowed";

        // Check when extra argument passed after correct command
        checkInvalidArguments(unexpectedCacheArgMsg, ENABLE, CACHES_ARGUMENT, CACHE_ONE,
            ALL_CACHES_ARGUMENT);
        checkInvalidArguments(unexpectedCacheArgMsg, STATUS, ALL_CACHES_ARGUMENT, CACHES_ARGUMENT,
            CACHE_ONE);

        // Check when after --caches argument extra argument is passed instead of list of caches
        checkInvalidArguments("Unexpected value: --all-caches", ENABLE, CACHES_ARGUMENT, ALL_CACHES_ARGUMENT);
    }

    /**
     * Check execution of metric operation and parse resulting table.
     *
     * @param expStatuses Expected table entries in command output.
     * @param args Command arguments.
     */
    private void checkExecutionSuccess(Map<String, Boolean> expStatuses, String... args) {
        exec(EXIT_CODE_OK, args);

        checkOutput(expStatuses, testOut.toString());

        checkClusterMetrics(expStatuses);
    }

    /**
     * @param expExitCode Expected exit code.
     * @param args Command arguments.
     */
    private void exec(int expExitCode, String... args) {
        String[] fullArgs = F.concat(new String[] {CACHE, "metrics"}, args);

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
     * @param expStatuses Expected metrics statuses.
     * @param testOutStr Test output.
     */
    private void checkOutput(Map<String, Boolean> expStatuses, String testOutStr) {
        for (Map.Entry<String, Boolean> entry : expStatuses.entrySet()) {
            String cacheName = entry.getKey();
            String metricsStatus = entry.getValue() ? "enabled" : "disabled";

            Matcher cacheStatusMatcher = Pattern.compile(cacheName + "\\s+" + metricsStatus).matcher(testOutStr);

            String msg = String.format("Unexpected count of table entries for metrics: [cacheName=%s, metricsStatus=%s]",
                cacheName, metricsStatus);

            int cnt = 0;

            while (cacheStatusMatcher.find())
                cnt++;

            assertEquals(msg, 1, cnt);
        }
    }

    /**
     * @param metricsStatuses Metrics statuses.
     */
    private void createCachesWithMetrics(Map<String, Boolean> metricsStatuses) {
        for (Map.Entry<String, Boolean> nameAndState : metricsStatuses.entrySet()) {
            grid(0).getOrCreateCache(new CacheConfiguration<>()
                .setName(nameAndState.getKey())
                .setStatisticsEnabled(nameAndState.getValue()));
        }

        checkClusterMetrics(metricsStatuses);
    }

    /**
     * @param expStatuses Expected cache metrics statuses.
     */
    private void checkClusterMetrics(Map<String, Boolean> expStatuses) {
        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : expStatuses.keySet()) {
                Boolean cacheMetricsEnabled = ignite.cache(cacheName).metrics().isStatisticsEnabled();

                assertEquals("Unexpected metrics mode for cache: " + cacheName, expStatuses.get(cacheName),
                    cacheMetricsEnabled);
            }
        }
    }
}
