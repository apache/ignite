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
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.cache.CacheCommandList;
import org.apache.ignite.internal.commandline.cache.CacheMetrics;
import org.apache.ignite.internal.commandline.cache.argument.CacheMetricsCommandArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.cache.CacheMetrics.SEP;
import static org.apache.ignite.internal.util.lang.GridFunc.t;

/**
 * Test for {@link CacheMetrics} command.
 */
public class CacheMetricsCommandTest extends GridCommandHandlerAbstractTest {
    /** All caches flag. */
    private static final String ALL_CACHES = CacheMetricsCommandArg.ALL_CACHES.argName();

    /** Command enable. */
    private static final String COMMAND_ENABLE = CacheMetricsCommandArg.ENABLE.argName();

    /** Command disable. */
    private static final String COMMAND_DISABLE = CacheMetricsCommandArg.DISABLE.argName();

    /** Command status. */
    private static final String COMMAND_STATUS = CacheMetricsCommandArg.STATUS.argName();

    /** Cache one. */
    private static final String CACHE_ONE = "cache-1";

    /** Cache two. */
    private static final String CACHE_TWO = "cache-2";

    /** Cache three. */
    private static final String CACHE_THREE = "cache-3";

    /** Non-metric cache. */
    private static final String NON_METRIC_CACHE = "non-metric-cache";

    /** Not found cache. */
    private static final String NOT_FOUND_CACHE = "not-found-cache";
    /** Status disabled. */

    private static final String STATUS_DISABLED = "DISABLED";

    /** Status enabled. */
    private static final String STATUS_ENABLED = "ENABLED";

    /** Empty result message. */
    private static final String EMPTY_RESULT_MESSAGE = "Empty result: none of the specified caches were found.";

    /** Not found caches message. */
    private static final String NOT_FOUND_MESSAGE = "Not found caches:" + SEP;

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
        createCachesWithMetricStatuses(t(CACHE_ONE, false), t(CACHE_TWO, true), t(NON_METRIC_CACHE, false));

        checkExecutionOk(successToggle(CACHE_ONE), COMMAND_ENABLE, CACHE_ONE);
        checkExecutionOk(successToggle(CACHE_TWO), COMMAND_DISABLE, CACHE_TWO);
        checkClusterMetrics(t(CACHE_ONE, true), t(CACHE_TWO, false), t(NON_METRIC_CACHE, false));

        checkExecutionOk(successToggle(CACHE_ONE, CACHE_TWO), COMMAND_DISABLE, CACHE_ONE + ',' + CACHE_TWO);
        checkClusterMetrics(t(CACHE_ONE, false), t(CACHE_TWO, false), t(NON_METRIC_CACHE, false));

        // Cache list with duplicates and shuffled order
        String cacheNames = String.join(",", CACHE_TWO, CACHE_TWO, CACHE_ONE, CACHE_ONE, CACHE_TWO);

        checkExecutionOk(successToggle(CACHE_ONE, CACHE_TWO), COMMAND_ENABLE, cacheNames);
        checkClusterMetrics(t(CACHE_ONE, true), t(CACHE_TWO, true), t(NON_METRIC_CACHE, false));
    }

    /**
     * Tests <tt>--all-caches</tt> flag for the enable/disable commands.
     */
    @Test
    public void testEnableDisableAll() {
        createCachesWithMetricStatuses(t(CACHE_ONE, false));

        checkExecutionOk(successToggle(CACHE_ONE), COMMAND_ENABLE, ALL_CACHES);
        checkClusterMetrics(t(CACHE_ONE, true));

        checkExecutionOk(successToggle(CACHE_ONE), COMMAND_DISABLE, ALL_CACHES);
        checkClusterMetrics(t(CACHE_ONE, false));

        createCachesWithMetricStatuses(t(CACHE_TWO, true), t(CACHE_THREE, false));

        checkExecutionOk(successToggle(CACHE_ONE, CACHE_TWO, CACHE_THREE), COMMAND_DISABLE, ALL_CACHES);
        checkClusterMetrics(t(CACHE_ONE, false), t(CACHE_TWO, false), t(CACHE_THREE, false));

        checkExecutionOk(successToggle(CACHE_ONE, CACHE_TWO, CACHE_THREE), COMMAND_ENABLE, ALL_CACHES);
        checkClusterMetrics(t(CACHE_ONE, true), t(CACHE_TWO, true), t(CACHE_THREE, true));
    }

    /**
     * Tests metrics enabling/disabling for a non-existing caches.
     */
    @Test
    public void testNotFoundCacheEnableDisable() {
        createCachesWithMetricStatuses(t(CACHE_ONE, false));

        String cacheArg = String.join(",", CACHE_ONE, CACHE_TWO, NOT_FOUND_CACHE);

        String msgCacheTwoNotFoundCache = String.join(SEP, successToggle(CACHE_ONE),
            notFound(CACHE_TWO, NOT_FOUND_CACHE));

        checkExecutionOk(msgCacheTwoNotFoundCache, COMMAND_ENABLE, cacheArg);
        checkClusterMetrics(t(CACHE_ONE, true));

        createCachesWithMetricStatuses(t(CACHE_TWO, true));

        String msgNotFoundCache = String.join(SEP, successToggle(CACHE_ONE, CACHE_TWO), notFound(NOT_FOUND_CACHE));

        checkExecutionOk(msgNotFoundCache, COMMAND_DISABLE, cacheArg);
        checkClusterMetrics(t(CACHE_ONE, false), t(CACHE_TWO, false));
    }

    /**
     * Tests metrics status for some (not all) caches in a cluster.
     */
    @Test
    public void testStatus() {
        createCachesWithMetricStatuses(t(CACHE_ONE, false), t(CACHE_TWO, true), t(NON_METRIC_CACHE, false));

        checkExecutionOk(successStatus(t(CACHE_ONE, STATUS_DISABLED)), COMMAND_STATUS, CACHE_ONE);
        checkExecutionOk(successStatus(t(CACHE_TWO, STATUS_ENABLED)), COMMAND_STATUS, CACHE_TWO);

        IgniteClusterEx cluster = grid(0).cluster();
        cluster.enableStatistics(Collections.singleton(CACHE_ONE), true);
        cluster.enableStatistics(Collections.singleton(CACHE_TWO), false);

        checkExecutionOk(successStatus(t(CACHE_ONE, STATUS_ENABLED), t(CACHE_TWO, STATUS_DISABLED)), COMMAND_STATUS, 
            CACHE_ONE + ',' + CACHE_TWO);

        cluster.enableStatistics(Arrays.asList(CACHE_ONE, CACHE_TWO), true);

        // Cache list with duplicates and shuffled order
        String cacheNames = String.join(",", NON_METRIC_CACHE, CACHE_TWO, CACHE_ONE, CACHE_TWO, NON_METRIC_CACHE);

        checkExecutionOk(successStatus(t(CACHE_ONE, STATUS_ENABLED), t(CACHE_TWO, STATUS_ENABLED),
                t(NON_METRIC_CACHE, STATUS_DISABLED)), COMMAND_STATUS, cacheNames);
    }

    /**
     * Tests <tt>--all-caches</tt> flag for the status command.
     */
    @Test
    public void testStatusAll() {
        createCachesWithMetricStatuses(t(CACHE_ONE, false));

        checkExecutionOk(successStatus(t(CACHE_ONE, STATUS_DISABLED)), COMMAND_STATUS, ALL_CACHES);

        createCachesWithMetricStatuses(t(CACHE_TWO, true), t(CACHE_THREE, false));

        checkExecutionOk(successStatus(t(CACHE_ONE, STATUS_DISABLED), t(CACHE_TWO, STATUS_ENABLED), 
            t(CACHE_THREE, STATUS_DISABLED)), COMMAND_STATUS, ALL_CACHES);

        IgniteClusterEx cluster = grid(0).cluster();
        cluster.enableStatistics(Collections.singleton(CACHE_ONE), true);
        cluster.enableStatistics(Collections.singleton(CACHE_TWO), false);

        checkExecutionOk(successStatus(t(CACHE_ONE, STATUS_ENABLED), t(CACHE_TWO, STATUS_DISABLED),
            t(CACHE_THREE, STATUS_DISABLED)), COMMAND_STATUS, ALL_CACHES);
    }

    /**
     * Tests metrics status request for a non-existing caches.
     */
    @Test
    public void testNotFoundCacheStatus() {
        createCachesWithMetricStatuses(t(CACHE_ONE, false));

        String cacheArg = String.join(",", CACHE_ONE, CACHE_TWO, NOT_FOUND_CACHE);

        String msgCacheTwoNotFoundCache = String.join(SEP, successStatus(t(CACHE_ONE, STATUS_DISABLED)),
            notFound(CACHE_TWO, NOT_FOUND_CACHE));

        checkExecutionOk(msgCacheTwoNotFoundCache, COMMAND_STATUS, cacheArg);

        createCachesWithMetricStatuses(t(CACHE_TWO, true));

        String msgNotFoundCache = String.join(SEP, successStatus(t(CACHE_ONE, STATUS_DISABLED),
                t(CACHE_TWO, STATUS_ENABLED)), notFound(NOT_FOUND_CACHE));

        checkExecutionOk(msgNotFoundCache, COMMAND_STATUS, cacheArg);
    }

    /**
     * Tests cases when none of requested caches were found in a cluster.
     */
    @Test
    public void testEmptyResult() {
        checkExecutionOk(EMPTY_RESULT_MESSAGE, COMMAND_ENABLE, NOT_FOUND_CACHE);

        checkExecutionOk(EMPTY_RESULT_MESSAGE, COMMAND_DISABLE, NOT_FOUND_CACHE);

        checkExecutionOk(EMPTY_RESULT_MESSAGE, COMMAND_ENABLE, ALL_CACHES);

        checkExecutionOk(EMPTY_RESULT_MESSAGE, COMMAND_DISABLE, ALL_CACHES);

        checkExecutionOk(EMPTY_RESULT_MESSAGE, COMMAND_STATUS, NOT_FOUND_CACHE);

        checkExecutionOk(EMPTY_RESULT_MESSAGE, COMMAND_STATUS, ALL_CACHES);
    }

    /**
     *
     */
    @Test
    public void testInvalidArguments() {
        checkInvalidArguments("Check arguments. Expected correct sub-command.");

        checkInvalidArguments("Check arguments. Expected correct sub-command.", "bad-command");

        checkInvalidArguments("Check arguments. Expected comma-separated cache names list " +
            "or '--all-caches' argument.", COMMAND_ENABLE);

        checkInvalidArguments("Check arguments. Expected comma-separated cache names list " +
            "or '--all-caches' argument.", COMMAND_DISABLE);

        checkInvalidArguments("Check arguments. Expected comma-separated cache names list " +
            "or '--all-caches' argument.", COMMAND_STATUS);

        checkInvalidArguments("Check arguments. Unexpected argument of --cache subcommand: " + CACHE_TWO,
            COMMAND_ENABLE, CACHE_ONE, CACHE_TWO);
    }

    /**
     * Check commands successful execution.
     *
     * @param expExitCode Expected exit code.
     * @param expectedOutput Expected command output.
     * @param args Command arguments.
     */
    private void checkExecution(int expExitCode, String expectedOutput, boolean checkLastOperation, String... args) {
        String[] fullArgs = F.concat(new String[] {CommandList.CACHE.text(), CacheCommandList.METRICS.text()}, args);

        int exitCode = execute(fullArgs);
        assertEquals("Unexpected exit code", expExitCode, exitCode);

        if (checkLastOperation)
            assertEquals("Unexpected operation result", expectedOutput, lastOperationResult);

        GridTestUtils.assertContains(log, testOut.toString(), expectedOutput);
    }

    /**
     * 
     */
    private void checkExecutionOk(String expectedOutput, String... args) {
        checkExecution(EXIT_CODE_OK, expectedOutput, true, args);
    }

    /**
     *
     */
    private void checkInvalidArguments(String expectedOutput, String... args) {
        checkExecution(EXIT_CODE_INVALID_ARGUMENTS, expectedOutput, false, args);
    }

    /**
     * @param cacheMetricsStatuses Metrics statuses.
     */
    private void createCachesWithMetricStatuses(IgniteBiTuple<String, Boolean>... cacheMetricsStatuses) {
        for (IgniteBiTuple<String, Boolean> nameAndState : cacheMetricsStatuses) {
            grid(0).getOrCreateCache(new CacheConfiguration<>().
                setName(nameAndState.get1())
                .setStatisticsEnabled(nameAndState.get2()));
        }

        checkClusterMetrics(cacheMetricsStatuses);
    }

    /**
     * @param expectedMetricsStatuses Expected cache metrics statuses.
     */
    private void checkClusterMetrics(IgniteBiTuple<String, Boolean>... expectedMetricsStatuses) {
        for (Ignite ignite : G.allGrids()) {
            for (IgniteBiTuple<String, Boolean> nameAndState : expectedMetricsStatuses) {
                String cacheName = nameAndState.get1();
                boolean cacheMetricsEnabled = ignite.cache(cacheName).metrics().isStatisticsEnabled();

                assertEquals("Unexpected metrics status for cache: " + cacheName,
                    nameAndState.get2().booleanValue(), cacheMetricsEnabled);
            }
        }
    }

    /**
     * Form expected <tt>--enable/disable</tt> command output with processed caches list.
     *
     * @param cacheNames Cache names.
     */
    private String successToggle(String... cacheNames) {
        return "Command performed successfully for caches:" + SEP + Arrays.toString(cacheNames);
    }

    /**
     * Form expected <tt>--status</tt> command output with table of processed caches.
     *
     * @param expectedMetricsStatuses Expected metrics statuses.
     */
    private String successStatus(IgniteBiTuple<String, String>... expectedMetricsStatuses) {
        String tableHdr = "[Cache Name -> Status]:";

        String tableRows = Arrays.stream(expectedMetricsStatuses)
            .map(t -> t.get1() + " -> " + t.get2())
            .collect(Collectors.joining(SEP));

        return tableHdr + SEP + tableRows;
    }

    /**
     * Form expected command output with not found caches list.
     *
     * @param cacheNames Cache names.
     */
    private String notFound(String... cacheNames) {
        return NOT_FOUND_MESSAGE + Arrays.toString(cacheNames);
    }
}
