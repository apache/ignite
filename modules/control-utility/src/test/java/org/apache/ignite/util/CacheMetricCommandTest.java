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
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.commandline.cache.CacheMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.metrics.CacheMetricOperation;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.regex.Pattern.quote;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.METRIC;
import static org.apache.ignite.internal.commandline.cache.CacheMetric.ALL_CACHES_ARGUMENT;
import static org.apache.ignite.internal.commandline.cache.CacheMetric.CACHES_ARGUMENT;
import static org.apache.ignite.internal.commandline.cache.CacheMetric.EXPECTED_CACHES_LIST_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.CacheMetric.INCORRECT_CACHE_ARGUMENT_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.CacheMetric.INCORRECT_METRIC_OPERATION_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.CacheMetric.NONE_CACHES_PROCESSED_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.CacheMetric.SUCCESS_MESSAGE;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommand.COLUMN_SEPARATOR;
import static org.apache.ignite.internal.util.lang.GridFunc.t;

/**
 * Test for {@link CacheMetric} command.
 */
public class CacheMetricCommandTest extends GridCommandHandlerAbstractTest {
    /** Enable operation. */
    private static final String ENABLE_COMMAND = CacheMetricOperation.ENABLE.toString();

    /** Disable operation. */
    private static final String DISABLE_COMMAND = CacheMetricOperation.DISABLE.toString();

    /** Status operation. */
    private static final String STATUS_COMMAND = CacheMetricOperation.STATUS.toString();

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

    /** Status disabled. */
    private static final String STATUS_DISABLED = "disabled";

    /** Status enabled. */
    private static final String STATUS_ENABLED = "enabled";

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
        createCachesWithMetricsModes(t(CACHE_ONE, false), t(CACHE_TWO, true), t(NON_METRIC_CACHE, false));

        checkExecutionSuccess(SUCCESS_MESSAGE, ENABLE_COMMAND, CACHES_ARGUMENT, CACHE_ONE);
        checkExecutionSuccess(SUCCESS_MESSAGE, DISABLE_COMMAND, CACHES_ARGUMENT, CACHE_TWO);
        checkClusterMetrics(t(CACHE_ONE, true), t(CACHE_TWO, false), t(NON_METRIC_CACHE, false));

        checkExecutionSuccess(SUCCESS_MESSAGE, DISABLE_COMMAND, CACHES_ARGUMENT, CACHE_ONE + ',' + CACHE_TWO);
        checkClusterMetrics(t(CACHE_ONE, false), t(CACHE_TWO, false), t(NON_METRIC_CACHE, false));

        // Cache list with duplicates and shuffled order
        String cacheNames = String.join(",", CACHE_TWO, CACHE_TWO, CACHE_ONE, CACHE_ONE, CACHE_TWO);

        checkExecutionSuccess(SUCCESS_MESSAGE, ENABLE_COMMAND, CACHES_ARGUMENT, cacheNames);
        checkClusterMetrics(t(CACHE_ONE, true), t(CACHE_TWO, true), t(NON_METRIC_CACHE, false));
    }

    /**
     * Tests <tt>--all-caches</tt> option for the enable/disable sub-commands.
     */
    @Test
    public void testEnableDisableAll() {
        createCachesWithMetricsModes(t(CACHE_ONE, false));

        checkExecutionSuccess(SUCCESS_MESSAGE, ENABLE_COMMAND, ALL_CACHES_ARGUMENT);
        checkClusterMetrics(t(CACHE_ONE, true));

        checkExecutionSuccess(SUCCESS_MESSAGE, DISABLE_COMMAND, ALL_CACHES_ARGUMENT);
        checkClusterMetrics(t(CACHE_ONE, false));

        createCachesWithMetricsModes(t(CACHE_TWO, true), t(CACHE_THREE, false));

        checkExecutionSuccess(SUCCESS_MESSAGE, DISABLE_COMMAND, ALL_CACHES_ARGUMENT);
        checkClusterMetrics(t(CACHE_ONE, false), t(CACHE_TWO, false), t(CACHE_THREE, false));

        checkExecutionSuccess(SUCCESS_MESSAGE, ENABLE_COMMAND, ALL_CACHES_ARGUMENT);
        checkClusterMetrics(t(CACHE_ONE, true), t(CACHE_TWO, true), t(CACHE_THREE, true));
    }

    /**
     * Tests metrics enabling/disabling for a non-existing caches.
     */
    @Test
    public void testNotFoundCacheEnableDisable() {
        createCachesWithMetricsModes(t(CACHE_ONE, false));

        String cacheArg = String.join(",", CACHE_ONE, CACHE_TWO, NON_EXISTENT_CACHE);

        checkExecutionError(descriptorsNotFound(CACHE_ONE, CACHE_TWO, NON_EXISTENT_CACHE), ENABLE_COMMAND, CACHES_ARGUMENT,
            cacheArg);

        createCachesWithMetricsModes(t(CACHE_TWO, true));

        checkExecutionError(descriptorsNotFound(CACHE_ONE, CACHE_TWO, NON_EXISTENT_CACHE), DISABLE_COMMAND, CACHES_ARGUMENT,
            cacheArg);
    }

    /**
     * Tests metrics status for some (not all) caches in a cluster.
     */
    @Test
    public void testStatus() {
        createCachesWithMetricsModes(t(CACHE_ONE, false), t(CACHE_TWO, true), t(NON_METRIC_CACHE, false));

        checkStatusExecutionSuccess(toMap(t(CACHE_ONE, STATUS_DISABLED)), CACHES_ARGUMENT, CACHE_ONE);
        checkStatusExecutionSuccess(toMap(t(CACHE_TWO, STATUS_ENABLED)), CACHES_ARGUMENT, CACHE_TWO);

        IgniteClusterEx cluster = grid(0).cluster();
        cluster.enableStatistics(Collections.singleton(CACHE_ONE), true);
        cluster.enableStatistics(Collections.singleton(CACHE_TWO), false);

        checkStatusExecutionSuccess(toMap(t(CACHE_ONE, STATUS_ENABLED), t(CACHE_TWO, STATUS_DISABLED)),
            CACHES_ARGUMENT, CACHE_ONE + ',' + CACHE_TWO);

        cluster.enableStatistics(Arrays.asList(CACHE_ONE, CACHE_TWO), true);

        // Cache list with duplicates and shuffled order
        String cacheNames = String.join(",", NON_METRIC_CACHE, CACHE_TWO, CACHE_ONE, CACHE_TWO, NON_METRIC_CACHE);

        checkStatusExecutionSuccess(toMap(t(CACHE_ONE, STATUS_ENABLED), t(CACHE_TWO, STATUS_ENABLED),
                t(NON_METRIC_CACHE, STATUS_DISABLED)), CACHES_ARGUMENT, cacheNames);
    }

    /**
     * Tests <tt>--all-caches</tt> option for the status sub-command.
     */
    @Test
    public void testStatusAll() {
        createCachesWithMetricsModes(t(CACHE_ONE, false));

        checkStatusExecutionSuccess(toMap(t(CACHE_ONE, STATUS_DISABLED)), ALL_CACHES_ARGUMENT);

        createCachesWithMetricsModes(t(CACHE_TWO, true), t(CACHE_THREE, false));

        checkStatusExecutionSuccess(toMap(t(CACHE_ONE, STATUS_DISABLED), t(CACHE_TWO, STATUS_ENABLED),
            t(CACHE_THREE, STATUS_DISABLED)), ALL_CACHES_ARGUMENT);

        IgniteClusterEx cluster = grid(0).cluster();
        cluster.enableStatistics(Collections.singleton(CACHE_ONE), true);
        cluster.enableStatistics(Collections.singleton(CACHE_TWO), false);

        checkStatusExecutionSuccess(toMap(t(CACHE_ONE, STATUS_ENABLED), t(CACHE_TWO, STATUS_DISABLED),
            t(CACHE_THREE, STATUS_DISABLED)), ALL_CACHES_ARGUMENT);
    }

    /**
     * Tests metrics status request for a non-existing caches.
     */
    @Test
    public void testNotFoundCacheStatus() {
        createCachesWithMetricsModes(t(CACHE_ONE, false));

        String cacheArg = String.join(",", CACHE_ONE, CACHE_TWO, NON_EXISTENT_CACHE);

        checkExecutionError(doesNotExist(CACHE_TWO), STATUS_COMMAND, CACHES_ARGUMENT, cacheArg);

        createCachesWithMetricsModes(t(CACHE_TWO, true));

        checkExecutionError(doesNotExist(NON_EXISTENT_CACHE), STATUS_COMMAND, CACHES_ARGUMENT, cacheArg);
    }

    /**
     * Tests commands on an empty cluster without caches.
     */
    @Test
    public void testNoCachesProcessed() {
        checkExecutionSuccess(NONE_CACHES_PROCESSED_MESSAGE, ENABLE_COMMAND, ALL_CACHES_ARGUMENT);

        checkExecutionSuccess(NONE_CACHES_PROCESSED_MESSAGE, DISABLE_COMMAND, ALL_CACHES_ARGUMENT);

        checkExecutionSuccess(NONE_CACHES_PROCESSED_MESSAGE, STATUS_COMMAND, ALL_CACHES_ARGUMENT);
    }

    /** */
    @Test
    public void testInvalidArguments() {
        String checkArgs = "Check arguments. ";

        // Check when no operation passed
        checkInvalidArguments(checkArgs + INCORRECT_METRIC_OPERATION_MESSAGE);

        // Check when unknown operation passed
        checkInvalidArguments(checkArgs + INCORRECT_METRIC_OPERATION_MESSAGE, "bad-command");

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
     * Check command execution status and results.
     *
     * @param expExitCode Expected exit code.
     * @param expectedOutput Expected command output.
     * @param args Command arguments.
     */
    private void checkExecutionStatusAndOutput(int expExitCode, String expectedOutput, String... args) {
        exec(expExitCode, args);

        GridTestUtils.assertContains(log, testOut.toString(), expectedOutput);
    }

    /**
     * @param expExitCode Expected exit code.
     * @param args Command arguments.
     */
    private void exec(int expExitCode, String... args) {
        String[] fullArgs = F.concat(new String[] {CACHE.text(), METRIC.text()}, args);

        int exitCode = execute(fullArgs);
        assertEquals("Unexpected exit code", expExitCode, exitCode);
    }

    /** */
    private void checkExecutionSuccess(String expectedOutput, String... args) {
        checkExecutionStatusAndOutput(EXIT_CODE_OK, expectedOutput, args);
    }

    /** */
    private void checkInvalidArguments(String expectedOutput, String... args) {
        checkExecutionStatusAndOutput(EXIT_CODE_INVALID_ARGUMENTS, expectedOutput, args);
    }

    /** */
    private void checkExecutionError(String expectedOutput, String... args) {
        checkExecutionStatusAndOutput(EXIT_CODE_UNEXPECTED_ERROR, expectedOutput, args);
    }

    /**
     * Check execution of 'status' command and resulting table.
     *
     * @param expEntries Expected entries for output of status sub-command.
     * @param statusArgs Status args.
     */
    private void checkStatusExecutionSuccess(Map<String, String> expEntries, String... statusArgs) {
        exec(EXIT_CODE_OK, F.concat(new String[] {STATUS_COMMAND}, statusArgs));

        Map<String, String> resEntries = parseTableResult(testOut.toString());

        assertEqualsMaps(expEntries, resEntries);
    }

    /**
     * @param out Test output.
     */
    private Map<String, String> parseTableResult(String out) {
        String outStart = "--------------------------------------------------------------------------------";

        String outEnd = "Command [" + CACHE.toCommandName() + "] finished with code: " + EXIT_CODE_OK;

        String[] rows = out.substring(
            out.indexOf(outStart) + outStart.length() + 1,
            out.indexOf(outEnd) - 1
        ).split(U.nl());

        Map<String, String> res = new HashMap<>();

        for (String row : rows) {
            Iterator<String> iter = Arrays.stream(row.split(quote(COLUMN_SEPARATOR)))
                .map(String::trim)
                .filter(str -> !str.isEmpty())
                .iterator();

            res.put(iter.next(), iter.next());
        }

        // Remove entry with table header
        res.remove("Cache Name");

        return res;
    }

    /**
     * @param cacheMetricsModes Metrics modes.
     */
    private void createCachesWithMetricsModes(IgniteBiTuple<String, Boolean>... cacheMetricsModes) {
        for (IgniteBiTuple<String, Boolean> nameAndState : cacheMetricsModes) {
            grid(0).getOrCreateCache(new CacheConfiguration<>().
                setName(nameAndState.get1())
                .setStatisticsEnabled(nameAndState.get2()));
        }

        checkClusterMetrics(cacheMetricsModes);
    }

    /**
     * @param expectedMetricsModes Expected cache metrics modes.
     */
    private void checkClusterMetrics(IgniteBiTuple<String, Boolean>... expectedMetricsModes) {
        for (Ignite ignite : G.allGrids()) {
            for (IgniteBiTuple<String, Boolean> nameAndState : expectedMetricsModes) {
                String cacheName = nameAndState.get1();
                boolean cacheMetricsEnabled = ignite.cache(cacheName).metrics().isStatisticsEnabled();

                assertEquals("Unexpected metrics mode for cache: " + cacheName,
                    nameAndState.get2().booleanValue(), cacheMetricsEnabled);
            }
        }
    }

    /** */
    private Map<String, String> toMap(IgniteBiTuple<String, String>... tuples) {
        Map<String, String> entries = new HashMap<>();

        for (IgniteBiTuple<String, String> mode : tuples)
            entries.put(mode.getKey(), mode.getValue());

        return entries;
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
