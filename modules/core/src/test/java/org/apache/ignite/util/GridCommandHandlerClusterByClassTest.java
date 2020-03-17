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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.CommonArgParser;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.NotNull;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.TestStorageUtils.corruptDataEntry;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.CommandList.BASELINE;
import static org.apache.ignite.internal.commandline.CommandList.WAL;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_PRINT_ERR_STACK_TRACE;
import static org.apache.ignite.internal.commandline.OutputFormat.MULTI_LINE;
import static org.apache.ignite.internal.commandline.OutputFormat.SINGLE_LINE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.HELP;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.LOCAL_OUTPUT;
import static org.apache.ignite.internal.commandline.cache.argument.PartitionReconciliationCommandArg.RECHECK_DELAY;
import static org.apache.ignite.testframework.GridTestUtils.LOCAL_DATETIME_REGEXP;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertMatches;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.GridTestUtils.readResource;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Command line handler test.
 * You can use this class if you don't need create nodes for each test because
 * here create {@link #SERVER_NODE_CNT} server and 1 client nodes at before all
 * tests. If you need create nodes for each test you can use
 * {@link GridCommandHandlerTest}
 */
public class GridCommandHandlerClusterByClassTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Special word for defining any char sequence from special word to the end of line in golden copy of help output */
    private static final String ANY = "<!any!>";

    /**
     * Very basic tests for running the command in different enviroment which other command are running in.
     */
    public void testFindAndDeleteGarbage() {
        Ignite ignite = crd;

        injectTestSystemOut();

        ignite.createCaches(Arrays.asList(
            new CacheConfiguration<>("garbage1").setGroupName("groupGarbage"),
            new CacheConfiguration<>("garbage2").setGroupName("groupGarbage")));

        assertEquals(EXIT_CODE_OK, execute("--cache", "find_garbage", "--port", "11212"));

        assertContains(log, testOut.toString(), "garbage not found");

        assertEquals(EXIT_CODE_OK, execute("--cache", "find_garbage",
            ignite(0).localNode().id().toString(), "--port", "11212"));

        assertContains(log, testOut.toString(), "garbage not found");

        assertEquals(EXIT_CODE_OK, execute("--cache", "find_garbage",
            "groupGarbage", "--port", "11212"));

        assertContains(log, testOut.toString(), "garbage not found");
    }

    /**
     * Smoke test for --tx --info command.
     */
    public void testTransactionInfo() throws Exception {
        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL).setBackups(1).setWriteSynchronizationMode(FULL_SYNC));

        for (Ignite ig : G.allGrids())
            assertNotNull(ig.cache(DEFAULT_CACHE_NAME));

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = startTransactions("testTransactionInfo", lockLatch, unlockLatch, false);

        try {
            U.awaitQuiet(lockLatch);

            doSleep(3000); // Should be more than enough for all transactions to appear in contexts.

            Set<GridCacheVersion> nearXids = new HashSet<>();

            for (int i = 0; i < SERVER_NODE_CNT; i++) {
                IgniteEx grid = grid(i);

                IgniteTxManager tm = grid.context().cache().context().tm();

                for (IgniteInternalTx tx : tm.activeTransactions())
                    nearXids.add(tx.nearXidVersion());
            }

            injectTestSystemOut();

            for (GridCacheVersion nearXid : nearXids) {
                assertEquals(EXIT_CODE_OK, execute("--tx", "--info", nearXid.toString()));

                String out = testOut.toString();

                assertContains(log, out, nearXid.toString());
            }
        }
        finally {
            unlockLatch.countDown();

            fut.get();
        }
    }

    /**
     * Smoke test for historical mode of --tx --info command.
     */
    public void testTransactionHistoryInfo() throws Exception {
        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL).setBackups(2).setWriteSynchronizationMode(FULL_SYNC));

        for (Ignite ig : G.allGrids())
            assertNotNull(ig.cache(DEFAULT_CACHE_NAME));

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = startTransactions("testTransactionHistoryInfo", lockLatch, unlockLatch, false);

        U.awaitQuiet(lockLatch);

        doSleep(3000); // Should be more than enough for all transactions to appear in contexts.

        Set<GridCacheVersion> nearXids = new HashSet<>();

        for (int i = 0; i < SERVER_NODE_CNT; i++) {
            IgniteEx grid = grid(i);

            IgniteTxManager tm = grid.context().cache().context().tm();

            for (IgniteInternalTx tx : tm.activeTransactions())
                nearXids.add(tx.nearXidVersion());
        }

        unlockLatch.countDown();

        fut.get();

        doSleep(3000); // Should be more than enough for all transactions to disappear from contexts after finish.

        injectTestSystemOut();

        boolean commitMatched = false;
        boolean rollbackMatched = false;

        for (GridCacheVersion nearXid : nearXids) {
            assertEquals(EXIT_CODE_OK, execute("--tx", "--info", nearXid.toString()));

            String out = testOut.toString();

            assertContains(log, out, "Transaction was found in completed versions history of the following nodes:");

            if (out.contains(TransactionState.COMMITTED.name())) {
                commitMatched = true;

                assertNotContains(log, out, TransactionState.ROLLED_BACK.name());
            }

            if (out.contains(TransactionState.ROLLED_BACK.name())) {
                rollbackMatched = true;

                assertNotContains(log, out, TransactionState.COMMITTED.name());
            }

        }

        assertTrue(commitMatched);
        assertTrue(rollbackMatched);
    }

    /** */
    public void testCacheHelp() throws Exception {
        Set<String> skippedCommands = new HashSet<>();
        skippedCommands.add(RECHECK_DELAY.toString());
        skippedCommands.add(LOCAL_OUTPUT.toString());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "help"));

        String output = testOut.toString();

        for (CacheSubcommands cmd : CacheSubcommands.values()) {
            if (cmd != HELP) {
                assertContains(log, output, cmd.toString());

                Class<? extends Enum<? extends CommandArg>> args = cmd.getCommandArgs();

                if (args != null)
                    for (Enum<? extends CommandArg> arg : args.getEnumConstants())
                        if (!skippedCommands.contains(arg.toString()))
                            assertTrue(cmd + " " + arg, output.contains(arg.toString()));
            }
            else
                assertContains(log, output, CommandHandler.UTILITY_NAME);
        }

        checkHelp(output, "org.apache.ignite.util/control.sh_cache_help.output");
    }

    /** */
    public void testCorrectCacheOptionsNaming() {
        Pattern p = Pattern.compile("^--([a-z]+(-)?)+([a-z]+)");

        for (CacheSubcommands cmd : CacheSubcommands.values()) {
            Class<? extends Enum<? extends CommandArg>> args = cmd.getCommandArgs();

            if (args != null)
                for (Enum<? extends CommandArg> arg : args.getEnumConstants())
                    assertTrue(arg.toString(), p.matcher(arg.toString()).matches());
        }
    }

    /** */
    public void testHelp() throws Exception {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--help"));

        String testOutStr = testOut.toString();

        for (CommandList cmd : CommandList.values())
            assertContains(log, testOutStr, cmd.toString());

        assertNotContains(log, testOutStr, "Control.sh");

        checkHelp(testOutStr, "org.apache.ignite.util/control.sh_help.output");
    }

    /**
     * Checks that golden copy of output and current output are same.
     *
     * @param output Current output.
     * @param resourceName Name of resource with golden copy on output.
     * @throws Exception If something goes wrong.
     */
    private void checkHelp(String output, String resourceName) throws Exception {
        String correctOutput = new String(readResource(getClass().getClassLoader(), resourceName));

        try {
            // Split by lines.
            List<String> correctOutputLines = U.sealList(correctOutput.split("\\r?\\n"));
            List<String> outputLines = U.sealList(output.split("\\r?\\n"));

            assertEquals("Wrong number of lines! Golden copy resource: " + resourceName, correctOutputLines.size(), outputLines.size());

            for (int i = 0; i < correctOutputLines.size(); i++) {
                String cLine = correctOutputLines.get(i);
                // Remove all spaces from end of line.
                String line = outputLines.get(i).replaceAll("\\s+$", "");

                if (cLine.contains(ANY)) {
                    String cuttedCLine = cLine.substring(0, cLine.length() - ANY.length());

                    assertTrue("line: " + i, line.startsWith(cuttedCLine));
                }
                else
                    assertEquals("line: " + i, cLine, line);
            }
        }
        catch (AssertionError e) {
            log.info("Correct output is: " + correctOutput);

            throw e;
        }
    }

    /** */
    public void testPrintTimestampAtEndsOfExecution() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute());

        String testOutStr = testOut.toString();

        assertMatches(log, testOutStr, "Time: " + LOCAL_DATETIME_REGEXP);
        assertMatches(log, testOutStr, "Control utility has completed execution at: " + LOCAL_DATETIME_REGEXP);
        assertMatches(log, testOutStr, "Execution time: \\d+ ms");

    }

    /** */
    public void testCacheIdleVerify() {
        IgniteEx ignite = crd;

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "no conflicts have been found");

        HashSet<Integer> clearKeys = new HashSet<>(asList(1, 2, 3, 4, 5, 6));

        ignite.context().cache().cache(DEFAULT_CACHE_NAME).clearLocallyAll(clearKeys, true, true, true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "conflict partitions");
    }

    /** */
    public void testCacheIdleVerifyNodeFilter() {
        IgniteEx ignite = crd;

        Object lastNodeCId = ignite.localNode().consistentId();

        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setNodeFilter(node -> !node.consistentId().equals(lastNodeCId))
            .setBackups(1));

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; i++)
                streamer.addData(i, i);
        }

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", DEFAULT_CACHE_NAME));

        assertContains(log, testOut.toString(), "no conflicts have been found");
    }

    /**
     * Tests that both update counter and hash conflicts are detected.
     */
    public void testCacheIdleVerifyTwoConflictTypes() {
        IgniteEx ignite = crd;

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "no conflicts have been found");

        GridCacheContext<Object, Object> cacheCtx = ignite.cachex(DEFAULT_CACHE_NAME).context();

        corruptDataEntry(cacheCtx, 1, true, false, new GridCacheVersion(0, 0, 0), "broken");

        corruptDataEntry(cacheCtx, 1 + cacheCtx.config().getAffinity().partitions() / 2, false, true, new GridCacheVersion(0, 0, 0), "broken");

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "found 2 conflict partitions");
    }

    /**
     * Tests that empty partitions with non-zero update counter are not included into the idle_verify dump.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyDumpSkipZerosUpdateCounters() throws Exception {
        IgniteEx ignite = crd;

        int emptyPartId = 31;

        // Less than parts number for ability to check skipZeros flag.
        createCacheAndPreload(ignite, emptyPartId);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--skip-zeros", DEFAULT_CACHE_NAME));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        assertTrue(fileNameMatcher.find());

        String zeroUpdateCntrs = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

        assertContains(log, zeroUpdateCntrs, "idle_verify check has finished, found " + emptyPartId + " partitions");
        assertContains(log, zeroUpdateCntrs, "1 partitions was skipped");
        assertContains(log, zeroUpdateCntrs, "idle_verify check has finished, no conflicts have been found.");

        assertSort(emptyPartId, zeroUpdateCntrs);

        // The result of the following cache operations is that
        // the size of the 32-th partition is equal to zero and update counter is equal to 2.
        ignite.cache(DEFAULT_CACHE_NAME).put(emptyPartId, emptyPartId);

        ignite.cache(DEFAULT_CACHE_NAME).remove(emptyPartId, emptyPartId);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--skip-zeros", DEFAULT_CACHE_NAME));

        fileNameMatcher = dumpFileNameMatcher();

        assertTrue(fileNameMatcher.find());

        String nonZeroUpdateCntrs = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

        assertContains(log, nonZeroUpdateCntrs, "idle_verify check has finished, found " + 31 + " partitions");
        assertContains(log, nonZeroUpdateCntrs, "1 partitions was skipped");
        assertContains(log, nonZeroUpdateCntrs, "idle_verify check has finished, no conflicts have been found.");

        assertSort(31, zeroUpdateCntrs);

        assertEquals(zeroUpdateCntrs, nonZeroUpdateCntrs);
    }

    /**
     * Tests that idle verify print partitions info.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyDump() throws Exception {
        IgniteEx ignite = crd;

        int keysCount = 20;//less than parts number for ability to check skipZeros flag.

        createCacheAndPreload(ignite, keysCount);

        int parts = ignite.affinity(DEFAULT_CACHE_NAME).partitions();

        ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME + "other"));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", DEFAULT_CACHE_NAME));

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "-6-dump", "--skip-zeros", DEFAULT_CACHE_NAME));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithZeros = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithZeros, "idle_verify check has finished, found " + parts + " partitions");
            assertContains(log, dumpWithZeros, "Partition: PartitionKeyV2 [grpId=1544803905, grpName=default, partId=0]");
            assertContains(log, dumpWithZeros, "updateCntr=0, size=0, partHash=0");
            assertContains(log, dumpWithZeros, "no conflicts have been found");

            assertSort(parts, dumpWithZeros);
        }

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--skip-zeros", DEFAULT_CACHE_NAME));

        fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithoutZeros = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithoutZeros, "idle_verify check has finished, found " + keysCount + " partitions");
            assertContains(log, dumpWithoutZeros, (parts - keysCount) + " partitions was skipped");
            assertContains(log, dumpWithoutZeros, "Partition: PartitionKeyV2 [grpId=1544803905, grpName=default, partId=");

            assertNotContains(log, dumpWithoutZeros, "updateCntr=0, size=0, partHash=0");

            assertContains(log, dumpWithoutZeros, "no conflicts have been found");

            assertSort(keysCount, dumpWithoutZeros);
        }
        else
            fail("Should be found both files");
    }

    /**
     * Common method for idle_verify tests with multiple options.
     */
    public void testCacheIdleVerifyMultipleCacheFilterOptions()
            throws Exception {
        IgniteEx ignite = crd;

        ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setGroupName("shared_grp")
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME));

        ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setGroupName("shared_grp")
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME + "_second"));

        ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 64))
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME + "_third"));

        ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 128))
                .setBackups(1)
                .setName("wrong_cache"));

        injectTestSystemOut();

        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found",
            "idle_verify task was executed with the following args: caches=[], excluded=[wrong.*], cacheFilter=[SYSTEM]",
            "--cache", "idle_verify", "--dump", "--cache-filter", "SYSTEM", "--exclude-caches", "wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 96 partitions",
            null,
            "--cache", "idle_verify", "--dump", "--exclude-caches", "wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 32 partitions",
            null,
            "--cache", "idle_verify", "--dump", "shared.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 160 partitions",
            null,
            "--cache", "idle_verify", "--dump", "shared.*,wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 160 partitions",
            null,
            "--cache", "idle_verify", "--dump", "shared.*,wrong.*", "--cache-filter", "USER"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, found 160 partitions",
            null,
            "--cache", "idle_verify", "--dump", "shared.*,wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "There are no caches matching given filter options",
            null,
            "--cache", "idle_verify", "--exclude-caches", ".*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            false,
            "Invalid cache name regexp",
            null,
            "--cache", "idle_verify", "--dump", "--exclude-caches", "["
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, no conflicts have been found.",
            null,
            "--cache", "idle_verify", "--exclude-caches", "wrong.*"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "idle_verify check has finished, no conflicts have been found.",
            null,
            "--cache", "idle_verify", "--dump", "--cache-filter", "PERSISTENT"
        );
        testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
            true,
            "There are no caches matching given filter options.",
            null,
            "--cache", "idle_verify", "--cache-filter", "NOT_PERSISTENT"
        );
    }

    /**
     * Runs idle_verify with specified arguments and checks the dump if dump option was present.
     *
     * @param exitOk whether CommandHandler should exit without errors
     * @param outputExp expected dump output
     * @param cmdExp expected command built from command line arguments
     * @param args command handler arguments
     * @throws IOException if some of file operations failed
     */
    private void testCacheIdleVerifyMultipleCacheFilterOptionsCommon(
        boolean exitOk,
        String outputExp,
        String cmdExp,
        String... args
    ) throws IOException {
        Set<String> argsSet = new HashSet<>(asList(args));

        int exitCode = execute(args);

        assertEquals(testOut.toString(), exitOk, EXIT_CODE_OK == exitCode);

        if (exitCode == EXIT_CODE_OK) {
            Matcher fileNameMatcher = dumpFileNameMatcher();

            if (fileNameMatcher.find()) {
                assertContains(log, argsSet, "--dump");

                Path filePath = Paths.get(fileNameMatcher.group(1));

                String dump = new String(Files.readAllBytes(filePath));

                Files.delete(filePath);

                assertContains(log, dump, outputExp);

                if (cmdExp != null)
                    assertContains(log, dump, cmdExp);
            }
            else {
                assertNotContains(log, argsSet, "--dump");

                assertContains(log, testOut.toString(), outputExp);
            }
        } else
            assertContains(log, testOut.toString(), outputExp);
    }

    /**
     * Checking sorting of partitions.
     *
     * @param expectedPartsCount Expected parts count.
     * @param output Output.
     */
    private void assertSort(int expectedPartsCount, String output) {
        Pattern partIdPattern = Pattern.compile(".*partId=([0-9]*)");
        Pattern primaryPattern = Pattern.compile("Partition instances: \\[PartitionHashRecordV2 \\[isPrimary=true");

        Matcher partIdMatcher = partIdPattern.matcher(output);
        Matcher primaryMatcher = primaryPattern.matcher(output);

        int i = 0;

        while (partIdMatcher.find()) {
            assertEquals(i++, Integer.parseInt(partIdMatcher.group(1)));
            assertTrue(primaryMatcher.find());//primary node should be first in every line
        }

        assertEquals(expectedPartsCount, i);
    }

    /**
     * Tests that idle verify print partitions info.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyDumpForCorruptedData() throws Exception {
        IgniteEx ignite = crd;

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        corruptingAndCheckDefaultCache(ignite, "USER", true);
    }

    /**
     * Tests that idle verify print partitions info while launched without dump option.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyForCorruptedData() throws Exception {
        IgniteEx ignite = crd;

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        corruptingAndCheckDefaultCache(ignite, "USER", false);
    }

    /**
     * @param ignite Ignite.
     * @param cacheFilter cacheFilter.
     * @param dump Whether idle_verify should be launched with dump option or not.
     */
    private void corruptingAndCheckDefaultCache(IgniteEx ignite, String cacheFilter, boolean dump) throws IOException {
        injectTestSystemOut();

        GridCacheContext<Object, Object> cacheCtx = ignite.cachex(DEFAULT_CACHE_NAME).context();

        corruptDataEntry(cacheCtx, 0, true, false, new GridCacheVersion(0, 0, 0), "broken");

        corruptDataEntry(cacheCtx, cacheCtx.config().getAffinity().partitions() / 2, false, true, new GridCacheVersion(0, 0, 0), "broken");

        String resReport = null;

        if (dump) {
            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--cache-filter", cacheFilter));

            Matcher fileNameMatcher = dumpFileNameMatcher();

            if (fileNameMatcher.find()) {
                resReport = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

                log.info(resReport);
            }
            else
                fail("Should be found dump with conflicts");
        }
        else {
            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--cache-filter", cacheFilter));

            resReport = testOut.toString();
        }

        assertContains(log, resReport, "found 2 conflict partitions: [counterConflicts=1, hashConflicts=1]");
    }

    /**
     * Tests that idle verify print partitions info over system caches.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyDumpForCorruptedDataOnSystemCache() throws Exception {
        int parts = 32;

        atomicConfiguration = new AtomicConfiguration()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(2);

        IgniteEx ignite = crd;

        injectTestSystemOut();

        // Adding some assignments without deployments.
        for (int i = 0; i < 100; i++) {
            ignite.semaphore("s" + i, i, false, true);

            ignite.atomicSequence("sq" + i, 0, true)
                .incrementAndGet();
        }

        CacheGroupContext storedSysCacheCtx = ignite.context().cache().cacheGroup(CU.cacheId("default-ds-group"));

        assertNotNull(storedSysCacheCtx);

        corruptDataEntry(storedSysCacheCtx.caches().get(0), new GridCacheInternalKeyImpl("sq0",
            "default-ds-group"), true, false, new GridCacheVersion(0, 0, 0), "broken");

        corruptDataEntry(storedSysCacheCtx.caches().get(0), new GridCacheInternalKeyImpl("sq" + parts / 2,
            "default-ds-group"), false, true, new GridCacheVersion(0, 0, 0), "broken");

        CacheGroupContext memoryVolatileCacheCtx = ignite.context().cache().cacheGroup(CU.cacheId(
            "default-volatile-ds-group@volatileDsMemPlc"));

        assertNotNull(memoryVolatileCacheCtx);
        assertEquals("volatileDsMemPlc", memoryVolatileCacheCtx.dataRegion().config().getName());
        assertEquals(false, memoryVolatileCacheCtx.dataRegion().config().isPersistenceEnabled());

        corruptDataEntry(memoryVolatileCacheCtx.caches().get(0), new GridCacheInternalKeyImpl("s0",
            "default-volatile-ds-group@volatileDsMemPlc"), true, false, new GridCacheVersion(0, 0, 0), "broken");

        corruptDataEntry(memoryVolatileCacheCtx.caches().get(0), new GridCacheInternalKeyImpl("s" + parts / 2,
            "default-volatile-ds-group@volatileDsMemPlc"), false, true, new GridCacheVersion(0, 0, 0), "broken");

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--cache-filter", "SYSTEM"));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            U.log(log, dumpWithConflicts);

            // Non-persistent caches do not have counter conflicts
            assertContains(log, dumpWithConflicts, "found 3 conflict partitions: [counterConflicts=1, " +
                "hashConflicts=2]");
        }
        else
            fail("Should be found dump with conflicts");
    }

    /**
     * Tests that idle verify print partitions info over persistence client caches.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyDumpForCorruptedDataOnPersistenceClientCache() throws Exception {
        IgniteEx ignite = crd;

        createCacheAndPreload(ignite, 100);

        corruptingAndCheckDefaultCache(ignite, "PERSISTENT", true);
    }

    /**
     * Tests that idle verify print partitions info with exclude cache group.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyDumpExcludedCacheGrp() throws Exception {
        IgniteEx ignite = crd;

        int parts = 32;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setGroupName("shared_grp")
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> secondCache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setGroupName("shared_grp")
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME + "_second"));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--exclude-caches", "shared_grp"));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithConflicts, "There are no caches matching given filter options");
        }
        else
            fail("Should be found dump with conflicts");
    }

    /**
     * Tests that idle verify print partitions info with exclude caches.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyDumpExcludedCaches() throws Exception {
        IgniteEx ignite = crd;

        int parts = 32;

        ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setGroupName("shared_grp")
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setGroupName("shared_grp")
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME + "_second"));

        ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME + "_third"));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--exclude-caches", DEFAULT_CACHE_NAME
            + "," + DEFAULT_CACHE_NAME + "_second"));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithConflicts, "idle_verify check has finished, found 32 partitions");
            assertContains(log, dumpWithConflicts, "default_third");
            assertNotContains(log, dumpWithConflicts, "shared_grp");
        }
        else
            fail("Should be found dump with conflicts");
    }

    /**
     * @return Build matcher for dump file name.
     */
    @NotNull private Matcher dumpFileNameMatcher() {
        Pattern fileNamePattern = Pattern.compile(".*VisorIdleVerifyDumpTask successfully written output to '(.*)'");

        return fileNamePattern.matcher(testOut.toString());
    }

    /** */
    public void testCacheContention() throws Exception {
        int cnt = 10;

        final ExecutorService svc = Executors.newFixedThreadPool(cnt);

        try {
            Ignite ignite = crd;

            final IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setAtomicityMode(TRANSACTIONAL)
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME));

            final CountDownLatch l = new CountDownLatch(1);

            final CountDownLatch l2 = new CountDownLatch(1);

            svc.submit(new Runnable() {
                @Override public void run() {
                    try (final Transaction tx = ignite.transactions().txStart()) {
                        cache.put(0, 0);

                        l.countDown();

                        U.awaitQuiet(l2);

                        tx.commit();
                    }
                }
            });

            for (int i = 0; i < cnt - 1; i++) {
                svc.submit(new Runnable() {
                    @Override public void run() {
                        U.awaitQuiet(l);

                        try (final Transaction tx = ignite.transactions().txStart()) {
                            cache.get(0);

                            tx.commit();
                        }
                    }
                });
            }

            U.awaitQuiet(l);

            Thread.sleep(300);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "contention", "5"));

            l2.countDown();

            String out = testOut.toString();

            assertContains(log, out, "TxEntry");
            assertContains(log, out, "op=READ");
            assertContains(log, out, "op=CREATE");
            assertContains(log, out, "id=" + ignite(0).cluster().localNode().id());
            assertContains(log, out, "id=" + ignite(1).cluster().localNode().id());
        }
        finally {
            svc.shutdown();
            svc.awaitTermination(100, TimeUnit.DAYS);
        }
    }

    /** */
    public void testCacheGroups() {
        Ignite ignite = crd;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setGroupName("G100")
            .setName(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", ".*", "--groups"));

        assertContains(log, testOut.toString(), "G100");
    }

    /** */
    public void testCacheAffinity() {
        Ignite ignite = crd;

        IgniteCache<Object, Object> cache1 = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 100; i++)
            cache1.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", ".*"));

        String out = testOut.toString();

        assertContains(log, out, "cacheName=" + DEFAULT_CACHE_NAME);
        assertContains(log, out, "prim=32");
        assertContains(log, out, "mapped=32");
        assertContains(log, out, "affCls=RendezvousAffinityFunction");
    }

    /** */
    public void testCacheConfigNoOutputFormat() {
        testCacheConfig(null, 1, 1);
    }

    /** */
    public void testCacheConfigSingleLineOutputFormatSingleNodeSignleCache() {
        testCacheConfigSingleLineOutputFormat(1, 1);
    }

    /** */
    public void testCacheConfigSingleLineOutputFormatTwoNodeSignleCache() {
        testCacheConfigSingleLineOutputFormat(2, 1);
    }

    /** */
    public void testCacheConfigSingleLineOutputFormatTwoNodeManyCaches() {
        testCacheConfigSingleLineOutputFormat(2, 100);
    }

    /** */
    public void testCacheConfigMultiLineOutputFormatSingleNodeSingleCache() {
        testCacheConfigMultiLineOutputFormat(1, 1);
    }

    /** */
    public void testCacheConfigMultiLineOutputFormatTwoNodeSingleCache() {
        testCacheConfigMultiLineOutputFormat(2, 1);
    }

    /** */
    public void testCacheConfigMultiLineOutputFormatTwoNodeManyCaches() {
        testCacheConfigMultiLineOutputFormat(2, 100);
    }

    /** */
    private void testCacheConfigSingleLineOutputFormat(int nodesCnt, int cachesCnt) {
        testCacheConfig("single-line", nodesCnt, cachesCnt);
    }

    /** */
    private void testCacheConfigMultiLineOutputFormat(int nodesCnt, int cachesCnt) {
        testCacheConfig("multi-line", nodesCnt, cachesCnt);
    }

    /** */
    private void testCacheConfig(String outputFormat, int nodesCnt, int cachesCnt) {
        assertTrue("Invalid number of nodes or caches", nodesCnt > 0 && cachesCnt > 0);

        Ignite ignite = crd;

        List<CacheConfiguration> ccfgs = new ArrayList<>(cachesCnt);

        for (int i = 0; i < cachesCnt; i++) {
            ccfgs.add(
                new CacheConfiguration<>()
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
                    .setBackups(1)
                    .setName(DEFAULT_CACHE_NAME + i)
            );
        }

        ignite.createCaches(ccfgs);

        IgniteCache<Object, Object> cache1 = ignite.cache(DEFAULT_CACHE_NAME + 0);

        for (int i = 0; i < 100; i++)
            cache1.put(i, i);

        injectTestSystemOut();

        int exitCode;

        if (outputFormat == null)
            exitCode = execute("--cache", "list", ".*", "--config");
        else
            exitCode = execute("--cache", "list", ".*", "--config", "--output-format", outputFormat);

        assertEquals(EXIT_CODE_OK, exitCode);

        String outStr = testOut.toString();

        if (outputFormat == null || SINGLE_LINE.text().equals(outputFormat)) {
            for (int i = 0; i < cachesCnt; i++)
                assertContains(log, outStr, "name=" + DEFAULT_CACHE_NAME + i);

            assertContains(log, outStr, "partitions=32");
            assertContains(log, outStr, "function=o.a.i.cache.affinity.rendezvous.RendezvousAffinityFunction");
        }
        else if (MULTI_LINE.text().equals(outputFormat)) {
            for (int i = 0; i < cachesCnt; i++)
                assertContains(log, outStr, "[cache = '" + DEFAULT_CACHE_NAME + i + "']");

            assertContains(log, outStr, "Affinity Partitions: 32");
            assertContains(log, outStr, "Affinity Function: o.a.i.cache.affinity.rendezvous.RendezvousAffinityFunction");
        }
        else
            fail("Unknown output format: " + outputFormat);
    }

    /** */
    public void testCacheDistribution() {
        Ignite ignite = crd;

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        // Run distribution for all node and all cache
        assertEquals(EXIT_CODE_OK, execute("--cache", "distribution", "null"));

        String out = testOut.toString();

        // Result include info by cache "default"
        assertContains(log ,out, "[next group: id=1544803905, name=default]");

        // Result include info by cache "ignite-sys-cache"
        assertContains(log ,out, "[next group: id=-2100569601, name=ignite-sys-cache]");

        // Run distribution for all node and all cache and include additional user attribute
        assertEquals(EXIT_CODE_OK, execute("--cache", "distribution", "null", "--user-attributes", "ZONE,CELL,DC"));

        out += "\n" + testOut.toString();

        List<String> outLines = Arrays.stream(out.split("\n"))
                                .map(String::trim)
                                .collect(toList());

        int firstIndex = outLines.indexOf("[next group: id=1544803905, name=default]");
        int lastIndex  = outLines.lastIndexOf("[next group: id=1544803905, name=default]");

        String dataLine = outLines.get(firstIndex + 1);
        String userArrtDataLine = outLines.get(lastIndex + 1);

        long commaNum = dataLine.chars().filter(i -> i == ',').count();
        long userArrtCommaNum = userArrtDataLine.chars().filter(i -> i == ',').count();

        // Check that number of columns increased by 3
        assertEquals(3, userArrtCommaNum - commaNum);
    }

    /** */
    public void testCacheResetLostPartitions() {
        Ignite ignite = crd;

        createCacheAndPreload(ignite, 100);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "reset_lost_partitions", "ignite-sys-cache,default"));

        final String out = testOut.toString();

        assertContains(log, out, "Reset LOST-partitions performed successfully. Cache group (name = 'ignite-sys-cache'");

        assertContains(log, out, "Reset LOST-partitions performed successfully. Cache group (name = 'default'");
    }

    /**
     * @param h Handler.
     * @param validateClo Validate clo.
     * @param args Args.
     */
    private void validate(CommandHandler h, IgniteInClosure<Map<ClusterNode, VisorTxTaskResult>> validateClo,
        String... args) {
        assertEquals(EXIT_CODE_OK, execute(h, args));

        validateClo.apply(h.getLastOperationResult());
    }

    /**
     * @param from From.
     * @param cnt Count.
     */
    private Map<Object, Object> generate(int from, int cnt) {
        Map<Object, Object> map = new TreeMap<>();

        for (int i = 0; i < cnt; i++)
            map.put(i + from, i + from);

        return map;
    }

    /**
     * Test execution of --wal print command.
     */
    public void testUnusedWalPrint() {
        Ignite ignite = crd;

        List<String> nodes = new ArrayList<>(2);

        for (ClusterNode node : ignite.cluster().forServers().nodes())
            nodes.add(node.consistentId().toString());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--wal", "print"));

        String out = testOut.toString();

        for (String id : nodes)
            assertContains(log, out, id);

        assertNotContains(log, out, "error");

        assertEquals(EXIT_CODE_OK, execute("--wal", "print", nodes.get(0)));

        out = testOut.toString();

        assertNotContains(log, out, nodes.get(1));

        assertNotContains(log, out, "error");
    }

    /**
     * Test execution of --wal delete command.
     */
    public void testUnusedWalDelete() {
        Ignite ignite = crd;

        List<String> nodes = new ArrayList<>(2);

        for (ClusterNode node : ignite.cluster().forServers().nodes())
            nodes.add(node.consistentId().toString());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--wal", "delete"));

        String out = testOut.toString();

        for (String id : nodes)
            assertContains(log, out, id);

        assertNotContains(log, out, "error");

        assertEquals(EXIT_CODE_OK, execute("--wal", "delete", nodes.get(0)));

        out = testOut.toString();

        assertNotContains(log, out, nodes.get(1));

        assertNotContains(log, out, "error");
    }

    /**
     * Starts several long transactions in order to test --tx command. Transactions will last until unlock latch is
     * released: first transaction will wait for unlock latch directly, some others will wait for key lock acquisition.
     *
     * @param lockLatch Lock latch. Will be released inside body of the first transaction.
     * @param unlockLatch Unlock latch. Should be released externally. First transaction won't be finished until unlock
     * latch is released.
     * @param topChangeBeforeUnlock <code>true</code> should be passed if cluster topology is expected to change between
     * method call and unlock latch release. Commit of the first transaction will be asserted to fail in such case.
     * @return Future to be completed after finish of all started transactions.
     */
    private IgniteInternalFuture<?> startTransactions(
        String testName,
        CountDownLatch lockLatch,
        CountDownLatch unlockLatch,
        boolean topChangeBeforeUnlock
    ) throws Exception {
        IgniteEx client = grid("client");

        AtomicInteger idx = new AtomicInteger();

        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                switch (id) {
                    case 0:
                        try (Transaction tx = grid(0).transactions().txStart()) {
                            grid(0).cache(DEFAULT_CACHE_NAME).putAll(generate(0, 100));

                            lockLatch.countDown();

                            U.awaitQuiet(unlockLatch);

                            tx.commit();

                            if (topChangeBeforeUnlock)
                                fail("Commit must fail");
                        }
                        catch (Exception e) {
                            if (topChangeBeforeUnlock)
                                assertTrue(X.hasCause(e, TransactionRollbackException.class));
                            else
                                throw e;
                        }

                        break;
                    case 1:
                        U.awaitQuiet(lockLatch);

                        doSleep(3000);

                        try (Transaction tx = grid(0).transactions().withLabel("label1").txStart(PESSIMISTIC, READ_COMMITTED, Integer.MAX_VALUE, 0)) {
                            grid(0).cache(DEFAULT_CACHE_NAME).putAll(generate(200, 110));

                            grid(0).cache(DEFAULT_CACHE_NAME).put(0, 0);
                        }

                        break;
                    case 2:
                        try (Transaction tx = grid(1).transactions().txStart()) {
                            U.awaitQuiet(lockLatch);

                            grid(1).cache(DEFAULT_CACHE_NAME).put(0, 0);
                        }

                        break;
                    case 3:
                        try (Transaction tx = client.transactions().withLabel("label2").txStart(OPTIMISTIC, READ_COMMITTED, 0, 0)) {
                            U.awaitQuiet(lockLatch);

                            client.cache(DEFAULT_CACHE_NAME).putAll(generate(100, 10));

                            client.cache(DEFAULT_CACHE_NAME).put(0, 0);

                            tx.commit();
                        }

                        break;
                }
            }
        }, 4, "tx-thread-" + testName);
    }

    /**
     * Test is that when the --help control.sh command is executed, output will contain non-experimental commands. In
     * case system property {@link IgniteSystemProperties#IGNITE_ENABLE_EXPERIMENTAL_COMMAND} = {@code true}.
     */
    public void testContainsNotExperimentalCmdInHelpOutputWhenEnableExperimentalTrue() {
        withSystemProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, "true");

        checkContainsNotExperimentalCmdInHelpOutput();
    }

    /**
     * Test is that when the --help control.sh command is executed, output
     * will contain non-experimental commands. In case system property
     * {@link IgniteSystemProperties#IGNITE_ENABLE_EXPERIMENTAL_COMMAND} =
     * {@code false}.
     */
    public void testContainsNotExperimentalCmdInHelpOutputWhenEnableExperimentalFalse() {
        withSystemProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, "false");

        checkContainsNotExperimentalCmdInHelpOutput();
    }

    /**
     * Test for contains of experimental commands in output of the --help
     * control.sh command.
     */
    public void testContainsExperimentalCmdInHelpOutput() {
        withSystemProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, "true");

        checkExperimentalCmdInHelpOutput(true);
    }

    /**
     * Test for not contains of experimental commands in output of the --help
     * control.sh command.
     */
    public void testNotContainsExperimentalCmdInHelpOutput() {
        withSystemProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, "false");

        checkExperimentalCmdInHelpOutput(false);
    }

    /**
     * Test to verify that the experimental command will not be executed if
     * {@link IgniteSystemProperties#IGNITE_ENABLE_EXPERIMENTAL_COMMAND} =
     * {@code false}, a warning will be displayed instead.
     * */
    public void testContainsWarnInsteadExecExperimentalCmdWhenEnableExperimentalFalse() {
        withSystemProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, "false");

        injectTestSystemOut();

        Map<CommandList, Collection<String>> cmdArgs = new EnumMap<>(CommandList.class);

        cmdArgs.put(WAL, asList("print", "delete"));

        String warning = String.format(
            "For use experimental command add %s=true to JVM_OPTS in %s",
            IGNITE_ENABLE_EXPERIMENTAL_COMMAND,
            UTILITY_NAME
        );

        stream(CommandList.values()).filter(cmd -> cmd.command().experimental())
            .peek(cmd -> assertTrue(cmdArgs.containsKey(cmd)))
            .forEach(cmd -> cmdArgs.get(cmd).forEach(cmdArg -> {
                assertEquals(EXIT_CODE_OK, execute(cmd.text(), cmdArg));

                assertContains(log, testOut.toString(), warning);
            }));
    }

    /**
     * Test print stack trace if an error occurs when option
     * {@link CommonArgParser#CMD_PRINT_ERR_STACK_TRACE} is enabled.
     */
    public void testPrintErrStackTraceWhenError() {
        injectTestSystemOut();

        execCmdWithError(CMD_PRINT_ERR_STACK_TRACE);

        assertContains(log, testOut.toString(), "Error stack trace:");
    }

    /**
     * The test checks that stack trace will not be displayed in case of an
     * error without option {@link CommonArgParser#CMD_PRINT_ERR_STACK_TRACE}
     * enabled.
     */
    public void testNotPrintErrStackTraceWhenError() {
        injectTestSystemOut();

        execCmdWithError();

        assertNotContains(log, testOut.toString(), "Error stack trace:");
    }

    /**
     * Checking for contains or not of experimental commands in output of the
     * --help control.sh command.
     *
     * @param contains Check contains or not.
     */
    private void checkExperimentalCmdInHelpOutput(boolean contains) {
        execHelpCmd(helpOut -> {
            stream(CommandList.values()).filter(cmd -> cmd.command().experimental())
                .forEach(cmd -> {
                    if (contains)
                        assertContains(log, helpOut, cmd.text());
                    else
                        assertNotContains(log, helpOut, cmd.text());
                });
        });
    }

    /**
     * Check that when executing the "--help" control.sh command, the output
     * will contain non-experimental commands.
     */
    private void checkContainsNotExperimentalCmdInHelpOutput() {
        execHelpCmd(helpOut -> {
            stream(CommandList.values()).filter(cmd -> !cmd.command().experimental())
                .forEach(cmd -> assertContains(log, helpOut, cmd.text()));
        });
    }

    /**
     * Executing the command "--help" control.sh with transfer of output to
     * consumer.
     *
     * @param consumer Consumer.
     */
    private void execHelpCmd(Consumer<String> consumer) {
        assert nonNull(consumer);

        injectTestSystemOut();

        execute("--help");

        consumer.accept(testOut.toString());
    }

    /**
     * Executing a command with an error.
     *
     * @param addArgs Additional arguments for executing the command.
     */
    private void execCmdWithError(String... addArgs) {
        List<String> args = new ArrayList<>();

        args.add(BASELINE.text());
        args.add(UUID.randomUUID().toString());
        args.addAll(asList(addArgs));

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute(args.toArray(new String[0])));
    }
}
