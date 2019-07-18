/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.baseline.BaselineArguments;
import org.apache.ignite.internal.commandline.cache.CacheCommands;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.commandline.cache.CacheValidateIndexes;
import org.apache.ignite.internal.commandline.cache.FindAndDeleteGarbage;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.junit.Test;
import org.junit.Rule;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.CommandList.WAL;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_DELETE;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_PRINT;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests Command Handler parsing arguments.
 */
@WithSystemProperty(key = IGNITE_ENABLE_EXPERIMENTAL_COMMAND, value = "true")
public class CommandHandlerParsingTest {
    /** */
    @ClassRule public static final TestRule classRule = new SystemPropertiesRule();

    /** */
    @Rule public final TestRule methodRule = new SystemPropertiesRule();

    /**
     * validate_indexes command arguments parsing and validation
     */
    @Test
    public void testValidateIndexArguments() {
        //happy case for all parameters
        try {
            int expectedCheckFirst = 10;
            int expectedCheckThrough = 11;
            UUID nodeId = UUID.randomUUID();

            ConnectionAndSslParameters args = parseArgs(Arrays.asList(
                CACHE.text(),
                VALIDATE_INDEXES.text(),
                "cache1, cache2",
                nodeId.toString(),
                CHECK_FIRST.toString(),
                Integer.toString(expectedCheckFirst),
                CHECK_THROUGH.toString(),
                Integer.toString(expectedCheckThrough)
            ));

            assertTrue(args.command() instanceof CacheCommands);

            CacheSubcommands subcommand = ((CacheCommands)args.command()).arg();

            CacheValidateIndexes.Arguments arg = (CacheValidateIndexes.Arguments)subcommand.subcommand().arg();

            assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId());
            assertEquals("checkFirst parameter unexpected value", expectedCheckFirst, arg.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedCheckThrough, arg.checkThrough());
        }
        catch (IllegalArgumentException e) {
            fail("Unexpected exception: " + e);
        }

        try {
            int expectedParam = 11;
            UUID nodeId = UUID.randomUUID();

            ConnectionAndSslParameters args = parseArgs(Arrays.asList(
                    CACHE.text(),
                    VALIDATE_INDEXES.text(),
                    nodeId.toString(),
                    CHECK_THROUGH.toString(),
                    Integer.toString(expectedParam)
                ));

            assertTrue(args.command() instanceof CacheCommands);

            CacheSubcommands subcommand = ((CacheCommands)args.command()).arg();

            CacheValidateIndexes.Arguments arg = (CacheValidateIndexes.Arguments)subcommand.subcommand().arg();

            assertNull("caches weren't specified, null value expected", arg.caches());
            assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId());
            assertEquals("checkFirst parameter unexpected value", -1, arg.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedParam, arg.checkThrough());
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            parseArgs(
                Arrays.asList(
                    CACHE.text(),
                    VALIDATE_INDEXES.text(),
                    CHECK_FIRST.toString(),
                    "0"
                )
            );

            fail("Expected exception hasn't been thrown");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            parseArgs(Arrays.asList(CACHE.text(), VALIDATE_INDEXES.text(), CHECK_THROUGH.toString()));

            fail("Expected exception hasn't been thrown");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    @Test
    public void testFindAndDeleteGarbage() {
        String nodeId = UUID.randomUUID().toString();
        String delete = FindAndDeleteGarbageArg.DELETE.toString();
        String groups = "group1,grpoup2,group3";

        List<List<String>> lists = generateArgumentList(
            FIND_AND_DELETE_GARBAGE.text(),
            new T2<>(nodeId, false),
            new T2<>(delete, false),
            new T2<>(groups, false)
        );

        for (List<String> list : lists) {
            ConnectionAndSslParameters args = parseArgs(list);

            assertTrue(args.command() instanceof CacheCommands);

            CacheSubcommands subcommand = ((CacheCommands)args.command()).arg();

            FindAndDeleteGarbage.Arguments arg = (FindAndDeleteGarbage.Arguments)subcommand.subcommand().arg();

            if (list.contains(nodeId)) {
                assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId().toString());
            }
            else {
                assertNull(arg.nodeId());
            }

            assertEquals(list.contains(delete), arg.delete());

            if (list.contains(groups)) {
                assertEquals(3, arg.groups().size());
            }
            else {
                assertNull(arg.groups());
            }
        }
    }

    private List<List<String>> generateArgumentList(String subcommand, T2<String, Boolean>...optional) {
        List<List<T2<String, Boolean>>> lists = generateAllCombinations(Arrays.asList(optional), (x) -> x.get2());

        ArrayList<List<String>> res = new ArrayList<>();

        ArrayList<String> empty = new ArrayList<>();

        empty.add(CACHE.text());
        empty.add(subcommand);

        res.add(empty);

        for (List<T2<String, Boolean>> list : lists) {
            ArrayList<String> arg = new ArrayList<>(empty);

            list.forEach(x -> arg.add(x.get1()));

            res.add(arg);
        }

        return res;
    }

    private <T> List<List<T>> generateAllCombinations(List<T> source, Predicate<T> stopFunc) {
        List<List<T>> res = new ArrayList<>();

        for (int i = 0; i < source.size(); i++) {
            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            generateAllCombinations(Collections.singletonList(removed), sourceCopy, stopFunc, res);
        }

        return res;
    }


    private <T> void generateAllCombinations(List<T> res, List<T> source, Predicate<T> stopFunc, List<List<T>> acc) {
        acc.add(res);

        if (stopFunc != null && stopFunc.test(res.get(res.size() - 1))) {
            return;
        }

        if (source.size() == 1) {
            ArrayList<T> list = new ArrayList<>(res);

            list.add(source.get(0));

            acc.add(list);

            return;
        }

        for (int i = 0; i < source.size(); i++) {
            ArrayList<T> res0 = new ArrayList<>(res);

            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            res0.add(removed);

            generateAllCombinations(res0, sourceCopy, stopFunc, acc);
        }
    }

    /**
     * Tests parsing and validation for the SSL arguments.
     */
    @Test
    public void testParseAndValidateSSLArguments() {
        for (CommandList cmd : CommandList.values()) {
            if (cmd == CommandList.CACHE || cmd == CommandList.WAL || cmd == CommandList.ROLLING_UPGRADE)
                continue; // --cache, --wal and --rolling-upgrade commands requires its own specific arguments.

            try {
                parseArgs(asList("--truststore"));

                fail("expected exception: Expected truststore");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            ConnectionAndSslParameters args = parseArgs(asList("--keystore", "testKeystore", "--keystore-password", "testKeystorePassword", "--keystore-type", "testKeystoreType",
                "--truststore", "testTruststore", "--truststore-password", "testTruststorePassword", "--truststore-type", "testTruststoreType",
                "--ssl-key-algorithm", "testSSLKeyAlgorithm", "--ssl-protocol", "testSSLProtocol", cmd.text()));

            assertEquals("testSSLProtocol", args.sslProtocol());
            assertEquals("testSSLKeyAlgorithm", args.sslKeyAlgorithm());
            assertEquals("testKeystore", args.sslKeyStorePath());
            assertArrayEquals("testKeystorePassword".toCharArray(), args.sslKeyStorePassword());
            assertEquals("testKeystoreType", args.sslKeyStoreType());
            assertEquals("testTruststore", args.sslTrustStorePath());
            assertArrayEquals("testTruststorePassword".toCharArray(), args.sslTrustStorePassword());
            assertEquals("testTruststoreType", args.sslTrustStoreType());

            assertEquals(cmd.command(), args.command());
        }
    }


    /**
     * Tests parsing and validation for user and password arguments.
     */
    @Test
    public void testParseAndValidateUserAndPassword() {
        for (CommandList cmd : CommandList.values()) {
            if (cmd == CommandList.CACHE || cmd == CommandList.WAL || cmd == CommandList.ROLLING_UPGRADE)
                continue; // --cache, --wal and --rolling-upgrade commands requires its own specific arguments.

            try {
                parseArgs(asList("--user"));

                fail("expected exception: Expected user name");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                parseArgs(asList("--password"));

                fail("expected exception: Expected password");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            ConnectionAndSslParameters args = parseArgs(asList("--user", "testUser", "--password", "testPass", cmd.text()));

            assertEquals("testUser", args.userName());
            assertEquals("testPass", args.password());
            assertEquals(cmd.command(), args.command());
        }
    }

    /**
     * Tests parsing and validation  of WAL commands.
     */
    @Test
    public void testParseAndValidateWalActions() {
        ConnectionAndSslParameters args = parseArgs(Arrays.asList(WAL.text(), WAL_PRINT));

        assertEquals(WAL.command(), args.command());

        T2<String, String> arg = ((WalCommands)args.command()).arg();

        assertEquals(WAL_PRINT, arg.get1());

        String nodes = UUID.randomUUID().toString() + "," + UUID.randomUUID().toString();

        args = parseArgs(Arrays.asList(WAL.text(), WAL_DELETE, nodes));

        arg = ((WalCommands)args.command()).arg();

        assertEquals(WAL_DELETE, arg.get1());

        assertEquals(nodes, arg.get2());

        try {
            parseArgs(Collections.singletonList(WAL.text()));

            fail("expected exception: invalid arguments for --wal command");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            parseArgs(Arrays.asList(WAL.text(), UUID.randomUUID().toString()));

            fail("expected exception: invalid arguments for --wal command");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tests that the auto confirmation flag was correctly parsed.
     */
    @Test
    public void testParseAutoConfirmationFlag() {
        for (CommandList cmd : CommandList.values()) {
            if (cmd != CommandList.DEACTIVATE
                && cmd != CommandList.BASELINE
                && cmd != CommandList.TX)
                continue;

            ConnectionAndSslParameters args = parseArgs(asList(cmd.text()));

            assertEquals(cmd.command(), args.command());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());
            assertFalse(args.autoConfirmation());

            switch (cmd) {
                case DEACTIVATE: {
                    args = parseArgs(asList(cmd.text(), "--yes"));

                    assertEquals(cmd.command(), args.command());
                    assertEquals(DFLT_HOST, args.host());
                    assertEquals(DFLT_PORT, args.port());
                    assertTrue(args.autoConfirmation());

                    break;
                }
                case BASELINE: {
                    for (String baselineAct : asList("add", "remove", "set")) {
                        args = parseArgs(asList(cmd.text(), baselineAct, "c_id1,c_id2", "--yes"));

                        assertEquals(cmd.command(), args.command());
                        assertEquals(DFLT_HOST, args.host());
                        assertEquals(DFLT_PORT, args.port());
                        assertTrue(args.autoConfirmation());

                        BaselineArguments arg = ((BaselineCommand)args.command()).arg();

                        assertEquals(baselineAct, arg.getCmd().text());
                        assertEquals(new HashSet<>(Arrays.asList("c_id1","c_id2")), new HashSet<>(arg.getConsistentIds()));
                    }

                    break;
                }

                case TX: {
                    args = parseArgs(asList(cmd.text(), "--xid", "xid1", "--min-duration", "10", "--kill", "--yes"));

                    assertEquals(cmd.command(), args.command());
                    assertEquals(DFLT_HOST, args.host());
                    assertEquals(DFLT_PORT, args.port());
                    assertTrue(args.autoConfirmation());

                    VisorTxTaskArg txTaskArg = ((TxCommands)args.command()).arg();

                    assertEquals("xid1", txTaskArg.getXid());
                    assertEquals(10_000, txTaskArg.getMinDuration().longValue());
                    assertEquals(VisorTxOperation.KILL, txTaskArg.getOperation());
                }
            }
        }
    }

    /**
     * Tests host and port arguments.
     * Tests connection settings arguments.
     */
    @Test
    public void testConnectionSettings() {
        for (CommandList cmd : CommandList.values()) {
            if (cmd == CommandList.CACHE || cmd == CommandList.WAL || cmd == CommandList.ROLLING_UPGRADE)
                continue; // --cache, --wal and --rolling-upgrade commands requires its own specific arguments.

            ConnectionAndSslParameters args = parseArgs(asList(cmd.text()));

            assertEquals(cmd.command(), args.command());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());

            args = parseArgs(asList("--port", "12345", "--host", "test-host", "--ping-interval", "5000",
                "--ping-timeout", "40000", cmd.text()));

            assertEquals(cmd.command(), args.command());
            assertEquals("test-host", args.host());
            assertEquals("12345", args.port());
            assertEquals(5000, args.pingInterval());
            assertEquals(40000, args.pingTimeout());

            try {
                parseArgs(asList("--port", "wrong-port", cmd.text()));

                fail("expected exception: Invalid value for port:");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                parseArgs(asList("--ping-interval", "-10", cmd.text()));

                fail("expected exception: Ping interval must be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                parseArgs(asList("--ping-timeout", "-20", cmd.text()));

                fail("expected exception: Ping timeout must be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * test parsing dump transaction arguments
     */
    @Test
    public void testTransactionArguments() {
        ConnectionAndSslParameters args;

        parseArgs(asList("--tx"));

        try {
            parseArgs(asList("--tx", "minDuration"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "minDuration", "-1"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "minSize"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "minSize", "-1"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "label"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "label", "tx123["));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            parseArgs(asList("--tx", "servers", "nodes", "1,2,3"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        args = parseArgs(asList("--tx", "--min-duration", "120", "--min-size", "10", "--limit", "100", "--order", "SIZE", "--servers"));

        VisorTxTaskArg arg = ((TxCommands)args.command()).arg();

        assertEquals(Long.valueOf(120 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(10), arg.getMinSize());
        assertEquals(Integer.valueOf(100), arg.getLimit());
        assertEquals(VisorTxSortOrder.SIZE, arg.getSortOrder());
        assertEquals(VisorTxProjection.SERVER, arg.getProjection());

        args = parseArgs(asList("--tx", "--min-duration", "130", "--min-size", "1", "--limit", "60", "--order", "DURATION",
            "--clients"));

        arg = ((TxCommands)args.command()).arg();

        assertEquals(Long.valueOf(130 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(1), arg.getMinSize());
        assertEquals(Integer.valueOf(60), arg.getLimit());
        assertEquals(VisorTxSortOrder.DURATION, arg.getSortOrder());
        assertEquals(VisorTxProjection.CLIENT, arg.getProjection());

        args = parseArgs(asList("--tx", "--nodes", "1,2,3"));

        arg = ((TxCommands)args.command()).arg();

        assertNull(arg.getProjection());
        assertEquals(Arrays.asList("1", "2", "3"), arg.getConsistentIds());
    }

    /** */
    @Test
    public void testValidateIndexesNotAllowedForSystemCache() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "validate_indexes", "cache1,ignite-sys-cache")),
            IllegalArgumentException.class,
            "validate_indexes not allowed for `ignite-sys-cache` cache."
        );
    }

    /** */
    @Test
    public void testIdleVerifyWithCheckCrcNotAllowedForSystemCache() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "idle_verify", "--check-crc", "--cache-filter", "ALL")),
            IllegalArgumentException.class,
            "idle_verify with --check-crc and --cache-filter ALL or SYSTEM not allowed. You should remove --check-crc or change --cache-filter value."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "idle_verify", "--check-crc", "--cache-filter", "SYSTEM")),
            IllegalArgumentException.class,
            "idle_verify with --check-crc and --cache-filter ALL or SYSTEM not allowed. You should remove --check-crc or change --cache-filter value."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "idle_verify", "--check-crc", "ignite-sys-cache")),
            IllegalArgumentException.class,
            "idle_verify with --check-crc not allowed for `ignite-sys-cache` cache."
        );
    }

    /**
     * @param args Raw arg list.
     * @return Common parameters container object.
     */
    private ConnectionAndSslParameters parseArgs(List<String> args) {
        return new CommonArgParser(setupTestLogger()).
            parseAndValidate(args.iterator());
    }

    /**
     * @return logger for tests.
     */
    private Logger setupTestLogger() {
        Logger result;

        result = Logger.getLogger(getClass().getName());
        result.setLevel(Level.INFO);
        result.setUseParentHandlers(false);

        result.addHandler(CommandHandler.setupStreamHandler());

        return result;
    }
}
