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

package org.apache.ignite.internal.commandline;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.ChangeTagCommand;
import org.apache.ignite.internal.management.DeactivateCommand;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.SetStateCommand;
import org.apache.ignite.internal.management.SetStateCommandArg;
import org.apache.ignite.internal.management.ShutdownPolicyCommand;
import org.apache.ignite.internal.management.ShutdownPolicyCommandArg;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.WarmUpCommand;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.BeforeNodeStartCommand;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.HelpCommand;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.management.baseline.BaselineAddCommand;
import org.apache.ignite.internal.management.baseline.BaselineAddCommandArg;
import org.apache.ignite.internal.management.baseline.BaselineCommand;
import org.apache.ignite.internal.management.baseline.BaselineRemoveCommand;
import org.apache.ignite.internal.management.baseline.BaselineSetCommand;
import org.apache.ignite.internal.management.cache.CacheCommand;
import org.apache.ignite.internal.management.cache.CacheFindGarbageCommandArg;
import org.apache.ignite.internal.management.cache.CacheScheduleIndexesRebuildCommandArg;
import org.apache.ignite.internal.management.cache.CacheValidateIndexesCommandArg;
import org.apache.ignite.internal.management.cdc.CdcCommand;
import org.apache.ignite.internal.management.consistency.ConsistencyCommand;
import org.apache.ignite.internal.management.defragmentation.DefragmentationCommand;
import org.apache.ignite.internal.management.encryption.EncryptionCommand;
import org.apache.ignite.internal.management.kill.KillCommand;
import org.apache.ignite.internal.management.meta.MetaCommand;
import org.apache.ignite.internal.management.metric.MetricCommand;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand;
import org.apache.ignite.internal.management.property.PropertyCommand;
import org.apache.ignite.internal.management.snapshot.SnapshotCommand;
import org.apache.ignite.internal.management.tx.TxCommand;
import org.apache.ignite.internal.management.tx.TxCommandArg;
import org.apache.ignite.internal.management.tx.TxSortOrder;
import org.apache.ignite.internal.management.wal.WalCommand;
import org.apache.ignite.internal.management.wal.WalDeleteCommandArg;
import org.apache.ignite.internal.management.wal.WalPrintCommand;
import org.apache.ignite.internal.management.wal.WalPrintCommand.WalPrintCommandArg;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.QueryMXBeanImpl.EXPECTED_GLOBAL_QRY_ID_FORMAT;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_VERBOSE;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_PORT;
import static org.apache.ignite.internal.management.api.CommandUtils.cmdText;
import static org.apache.ignite.internal.management.api.CommandUtils.executable;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.util.CdcCommandTest.DELETE_LOST_SEGMENT_LINKS;
import static org.apache.ignite.util.GridCommandHandlerIndexingCheckSizeTest.CACHE;
import static org.apache.ignite.util.SystemViewCommandTest.NODE_ID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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
    private static final String INVALID_REGEX = "[]";

    /** */
    public static final String WAL_PRINT = "print";

    /** */
    public static final String WAL_DELETE = "delete";

    /** */
    public static final String CHECK_FIRST = "--check-first";

    /** */
    public static final String CHECK_THROUGH = "--check-through";

    /** */
    public static final String VALIDATE_INDEXES = "validate_indexes";

    /** */
    public static final String WAL = "--wal";

    /** */
    public static final String SHUTDOWN_POLICY = "--shutdown-policy";

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

            ConnectionAndSslParameters args = parseArgs(asList(
                CACHE,
                VALIDATE_INDEXES,
                "cache1,cache2",
                nodeId.toString(),
                CHECK_FIRST.toString(),
                Integer.toString(expectedCheckFirst),
                CHECK_THROUGH.toString(),
                Integer.toString(expectedCheckThrough)
            ));

            CacheValidateIndexesCommandArg arg = (CacheValidateIndexesCommandArg)args.commandArg();

            assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeIds()[0]);
            assertEquals("checkFirst parameter unexpected value", expectedCheckFirst, arg.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedCheckThrough, arg.checkThrough());
        }
        catch (IllegalArgumentException e) {
            fail("Unexpected exception: " + e);
        }

        try {
            int expectedParam = 11;
            UUID nodeId = UUID.randomUUID();

            ConnectionAndSslParameters args = parseArgs(asList(
                    CACHE,
                    VALIDATE_INDEXES,
                    nodeId.toString(),
                    CHECK_THROUGH.toString(),
                    Integer.toString(expectedParam)
                ));

            CacheValidateIndexesCommandArg arg = (CacheValidateIndexesCommandArg)args.commandArg();

            assertNull("caches weren't specified, null value expected", arg.caches());
            assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeIds()[0]);
            assertEquals("checkFirst parameter unexpected value", -1, arg.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedParam, arg.checkThrough());
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        assertParseArgsThrows(
            "Value for '--check-first' property should be positive.",
            CACHE,
            VALIDATE_INDEXES,
            CHECK_FIRST.toString(),
            "0"
        );
        assertParseArgsThrows(
            "Please specify a value for argument: --check-through",
            CACHE,
            VALIDATE_INDEXES,
            CHECK_THROUGH.toString()
        );
    }

    /** */
    @Test
    public void testFindAndDeleteGarbage() {
        String nodeId = UUID.randomUUID().toString();
        String delete = "--delete";
        String groups = "group1,grpoup2,group3";

        List<List<String>> lists = generateArgumentList(
            "find_garbage",
            new T2<>(nodeId, false),
            new T2<>(delete, false),
            new T2<>(groups, false)
        );

        for (List<String> list : lists) {
            ConnectionAndSslParameters args = parseArgs(list);

            CacheFindGarbageCommandArg arg = (CacheFindGarbageCommandArg)args.commandArg();

            if (list.contains(nodeId))
                assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeIds()[0].toString());
            else
                assertNull(arg.nodeIds());

            assertEquals(list.contains(delete), arg.delete());

            if (list.contains(groups))
                assertEquals(3, arg.groups().length);
            else
                assertNull(arg.groups());
        }
    }

    /** */
    private List<List<String>> generateArgumentList(String subcommand, T2<String, Boolean>...optional) {
        List<List<T2<String, Boolean>>> lists = generateAllCombinations(asList(optional), (x) -> x.get2());

        ArrayList<List<String>> res = new ArrayList<>();

        ArrayList<String> empty = new ArrayList<>();

        empty.add(CACHE);
        empty.add(subcommand);

        res.add(empty);

        for (List<T2<String, Boolean>> list : lists) {
            ArrayList<String> arg = new ArrayList<>(empty);

            list.forEach(x -> arg.add(x.get1()));

            res.add(arg);
        }

        return res;
    }

    /** */
    private <T> List<List<T>> generateAllCombinations(List<T> source, Predicate<T> stopFunc) {
        List<List<T>> res = new ArrayList<>();

        for (int i = 0; i < source.size(); i++) {
            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            generateAllCombinations(singletonList(removed), sourceCopy, stopFunc, res);
        }

        return res;
    }

    /** */
    private <T> void generateAllCombinations(List<T> res, List<T> source, Predicate<T> stopFunc, List<List<T>> acc) {
        acc.add(res);

        if (stopFunc != null && stopFunc.test(res.get(res.size() - 1)))
            return;

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
        new IgniteCommandRegistry().commands().forEachRemaining(e -> {
            Command<?, ?> cmd = e.getValue();

            if (requireArgs(cmd.getClass()))
                return;

            assertParseArgsThrows("Please specify a value for argument: --truststore", "--truststore", "--tx");

            ConnectionAndSslParameters args = parseArgs(asList(
                "--keystore", "testKeystore",
                "--keystore-password", "testKeystorePassword",
                "--keystore-type", "testKeystoreType",
                "--truststore", "testTruststore",
                "--truststore-password", "testTruststorePassword",
                "--truststore-type", "testTruststoreType",
                "--ssl-key-algorithm", "testSSLKeyAlgorithm",
                "--ssl-protocol", "testSSLProtocol",
                cmdText(cmd)
            ));

            assertArrayEquals(new String[] {"testSSLProtocol"}, args.sslProtocol());
            assertEquals("testSSLKeyAlgorithm", args.sslKeyAlgorithm());
            assertEquals("testKeystore", args.sslKeyStorePath());
            assertArrayEquals("testKeystorePassword".toCharArray(), args.sslKeyStorePassword());
            assertEquals("testKeystoreType", args.sslKeyStoreType());
            assertEquals("testTruststore", args.sslTrustStorePath());
            assertArrayEquals("testTruststorePassword".toCharArray(), args.sslTrustStorePassword());
            assertEquals("testTruststoreType", args.sslTrustStoreType());

            assertEquals(cmd.getClass(), args.command().getClass());
        });
    }

    /**
     * Tests parsing and validation for user and password arguments.
     */
    @Test
    public void testParseAndValidateUserAndPassword() {
        new IgniteCommandRegistry().commands().forEachRemaining(e -> {
            Command<?, ?> cmd = e.getValue();

            if (requireArgs(cmd.getClass()))
                return;

            assertParseArgsThrows("Please specify a value for argument: --user", "--user", "--tx");
            assertParseArgsThrows("Please specify a value for argument: --password", "--password", "--tx");

            ConnectionAndSslParameters args = parseArgs(asList("--user", "testUser", "--password", "testPass", cmdText(cmd)));

            assertEquals("testUser", args.userName());
            assertEquals("testPass", args.password());
            assertEquals(cmd.getClass(), args.command().getClass());
        });
    }

    /**
     * Tests parsing and validation  of WAL commands.
     */
    @Test
    public void testParseAndValidateWalActions() {
        ConnectionAndSslParameters args = parseArgs(asList(WAL, WAL_PRINT));

        assertEquals(WalPrintCommand.class, args.command().getClass());

        WalDeleteCommandArg arg = (WalDeleteCommandArg)args.commandArg();

        assertTrue(arg instanceof WalPrintCommandArg);

        String nodes = UUID.randomUUID().toString() + "," + UUID.randomUUID().toString();

        args = parseArgs(asList(WAL, WAL_DELETE, nodes));

        arg = (WalDeleteCommandArg)args.commandArg();

        assertFalse(arg instanceof WalPrintCommandArg);

        assertEquals(nodes, String.join(",", arg.consistentIds()));

        assertParseArgsThrows("Command wal can't be executed", WAL);

        String rnd = UUID.randomUUID().toString();

        assertParseArgsThrows("Command wal can't be executed", WAL, rnd);
    }

    /**
     * Tets checks a parser of shutdown policy command.
     */
    @Test
    public void testParseShutdownPolicyParameters() {
        ConnectionAndSslParameters args = parseArgs(asList(SHUTDOWN_POLICY));

        assertEquals(ShutdownPolicyCommand.class, args.command().getClass());

        assertNull(((ShutdownPolicyCommandArg)args.commandArg()).shutdownPolicy());

        for (ShutdownPolicy policy : ShutdownPolicy.values()) {
            args = parseArgs(asList(SHUTDOWN_POLICY, String.valueOf(policy)));

            assertEquals(ShutdownPolicyCommand.class, args.command().getClass());

            assertSame(policy, ((ShutdownPolicyCommandArg)args.commandArg()).shutdownPolicy());
        }
    }

    /**
     * Tests that the auto confirmation flag was correctly parsed.
     */
    @Test
    public <A extends IgniteDataTransferObject> void testParseAutoConfirmationFlag() {
        new IgniteCommandRegistry().commands().forEachRemaining(e -> {
            Command<A, ?> cmdL = (Command<A, ?>)e.getValue();

            // SET_STATE command has mandatory argument used in confirmation message.
            CliCommandInvoker<A> cmd = cmdL.getClass() != SetStateCommand.class
                ? new CliCommandInvoker<>(cmdL, null, null)
                : new CliCommandInvoker<>(parseArgs(asList(cmdText(cmdL), "ACTIVE")).command(), null, null);

            if (!(cmd instanceof ComputeCommand)
                && !(cmd instanceof LocalCommand)
                && !(cmd instanceof BeforeNodeStartCommand)
                && !(cmd instanceof HelpCommand)) {
                return;
            }

            try {
                if (cmdL.confirmationPrompt(cmdL.argClass().newInstance()) == null)
                    return;
            }
            catch (InstantiationException | IllegalAccessException ex) {
                throw new IgniteException(ex);
            }

            ConnectionAndSslParameters args;

            if (cmdL.getClass() == SetStateCommand.class)
                args = parseArgs(asList(cmdText(cmdL), "ACTIVE"));
            else if (cmdL.getClass() == ChangeTagCommand.class)
                args = parseArgs(asList(cmdText(cmdL), "newTagValue"));
            else if (cmdL.getClass() == WarmUpCommand.class)
                args = parseArgs(asList(cmdText(cmdL), "--stop"));
            else if (cmdL.getClass() == CdcCommand.class)
                args = parseArgs(asList(cmdText(cmdL), DELETE_LOST_SEGMENT_LINKS, NODE_ID, UUID.randomUUID().toString()));
            else
                args = parseArgs(asList(cmdText(cmdL)));

            checkCommonParametersCorrectlyParsed(cmdL, args, false);

            if (cmdL.getClass() == DeactivateCommand.class) {
                args = parseArgs(asList(cmdText(cmdL), "--yes"));

                checkCommonParametersCorrectlyParsed(cmdL, args, true);

                args = parseArgs(asList(cmdText(cmdL), "--force", "--yes"));

                checkCommonParametersCorrectlyParsed(cmdL, args, true);
            }
            else if (cmdL.getClass() == SetStateCommand.class) {
                for (String newState : asList("ACTIVE_READ_ONLY", "ACTIVE", "INACTIVE")) {
                    args = parseArgs(asList(cmdText(cmdL), newState, "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    ClusterState argState = (((SetStateCommandArg)args.commandArg())).state();

                    assertEquals(newState, argState.toString());
                }

                for (String newState : asList("ACTIVE_READ_ONLY", "ACTIVE", "INACTIVE")) {
                    args = parseArgs(asList(cmdText(cmdL), newState, "--force", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    ClusterState argState = (((SetStateCommandArg)args.commandArg())).state();

                    assertEquals(newState, argState.toString());
                }
            }
            else if (cmdL.getClass() == BaselineCommand.class) {
                for (String baselineAct : asList("add", "remove", "set")) {
                    args = parseArgs(asList(cmdText(cmdL), baselineAct, "c_id1,c_id2", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    BaselineAddCommandArg arg = (BaselineAddCommandArg)args.commandArg();

                    if (baselineAct.equals("add"))
                        assertEquals(BaselineAddCommand.class, args.command().getClass());
                    else if (baselineAct.equals("remove"))
                        assertEquals(BaselineRemoveCommand.class, args.command().getClass());
                    else if (baselineAct.equals("set"))
                        assertEquals(BaselineSetCommand.class, args.command().getClass());

                    assertEquals(new HashSet<>(asList("c_id1", "c_id2")), new HashSet<>(Arrays.asList(arg.consistentIDs())));
                }
            }
            else if (cmdL.getClass() == TxCommand.class) {
                args = parseArgs(asList(cmdText(cmdL), "--xid", "xid1", "--min-duration", "10", "--kill", "--yes"));

                checkCommonParametersCorrectlyParsed(cmdL, args, true);

                TxCommandArg txTaskArg = (TxCommandArg)args.commandArg();

                assertEquals("xid1", txTaskArg.xid());
                assertEquals(10_000, txTaskArg.minDuration().longValue());
                assertTrue(txTaskArg.kill());
            }
            else if (cmdL.getClass() == ChangeTagCommand.class) {
                args = parseArgs(asList(cmdText(cmdL), "newTagValue", "--yes"));

                checkCommonParametersCorrectlyParsed(cmdL, args, true);
            }
            else if (cmdL.getClass() == WarmUpCommand.class) {
                args = parseArgs(asList(cmdText(cmdL), "--stop", "--yes"));

                checkCommonParametersCorrectlyParsed(cmdL, args, true);
            }
            else if (cmdL.getClass() == CdcCommand.class) {
                args = parseArgs(asList(cmdText(cmdL), DELETE_LOST_SEGMENT_LINKS,
                    NODE_ID, UUID.randomUUID().toString(), "--yes"));

                checkCommonParametersCorrectlyParsed(cmdL, args, true);
            }
            else
                fail("Unknown command: " + cmd);
        });
    }

    /** */
    private void checkCommonParametersCorrectlyParsed(
        Command<?, ?> cmd,
        ConnectionAndSslParameters args,
        boolean autoConfirm
    ) {
        assertEquals(cmd.getClass(), args.command().getClass());
        assertEquals(DFLT_HOST, args.host());
        assertEquals(DFLT_PORT, args.port());
        assertEquals(autoConfirm, args.autoConfirmation());
    }

    /**
     * Tests host and port arguments.
     * Tests connection settings arguments.
     */
    @Test
    public void testConnectionSettings() {
        new IgniteCommandRegistry().commands().forEachRemaining(e -> {
            Command<?, ?> cmd = e.getValue();

            if (requireArgs(cmd.getClass()))
                return;

            String name = cmdText(cmd);

            ConnectionAndSslParameters args = parseArgs(asList(name));

            assertEquals(cmd.getClass(), args.command().getClass());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());

            args = parseArgs(asList("--port", "12345", "--host", "test-host", "--ping-interval", "5000",
                "--ping-timeout", "40000", name));

            assertEquals(cmd.getClass(), args.command().getClass());
            assertEquals("test-host", args.host());
            assertEquals(12345, args.port());
            assertEquals(5000, args.pingInterval());
            assertEquals(40000, args.pingTimeout());

            assertParseArgsThrows("Can't parse number 'wrong-port'", "--port", "wrong-port", name);
            assertParseArgsThrows("Invalid value for --ping-interval: -10", "--ping-interval", "-10", name);
            assertParseArgsThrows("Invalid value for --ping-timeout: -20", "--ping-timeout", "-20", name);
        });
    }

    /**
     * Test parsing dump transaction arguments.
     */
    @Test
    public void testTransactionArguments() {
        ConnectionAndSslParameters args;

        parseArgs(asList("--tx"));

        assertParseArgsThrows("Please specify a value for argument: --min-duration", "--tx", "--min-duration");
        assertParseArgsThrows("Ouch! Argument is invalid: --min-duration", "--tx", "--min-duration", "-1");
        assertParseArgsThrows("Please specify a value for argument: --min-size", "--tx", "--min-size");
        assertParseArgsThrows("Ouch! Argument is invalid: --min-size", "--tx", "--min-size", "-1");
        assertParseArgsThrows("--label", "--tx", "--label");
        assertParseArgsThrows("Illegal regex syntax", "--tx", "--label", "tx123[");
        assertParseArgsThrows("Only one of [--servers, --nodes, --clients] allowed", "--tx", "--servers", "--nodes", "1,2,3");

        args = parseArgs(asList("--tx", "--min-duration", "120", "--min-size", "10", "--limit", "100", "--order", "SIZE", "--servers"));

        TxCommandArg arg = (TxCommandArg)args.commandArg();

        assertEquals(Long.valueOf(120 * 1000L), arg.minDuration());
        assertEquals(Integer.valueOf(10), arg.minSize());
        assertEquals(Integer.valueOf(100), arg.limit());
        assertEquals(TxSortOrder.SIZE, arg.order());
        assertTrue(arg.servers());
        assertFalse(arg.clients());

        args = parseArgs(asList("--tx", "--min-duration", "130", "--min-size", "1", "--limit", "60", "--order", "DURATION",
            "--clients"));

        arg = (TxCommandArg)args.commandArg();

        assertEquals(Long.valueOf(130 * 1000L), arg.minDuration());
        assertEquals(Integer.valueOf(1), arg.minSize());
        assertEquals(Integer.valueOf(60), arg.limit());
        assertEquals(TxSortOrder.DURATION, arg.order());
        assertFalse(arg.servers());
        assertTrue(arg.clients());

        args = parseArgs(asList("--tx", "--nodes", "1,2,3"));

        arg = (TxCommandArg)args.commandArg();

        assertFalse(arg.servers());
        assertFalse(arg.clients());
        assertArrayEquals(new String[] {"1", "2", "3"}, arg.nodes());
    }

    /**
     * Test parsing kill arguments.
     */
    @Test
    public void testKillArguments() {
        assertParseArgsThrows("Command kill can't be executed", "--kill");

        String uuid = UUID.randomUUID().toString();

        // Scan command format errors.
        assertParseArgsThrows("Argument origin_node_id required.", "--kill", "scan");
        assertParseArgsThrows("Argument cache_name required.", "--kill", "scan", uuid);
        assertParseArgsThrows("Argument query_id required.", "--kill", "scan", uuid, "cache");

        assertParseArgsThrows("String representation of \"java.util.UUID\" is exepected", IllegalArgumentException.class,
            "--kill", "scan", "not_a_uuid");

        assertParseArgsThrows("Can't parse number 'not_a_number'", NumberFormatException.class,
            "--kill", "scan", uuid, "my-cache", "not_a_number");

        // Compute command format errors.
        assertParseArgsThrows("Argument session_id required.", "--kill", "compute");

        assertParseArgsThrows("Invalid UUID string: not_a_uuid", IllegalArgumentException.class,
            "--kill", "compute", "not_a_uuid");

        // Service command format errors.
        assertParseArgsThrows("Argument name required.", "--kill", "service");

        // Transaction command format errors.
        assertParseArgsThrows("Argument xid required.", "--kill", "transaction");

        // SQL command format errors.
        assertParseArgsThrows("Argument query_id required.", "--kill", "sql");

        assertParseArgsThrows("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT,
            "--kill", "sql", "not_sql_id");

        // Continuous command format errors.
        assertParseArgsThrows("Argument origin_node_id required.", "--kill", "continuous");

        assertParseArgsThrows("Argument routine_id required.", "--kill", "continuous", UUID.randomUUID().toString());

        assertParseArgsThrows("String representation of \"java.util.UUID\" is exepected", IllegalArgumentException.class,
            "--kill", "continuous", "not_a_uuid");

        assertParseArgsThrows("String representation of \"java.util.UUID\" is exepected", IllegalArgumentException.class,
            "--kill", "continuous", UUID.randomUUID().toString(), "not_a_uuid");
    }

    /**
     * Negative argument validation test for tracing-configuration command.
     *
     * validate that following tracing-configuration arguments validated as expected:
     * <ul>
     *     <li>
     *         reset_all, get_all
     *         <ul>
     *             <li>
     *                 --scope
     *                 <ul>
     *                     <li>
     *                         if value is missing:
     *                          IllegalArgumentException
     *                          (The scope should be specified.
     *                          The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].")
     *                     </li>
     *                     <li>
     *                         if unsupported value is used:
     *                          IllegalArgumentException
     *                          (Invalid scope 'aaa'. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX])
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         reset, get:
     *         <ul>
     *             <li>
     *                 --scope
     *                 <ul>
     *                     <li>
     *                         if value is missing:
     *                          IllegalArgumentException
     *                          (The scope should be specified.
     *                          The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].")
     *                     </li>
     *                     <li>
     *                         if unsupported value is used:
     *                          IllegalArgumentException
     *                          (Invalid scope 'aaa'. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX])
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 --label
     *                 <ul>
     *                     <li>
     *                         if value is missing:
     *                          IllegalArgumentException (The label should be specified.)
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         set:
     *         <ul>
     *             <li>
     *                 --scope
     *                 <ul>
     *                     <li>
     *                         if value is missing:
     *                          IllegalArgumentException
     *                          (The scope should be specified.
     *                          The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].")
     *                     </li>
     *                     <li>
     *                         if unsupported value is used:
     *                          IllegalArgumentException
     *                          (Invalid scope 'aaa'. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX])
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 --label
     *                 <ul>
     *                     <li>
     *                          if value is missing:
     *                              IllegalArgumentException (The label should be specified.)
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 --sampling-rate
     *                 <ul>
     *                     <li>
     *                          if value is missing:
     *                              IllegalArgumentException
     *                              (The sampling-rate should be specified. Decimal value between 0 and 1 should be used.)
     *                     </li>
     *                     <li>
     *                          if unsupported value is used:
     *                              IllegalArgumentException (Invalid samling-rate 'aaa'. Decimal value between 0 and 1 should be used.)
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 --included-scopes
     *                 <ul>
     *                     <li>
     *                          if value is missing:
     *                              IllegalArgumentException (At least one supported scope should be specified.)
     *                     </li>
     *                     <li>
     *                          if unsupported value is used:
     *                              IllegalArgumentException
     *                              (Invalid supported scope: aaa.
     *                              The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].)
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     * </ul>
     */
    @Test
    public void testTracingConfigurationArgumentsValidation() {
        // reset
        assertParseArgsThrows("Please specify a value for argument: --scope", "--tracing-configuration", "reset", "--scope");

        assertParseArgsThrows("Failed to parse --scope command argument", "--tracing-configuration", "reset", "--scope", "aaa");

        assertParseArgsThrows("Please specify a value for argument: --label", "--tracing-configuration", "reset", "--label");

        // reset all
        assertParseArgsThrows("Please specify a value for argument: --scope", "--tracing-configuration", "reset_all", "--scope");

        assertParseArgsThrows("Failed to parse --scope command argument", "--tracing-configuration", "reset_all", "--scope", "aaa");

        // get
        assertParseArgsThrows("Please specify a value for argument: --scope", "--tracing-configuration", "get", "--scope");

        assertParseArgsThrows("Failed to parse --scope command argument", "--tracing-configuration", "get", "--scope", "aaa");

        assertParseArgsThrows("Please specify a value for argument: --label", "--tracing-configuration", "get", "--label");

        // get all
        assertParseArgsThrows("Please specify a value for argument: --scope", "--tracing-configuration", "get_all", "--scope");

        assertParseArgsThrows("Failed to parse --scope command argument", "--tracing-configuration", "get_all", "--scope", "aaa");

        // set
        assertParseArgsThrows("Please specify a value for argument: --scope", "--tracing-configuration", "set", "--scope");

        assertParseArgsThrows("Failed to parse --scope command argument", "--tracing-configuration", "set", "--scope", "aaa");

        assertParseArgsThrows("Please specify a value for argument: --label", "--tracing-configuration", "set", "--label");

        assertParseArgsThrows("Please specify a value for argument: --sampling-rate", "--tracing-configuration", "set", "--sampling-rate");

        assertParseArgsThrows("Failed to parse --sampling-rate command argument",
            "--tracing-configuration", "set", "--sampling-rate", "aaa");

        assertParseArgsThrows("Invalid sampling-rate '-1.0'. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate", "-1", "--scope", "SQL");

        assertParseArgsThrows("Invalid sampling-rate '2.0'. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate", "2", "--scope", "SQL");

        assertParseArgsThrows("Please specify a value for argument: --included-scopes",
            "--tracing-configuration", "set", "--included-scopes");

        assertParseArgsThrows("Failed to parse --included-scopes command argument",
            "--tracing-configuration", "set", "--included-scopes", "TX,aaa");
    }

    /**
     * Positive argument validation test for tracing-configuration command.
     */
    @Test
    public void testTracingConfigurationArgumentsValidationMandatoryArgumentSet() {
        parseArgs(asList("--tracing-configuration"));

        parseArgs(asList("--tracing-configuration", "get_all"));

        assertParseArgsThrows("Mandatory argument(s) missing: [--scope]", "--tracing-configuration", "reset");

        assertParseArgsThrows("Mandatory argument(s) missing: [--scope]", "--tracing-configuration", "get");

        assertParseArgsThrows("Mandatory argument(s) missing: [--scope]", "--tracing-configuration", "set");
    }

    /**
     * Test checks that option {@link ArgumentParser#CMD_VERBOSE} is parsed
     * correctly and if it is not present, it takes the default value
     * {@code false}.
     */
    @Test
    public void testParseVerboseOption() {
        new IgniteCommandRegistry().commands().forEachRemaining(e -> {
            Command<?, ?> cmd = e.getValue();

            if (requireArgs(cmd.getClass()))
                return;

            assertFalse(cmd.toString(), parseArgs(singletonList(cmdText(cmd))).verbose());
            assertTrue(cmd.toString(), parseArgs(asList(cmdText(cmd), CMD_VERBOSE)).verbose());
        });
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testIndexForceRebuildWrongArgs() {
        String nodeId = UUID.randomUUID().toString();

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_force_rebuild", "--node-id")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --node-id"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_force_rebuild", "--node-id", nodeId, "--cache-names")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --cache-names"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_force_rebuild", "--node-id", nodeId, "--group-names")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --group-names"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                CACHE, "indexes_force_rebuild",
                "--node-id", nodeId,
                "--group-names", "someNames",
                "--cache-names", "someNames"
            )),
            IllegalArgumentException.class,
            "Only one of [--group-names, --cache-names] allowed"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                CACHE, "indexes_force_rebuild",
                "--node-id", nodeId,
                "--cache-names", "someNames",
                "--cache-names", "someMoreNames"
            )),
            IllegalArgumentException.class,
            "--cache-names argument specified twice"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                CACHE, "indexes_force_rebuild",
                "--node-id", nodeId,
                "--group-names", "someNames",
                "--group-names", "someMoreNames"
            )),
            IllegalArgumentException.class,
            "--group-names argument specified twice"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                CACHE, "indexes_force_rebuild",
                "--node-id", nodeId,
                "--group-names", "--some-other-arg"
            )),
            IllegalArgumentException.class,
            "Unexpected value: --some-other-arg"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                CACHE, "indexes_force_rebuild",
                "--node-id", nodeId,
                "--cache-names", "--some-other-arg"
            )),
            IllegalArgumentException.class,
            "Unexpected value: --some-other-arg"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                CACHE, "indexes_force_rebuild",
                "--node-id", nodeId,
                "--node-ids", nodeId + ',' + nodeId,
                "--cache-names", "someNames"
            )),
            IllegalArgumentException.class,
            "Only one of [--node-ids, --all-nodes, --node-id] allowed"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                CACHE, "indexes_force_rebuild",
                "--node-id", nodeId,
                "--all-nodes"
            )),
            IllegalArgumentException.class,
            "Only one of [--node-ids, --all-nodes, --node-id] allowed"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                CACHE, "indexes_force_rebuild",
                "--node-ids", nodeId + ',' + nodeId,
                "--all-nodes"
            )),
            IllegalArgumentException.class,
            "Only one of [--node-ids, --all-nodes, --node-id] allowed"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                CACHE, "indexes_force_rebuild",
                "--node-id", nodeId,
                "--node-ids", nodeId + ',' + nodeId,
                "--all-nodes"
            )),
            IllegalArgumentException.class,
            "Only one of [--node-ids, --all-nodes, --node-id] allowed"
        );
    }

    /** */
    @Test
    public void testScheduleIndexRebuildWrongArgs() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --node-id"
        );

        String nodeId = UUID.randomUUID().toString();

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id", nodeId, "--cache-names")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --cache-names"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id", nodeId, "--node-id", nodeId)),
            IllegalArgumentException.class,
            "--node-id argument specified twice"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id", nodeId, "--cache-names", "a",
                "--cache-names", "b")),
            IllegalArgumentException.class,
            "--cache-names argument specified twice"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id", nodeId, "--group-names", "a",
                "--group-names", "b")),
            IllegalArgumentException.class,
            "--group-names argument specified twice"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild")),
            IllegalArgumentException.class,
            "One of [--group-names, --cache-names] required"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--cache-names", "foo[]")),
            IllegalArgumentException.class,
            "Square brackets must contain comma-separated indexes or not be used at all."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id", nodeId, "--all-nodes", "--group-names", "a")),
            IllegalArgumentException.class,
            "Only one of [--node-ids, --all-nodes, --node-id]"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id", nodeId, "--node-ids", nodeId + ',' + nodeId,
                "--group-names", "a")),
            IllegalArgumentException.class,
            "Only one of [--node-ids, --all-nodes, --node-id]"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--all-nodes", "--node-ids", nodeId + ',' + nodeId,
                "--group-names", "a")),
            IllegalArgumentException.class,
            "Only one of [--node-ids, --all-nodes, --node-id]"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id", nodeId, "--all-nodes",
                "--node-ids", nodeId + ',' + nodeId, "--group-names", "a")),
            IllegalArgumentException.class,
            "Only one of [--node-ids, --all-nodes, --node-id]"
        );
    }

    /** */
    @Test
    public void testScheduleIndexRebuildArgs() {
        UUID nodeId = UUID.randomUUID();

        Map<String, Set<String>> params1 = new HashMap<>();
        params1.put("cache1", new HashSet<>(Arrays.asList("foo", "bar")));
        params1.put("cache2", null);
        params1.put("foocache", new HashSet<>(Arrays.asList("idx", "bar")));
        params1.put("bar", Collections.singleton("foo"));

        CacheScheduleIndexesRebuildCommandArg arg1 = (CacheScheduleIndexesRebuildCommandArg)
            parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id", nodeId.toString(),
                "--cache-names", buildScheduleIndexRebuildCacheNames(params1))).commandArg();

        assertEquals(normalizeScheduleIndexRebuildCacheNamesMap(params1), arg1.cacheToIndexes());
        assertEquals(null, arg1.groupNames());

        Map<String, Set<String>> params2 = new HashMap<>();
        params2.put("cache1", new HashSet<>(Arrays.asList("foo", "bar")));
        params2.put("cache2", null);
        params2.put("foocache", new HashSet<>(Arrays.asList("idx", "bar")));
        params2.put("bar", Collections.singleton("foo"));

        Map<String, Set<String>> normalized = normalizeScheduleIndexRebuildCacheNamesMap(params2);

        CacheScheduleIndexesRebuildCommandArg arg2 = (CacheScheduleIndexesRebuildCommandArg)
            parseArgs(asList(CACHE, "schedule_indexes_rebuild", "--node-id", nodeId.toString(),
                "--cache-names", buildScheduleIndexRebuildCacheNames(params2), "--group-names", "foocache,someGrp")
            ).commandArg();

        assertEquals(normalized, arg2.cacheToIndexes());
        assertArrayEquals(new String[]{"foocache", "someGrp"}, arg2.groupNames());
    }

    /**
     * Builds a new --cache-names parameters map replacing nulls with empty set so it should be the same as
     * the parsed argument of the {@link CacheScheduleIndexesRebuildCommandArg#cacheToIndexes()}.
     *
     * @param paramsMap Cache -> indexes map.
     * @return New map with nulls replaced with empty set.
     */
    private static Map<String, Set<String>> normalizeScheduleIndexRebuildCacheNamesMap(Map<String, Set<String>> paramsMap) {
        return paramsMap.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue() != null ? e.getValue() : emptySet()));
    }

    /**
     * Builds a --cache-names parameter string for the schedule_indexes_rebuild command from the
     * cache -> indexes map.
     * Example: {foo: [], bar: null, test: [idx1, idx2]} will be converted into the "foo[],bar,test[idx1,idx2]" string.
     *
     * @param paramsMap Cache -> indexes map.
     * @return --cache-names parameter string.
     */
    private String buildScheduleIndexRebuildCacheNames(Map<String, Set<String>> paramsMap) {
        return paramsMap.entrySet().stream().map(e -> {
            StringBuilder sb = new StringBuilder();

            String cacheName = e.getKey();
            Set<String> indexes = e.getValue();

            sb.append(cacheName);

            if (indexes != null) {
                sb.append("[");

                sb.append(String.join(",", indexes));

                sb.append("]");
            }

            return sb.toString();
        }).collect(Collectors.joining(","));
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testIndexListWrongArgs() {
        String nodeId = UUID.randomUUID().toString();

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_list", "--node-id")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --node-id"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_list", "--node-id", nodeId, "--group-name")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --group-name"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_list", "--node-id", nodeId, "--group-name", INVALID_REGEX)),
            IllegalArgumentException.class,
            "Invalid group name regex: " + INVALID_REGEX
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_list", "--node-id", nodeId, "--cache-name")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --cache-name"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_list", "--node-id", nodeId, "--cache-name", INVALID_REGEX)),
            IllegalArgumentException.class,
            "Invalid cache name regex: " + INVALID_REGEX
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_list", "--node-id", nodeId, "--index-name")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --index-name"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_list", "--node-id", nodeId, "--index-name", INVALID_REGEX)),
            IllegalArgumentException.class,
            "Invalid index name regex: " + INVALID_REGEX
        );
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testIndexRebuildStatusWrongArgs() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(CACHE, "indexes_list", "--node-id")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --node-id"
        );
    }

    /**
     * Test verifies correctness of parsing of arguments --warm-up command.
     */
    @Test
    public void testWarmUpArgs() {
        String[][] args = {
            {"--warm-up"},
            {"--warm-up", "1"},
            {"--warm-up", "stop"}
        };

        for (String[] arg : args) {
            GridTestUtils.assertThrows(
                null,
                () -> parseArgs(asList(arg)),
                IllegalArgumentException.class,
                "Command warm-up can't be executed"
            );
        }

        assertNotNull(parseArgs(asList("--warm-up", "--stop")));
    }

    /** Tests that enum {@link Argument} has enum constants description: {@link EnumDescription}. */
    @Test
    public void testEnumParameterDescription() {
        new IgniteCommandRegistry().commands().forEachRemaining(e -> checkEnumDescription(e.getValue()));
    }

    /** */
    private void checkEnumDescription(Command<?, ?> cmd) {
        if (cmd instanceof CommandsRegistry)
            ((CommandsRegistry<?, ?>)cmd).commands().forEachRemaining(e -> checkEnumDescription(e.getValue()));

        if (!executable(cmd))
            return;

        Consumer<Field> fldCnsmr = fld -> {
            if (!fld.getType().isEnum())
                return;

            EnumDescription descAnn = fld.getAnnotation(EnumDescription.class);

            assertNotNull("Please, specify a description to the enum parameter using " +
                "@" + EnumDescription.class.getSimpleName() + " annotation. " +
                "Parameter: " + cmd.argClass().getSimpleName() + "#" + fld.getName(),
                descAnn);

            assertEquals("Please, specify a description to enum constants: " +
                    stream(fld.getType().getEnumConstants())
                        .filter(e -> stream(descAnn.names()).noneMatch(n -> n.equals(((Enum<?>)e).name())))
                        .collect(Collectors.toSet()) +
                    ". Parameter: " + cmd.argClass().getSimpleName() + "#" + fld.getName(),
                fld.getType().getEnumConstants().length, descAnn.names().length);

            Argument argAnn = fld.getAnnotation(Argument.class);
            Positional posAnn = fld.getAnnotation(Positional.class);

            if (posAnn == null) {
                assertFalse("Please, set a description for the argument: " +
                        cmd.argClass().getSimpleName() + "#" + fld.getName(),
                    argAnn.description().isEmpty());
            }
            else {
                assertTrue("Please, remove a description for the positional argument: " +
                        cmd.argClass().getSimpleName() + "#" + fld.getName(),
                    argAnn.description().isEmpty());
            }
        };

        visitCommandParams(cmd.argClass(), fldCnsmr, fldCnsmr, (grp, flds) -> flds.forEach(fldCnsmr));
    }

    /**
     * @param args Raw arg list.
     * @return Common parameters container object.
     */
    private ConnectionAndSslParameters parseArgs(List<String> args) {
        return new ArgumentParser(setupTestLogger(), new IgniteCommandRegistry()).parseAndValidate(args);
    }

    /**
     * @return logger for tests.
     */
    private IgniteLogger setupTestLogger() {
        try {
            return U.initLogger(null, getClass().getName(), null, U.defaultWorkDirectory());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Checks that parse arguments fails with {@link IllegalArgumentException} and {@code failMsg} message.
     *
     * @param failMsg Exception message (optional).
     * @param args Incoming arguments.
     */
    private void assertParseArgsThrows(@Nullable String failMsg, String... args) {
        assertParseArgsThrows(failMsg, IllegalArgumentException.class, args);
    }

    /**
     * Checks that parse arguments fails with {@code exception} and {@code failMsg} message.
     *
     * @param failMsg Exception message (optional).
     * @param cls Exception class.
     * @param args Incoming arguments.
     */
    private void assertParseArgsThrows(@Nullable String failMsg, Class<? extends Exception> cls, String... args) {
        assertThrows(null, () -> parseArgs(asList(args)), cls, failMsg);
    }

    /**
     * Return {@code True} if cmd there are required arguments.
     *
     * @return {@code True} if cmd there are required arguments.
     */
    private boolean requireArgs(Class<?> cmd) {
        return cmd == CacheCommand.class ||
            cmd == WalCommand.class ||
            cmd == SetStateCommand.class ||
            cmd == EncryptionCommand.class ||
            cmd == KillCommand.class ||
            cmd == SnapshotCommand.class ||
            cmd == ChangeTagCommand.class ||
            cmd == MetaCommand.class ||
            cmd == WarmUpCommand.class ||
            cmd == PropertyCommand.class ||
            cmd == SystemViewCommand.class ||
            cmd == MetricCommand.class ||
            cmd == DefragmentationCommand.class ||
            cmd == PerformanceStatisticsCommand.class ||
            cmd == ConsistencyCommand.class ||
            cmd == CdcCommand.class;
    }
}
