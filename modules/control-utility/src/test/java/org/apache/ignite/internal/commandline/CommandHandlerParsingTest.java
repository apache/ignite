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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.commandline.cache.CacheCommands;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.commandline.cache.CacheValidateIndexes;
import org.apache.ignite.internal.commandline.cache.FindAndDeleteGarbage;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.management.SetStateCommandArg;
import org.apache.ignite.internal.management.ShutdownPolicyCommandArg;
import org.apache.ignite.internal.management.baseline.BaselineAddCommand;
import org.apache.ignite.internal.management.baseline.BaselineAddCommandArg;
import org.apache.ignite.internal.management.baseline.BaselineRemoveCommand;
import org.apache.ignite.internal.management.baseline.BaselineSetCommand;
import org.apache.ignite.internal.management.cache.CacheScheduleIndexesRebuildCommandArg;
import org.apache.ignite.internal.management.tx.TxCommandArg;
import org.apache.ignite.internal.management.wal.WalDeleteCommandArg;
import org.apache.ignite.internal.management.wal.WalPrintCommand.WalPrintCommandArg;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.QueryMXBeanImpl.EXPECTED_GLOBAL_QRY_ID_FORMAT;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.CommandList.CDC;
import static org.apache.ignite.internal.commandline.CommandList.CLUSTER_CHANGE_TAG;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;
import static org.apache.ignite.internal.commandline.CommandList.SHUTDOWN_POLICY;
import static org.apache.ignite.internal.commandline.CommandList.WAL;
import static org.apache.ignite.internal.commandline.CommandList.WARM_UP;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_VERBOSE;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.util.CdcCommandTest.DELETE_LOST_SEGMENT_LINKS;
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

            ConnectionAndSslParameters args = parseArgs(asList(
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

        assertParseArgsThrows(
            "Value for '--check-first' property should be positive.",
            CACHE.text(),
            VALIDATE_INDEXES.text(),
            CHECK_FIRST.toString(),
            "0"
        );
        assertParseArgsThrows(
            "Numeric value for '--check-through' parameter expected.",
            CACHE.text(),
            VALIDATE_INDEXES.text(),
            CHECK_THROUGH.toString()
        );
    }

    /** */
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

            if (list.contains(nodeId))
                assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId().toString());
            else
                assertNull(arg.nodeId());

            assertEquals(list.contains(delete), arg.delete());

            if (list.contains(groups))
                assertEquals(3, arg.groups().size());
            else
                assertNull(arg.groups());
        }
    }

    /** */
    private List<List<String>> generateArgumentList(String subcommand, T2<String, Boolean>...optional) {
        List<List<T2<String, Boolean>>> lists = generateAllCombinations(asList(optional), (x) -> x.get2());

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
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            assertParseArgsThrows("Expected SSL trust store path", "--truststore");

            ConnectionAndSslParameters args = parseArgs(asList(
                "--keystore", "testKeystore",
                "--keystore-password", "testKeystorePassword",
                "--keystore-type", "testKeystoreType",
                "--truststore", "testTruststore",
                "--truststore-password", "testTruststorePassword",
                "--truststore-type", "testTruststoreType",
                "--ssl-key-algorithm", "testSSLKeyAlgorithm",
                "--ssl-protocol", "testSSLProtocol",
                cmd.text())
            );

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
            if (requireArgs(cmd))
                continue;

            assertParseArgsThrows("Expected user name", "--user");
            assertParseArgsThrows("Expected password", "--password");

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
        ConnectionAndSslParameters args = parseArgs(asList(WAL.text(), WAL_PRINT));

        assertEquals(WAL.command(), args.command());

        WalDeleteCommandArg arg = (WalDeleteCommandArg)((DeclarativeCommandAdapter)args.command()).arg();

        assertTrue(arg instanceof WalPrintCommandArg);

        String nodes = UUID.randomUUID().toString() + "," + UUID.randomUUID().toString();

        args = parseArgs(asList(WAL.text(), WAL_DELETE, nodes));

        arg = (WalDeleteCommandArg)((DeclarativeCommandAdapter)args.command()).arg();

        assertFalse(arg instanceof WalPrintCommandArg);

        assertEquals(nodes, String.join(",", arg.consistentIds()));

        assertParseArgsThrows("Command wal can't be executed", WAL.text());

        String rnd = UUID.randomUUID().toString();

        assertParseArgsThrows("Command wal can't be executed", WAL.text(), rnd);
    }

    /**
     * Tets checks a parser of shutdown policy command.
     */
    @Test
    public void testParseShutdownPolicyParameters() {
        ConnectionAndSslParameters args = parseArgs(asList(SHUTDOWN_POLICY.text()));

        assertEquals(SHUTDOWN_POLICY.command(), args.command());

        assertNull(((DeclarativeCommandAdapter<ShutdownPolicyCommandArg>)args.command()).arg().shutdownPolicy());

        for (ShutdownPolicy policy : ShutdownPolicy.values()) {
            args = parseArgs(asList(SHUTDOWN_POLICY.text(), String.valueOf(policy)));

            assertEquals(SHUTDOWN_POLICY.command(), args.command());

            assertSame(policy, ((DeclarativeCommandAdapter<ShutdownPolicyCommandArg>)args.command()).arg().shutdownPolicy());
        }
    }

    /**
     * Tests that the auto confirmation flag was correctly parsed.
     */
    @Test
    public void testParseAutoConfirmationFlag() {
        for (CommandList cmdL : CommandList.values()) {
            // SET_STATE command has mandatory argument used in confirmation message.
            Command cmd = cmdL != SET_STATE ? cmdL.command() : parseArgs(asList(cmdL.text(), "ACTIVE")).command();

            if (cmd.confirmationPrompt() == null)
                continue;

            ConnectionAndSslParameters args;

            if (cmdL == SET_STATE)
                args = parseArgs(asList(cmdL.text(), "ACTIVE"));
            else if (cmdL == CLUSTER_CHANGE_TAG)
                args = parseArgs(asList(cmdL.text(), "newTagValue"));
            else if (cmdL == WARM_UP)
                args = parseArgs(asList(cmdL.text(), "--stop"));
            else if (cmdL == CDC)
                args = parseArgs(asList(cmdL.text(), DELETE_LOST_SEGMENT_LINKS, NODE_ID, UUID.randomUUID().toString()));
            else
                args = parseArgs(asList(cmdL.text()));

            checkCommonParametersCorrectlyParsed(cmdL, args, false);

            switch (cmdL) {
                case DEACTIVATE: {
                    args = parseArgs(asList(cmdL.text(), "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    args = parseArgs(asList(cmdL.text(), "--force", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    break;
                }
                case SET_STATE: {
                    for (String newState : asList("ACTIVE_READ_ONLY", "ACTIVE", "INACTIVE")) {
                        args = parseArgs(asList(cmdL.text(), newState, "--yes"));

                        checkCommonParametersCorrectlyParsed(cmdL, args, true);

                        ClusterState argState =
                            (((DeclarativeCommandAdapter<SetStateCommandArg>)args.command()).arg()).state();

                        assertEquals(newState, argState.toString());
                    }

                    for (String newState : asList("ACTIVE_READ_ONLY", "ACTIVE", "INACTIVE")) {
                        args = parseArgs(asList(cmdL.text(), newState, "--force", "--yes"));

                        checkCommonParametersCorrectlyParsed(cmdL, args, true);

                        ClusterState argState =
                            (((DeclarativeCommandAdapter<SetStateCommandArg>)args.command()).arg()).state();

                        assertEquals(newState, argState.toString());
                    }

                    break;
                }
                case BASELINE: {
                    for (String baselineAct : asList("add", "remove", "set")) {
                        args = parseArgs(asList(cmdL.text(), baselineAct, "c_id1,c_id2", "--yes"));

                        checkCommonParametersCorrectlyParsed(cmdL, args, true);

                        BaselineAddCommandArg arg = ((DeclarativeCommandAdapter<BaselineAddCommandArg>)args.command()).arg();
                        org.apache.ignite.internal.management.api.Command<?, ?> cmd0 =
                            ((DeclarativeCommandAdapter<BaselineAddCommandArg>)args.command()).command();

                        if (baselineAct.equals("add"))
                            assertEquals(BaselineAddCommand.class, cmd0.getClass());
                        else if (baselineAct.equals("remove"))
                            assertEquals(BaselineRemoveCommand.class, cmd0.getClass());
                        else if (baselineAct.equals("set"))
                            assertEquals(BaselineSetCommand.class, cmd0.getClass());

                        assertEquals(new HashSet<>(asList("c_id1", "c_id2")), new HashSet<>(Arrays.asList(arg.consistentIDs())));
                    }

                    break;
                }

                case TX: {
                    args = parseArgs(asList(cmdL.text(), "--xid", "xid1", "--min-duration", "10", "--kill", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    TxCommandArg txTaskArg = ((DeclarativeCommandAdapter<TxCommandArg>)args.command()).arg();

                    assertEquals("xid1", txTaskArg.xid());
                    assertEquals(10_000, txTaskArg.minDuration().longValue());
                    assertTrue(txTaskArg.kill());

                    break;
                }

                case CLUSTER_CHANGE_TAG: {
                    args = parseArgs(asList(cmdL.text(), "newTagValue", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    break;
                }

                case WARM_UP: {
                    args = parseArgs(asList(cmdL.text(), "--stop", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    break;
                }

                case CDC: {
                    args = parseArgs(asList(cmdL.text(), DELETE_LOST_SEGMENT_LINKS,
                        NODE_ID, UUID.randomUUID().toString(), "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    break;
                }

                default:
                    fail("Unknown command: " + cmd);
            }
        }
    }

    /** */
    private void checkCommonParametersCorrectlyParsed(
        CommandList cmd,
        ConnectionAndSslParameters args,
        boolean autoConfirm
    ) {
        assertEquals(cmd.command(), args.command());
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
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

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

            assertParseArgsThrows("Invalid value for port: wrong-port", "--port", "wrong-port", cmd.text());
            assertParseArgsThrows("Invalid value for ping interval: -10", "--ping-interval", "-10", cmd.text());
            assertParseArgsThrows("Invalid value for ping timeout: -20", "--ping-timeout", "-20", cmd.text());
        }
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
        assertParseArgsThrows("Only one of [servers, clients, nodes] allowed", "--tx", "--servers", "--nodes", "1,2,3");

        args = parseArgs(asList("--tx", "--min-duration", "120", "--min-size", "10", "--limit", "100", "--order", "SIZE", "--servers"));

        TxCommandArg arg = ((DeclarativeCommandAdapter<TxCommandArg>)args.command()).arg();

        assertEquals(Long.valueOf(120 * 1000L), arg.minDuration());
        assertEquals(Integer.valueOf(10), arg.minSize());
        assertEquals(Integer.valueOf(100), arg.limit());
        assertEquals(VisorTxSortOrder.SIZE, arg.order());
        assertTrue(arg.servers());
        assertFalse(arg.clients());

        args = parseArgs(asList("--tx", "--min-duration", "130", "--min-size", "1", "--limit", "60", "--order", "DURATION",
            "--clients"));

        arg = ((DeclarativeCommandAdapter<TxCommandArg>)args.command()).arg();

        assertEquals(Long.valueOf(130 * 1000L), arg.minDuration());
        assertEquals(Integer.valueOf(1), arg.minSize());
        assertEquals(Integer.valueOf(60), arg.limit());
        assertEquals(VisorTxSortOrder.DURATION, arg.order());
        assertFalse(arg.servers());
        assertTrue(arg.clients());

        args = parseArgs(asList("--tx", "--nodes", "1,2,3"));

        arg = ((DeclarativeCommandAdapter<TxCommandArg>)args.command()).arg();

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
     * Test checks that option {@link CommonArgParser#CMD_VERBOSE} is parsed
     * correctly and if it is not present, it takes the default value
     * {@code false}.
     */
    @Test
    public void testParseVerboseOption() {
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            assertFalse(cmd.toString(), parseArgs(singletonList(cmd.text())).verbose());
            assertTrue(cmd.toString(), parseArgs(asList(cmd.text(), CMD_VERBOSE)).verbose());
        }
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testIndexForceRebuildWrongArgs() {
        String nodeId = UUID.randomUUID().toString();

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --node-id"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id", nodeId, "--cache-names")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --cache-names"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_force_rebuild", "--node-id", nodeId, "--group-names")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --group-names"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                "--cache", "indexes_force_rebuild",
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
                "--cache", "indexes_force_rebuild",
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
                "--cache", "indexes_force_rebuild",
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
                "--cache", "indexes_force_rebuild",
                "--node-id", nodeId,
                "--group-names", "--some-other-arg"
            )),
            IllegalArgumentException.class,
            "Unexpected value: --some-other-arg"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList(
                "--cache", "indexes_force_rebuild",
                "--node-id", nodeId,
                "--cache-names", "--some-other-arg"
            )),
            IllegalArgumentException.class,
            "Unexpected value: --some-other-arg"
        );
    }

    /** */
    @Test
    public void testScheduleIndexRebuildWrongArgs() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "schedule_indexes_rebuild", "--node-id")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --node-id"
        );

        String nodeId = UUID.randomUUID().toString();

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "schedule_indexes_rebuild", "--node-id", nodeId, "--cache-names")),
            IllegalArgumentException.class,
            "Please specify a value for argument: --cache-names"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "schedule_indexes_rebuild", "--node-id", nodeId, "--node-id", nodeId)),
            IllegalArgumentException.class,
            "--node-id argument specified twice"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "schedule_indexes_rebuild", "--node-id", nodeId, "--cache-names", "a",
                "--cache-names", "b")),
            IllegalArgumentException.class,
            "--cache-names argument specified twice"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "schedule_indexes_rebuild", "--node-id", nodeId, "--group-names", "a",
                "--group-names", "b")),
            IllegalArgumentException.class,
            "--group-names argument specified twice"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "schedule_indexes_rebuild")),
            IllegalArgumentException.class,
            "One of [--group-names, --cache-names] required"
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "schedule_indexes_rebuild", "--cache-names", "foo[]")),
            IllegalArgumentException.class,
            "Square brackets must contain comma-separated indexes or not be used at all."
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

        CacheCommands cacheCommand1 = (CacheCommands)parseArgs(asList("--cache", "schedule_indexes_rebuild", "--node-id", nodeId.toString(),
            "--cache-names", buildScheduleIndexRebuildCacheNames(params1))
        ).command();

        CacheScheduleIndexesRebuildCommandArg arg1 =
            (CacheScheduleIndexesRebuildCommandArg)cacheCommand1.arg().subcommand().arg();
        assertEquals(normalizeScheduleIndexRebuildCacheNamesMap(params1), arg1.cacheToIndexes());
        assertEquals(null, arg1.groupNames());

        Map<String, Set<String>> params2 = new HashMap<>();
        params2.put("cache1", new HashSet<>(Arrays.asList("foo", "bar")));
        params2.put("cache2", null);
        params2.put("foocache", new HashSet<>(Arrays.asList("idx", "bar")));
        params2.put("bar", Collections.singleton("foo"));

        CacheCommands cacheCommand2 = (CacheCommands)parseArgs(asList("--cache", "schedule_indexes_rebuild", "--node-id", nodeId.toString(),
            "--cache-names", buildScheduleIndexRebuildCacheNames(params2), "--group-names", "foocache,someGrp")
        ).command();

        Map<String, Set<String>> normalized = normalizeScheduleIndexRebuildCacheNamesMap(params2);

        CacheScheduleIndexesRebuildCommandArg arg2 =
            (CacheScheduleIndexesRebuildCommandArg)cacheCommand2.arg().subcommand().arg();
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
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id")),
            IllegalArgumentException.class,
            "Failed to read node id."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--group-name")),
            IllegalArgumentException.class,
            "Failed to read group name regex."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--group-name", INVALID_REGEX)),
            IllegalArgumentException.class,
            "Invalid group name regex: " + INVALID_REGEX
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--cache-name")),
            IllegalArgumentException.class,
            "Failed to read cache name regex."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--cache-name", INVALID_REGEX)),
            IllegalArgumentException.class,
            "Invalid cache name regex: " + INVALID_REGEX
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--index-name")),
            IllegalArgumentException.class,
            "Failed to read index name regex."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id", nodeId, "--index-name", INVALID_REGEX)),
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
            () -> parseArgs(asList("--cache", "indexes_list", "--node-id")),
            IllegalArgumentException.class,
            "Failed to read node id."
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

    /**
     * @param args Raw arg list.
     * @return Common parameters container object.
     */
    private ConnectionAndSslParameters parseArgs(List<String> args) {
        return new CommonArgParser(setupTestLogger(), CommandList.commands()).
            parseAndValidate(args.iterator());
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
    private boolean requireArgs(@Nullable CommandList cmd) {
        return cmd == CommandList.CACHE ||
            cmd == CommandList.WAL ||
            cmd == CommandList.SET_STATE ||
            cmd == CommandList.ENCRYPTION ||
            cmd == CommandList.KILL ||
            cmd == CommandList.SNAPSHOT ||
            cmd == CommandList.CLUSTER_CHANGE_TAG ||
            cmd == CommandList.METADATA ||
            cmd == CommandList.WARM_UP ||
            cmd == CommandList.PROPERTY ||
            cmd == CommandList.SYSTEM_VIEW ||
            cmd == CommandList.METRIC ||
            cmd == CommandList.DEFRAGMENTATION ||
            cmd == CommandList.PERFORMANCE_STATISTICS ||
            cmd == CommandList.CONSISTENCY ||
            cmd == CDC;
    }
}
