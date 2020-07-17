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
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.cluster.ClusterState;
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
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.QueryMXBeanImpl.EXPECTED_GLOBAL_QRY_ID_FORMAT;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.CommandList.CLUSTER_CHANGE_TAG;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;
import static org.apache.ignite.internal.commandline.CommandList.WAL;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_VERBOSE;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_DELETE;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_PRINT;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
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

        assertParseArgsThrows("Value for '--check-first' property should be positive.", CACHE.text(), VALIDATE_INDEXES.text(), CHECK_FIRST.toString(), "0");
        assertParseArgsThrows("Numeric value for '--check-through' parameter expected.", CACHE.text(), VALIDATE_INDEXES.text(), CHECK_THROUGH.toString());
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

        T2<String, String> arg = ((WalCommands)args.command()).arg();

        assertEquals(WAL_PRINT, arg.get1());

        String nodes = UUID.randomUUID().toString() + "," + UUID.randomUUID().toString();

        args = parseArgs(asList(WAL.text(), WAL_DELETE, nodes));

        arg = ((WalCommands)args.command()).arg();

        assertEquals(WAL_DELETE, arg.get1());

        assertEquals(nodes, arg.get2());

        assertParseArgsThrows("Expected arguments for " + WAL.text(), WAL.text());

        String rnd = UUID.randomUUID().toString();

        assertParseArgsThrows("Unexpected action " + rnd + " for " + WAL.text(), WAL.text(), rnd);
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

                        ClusterState argState = ((ClusterStateChangeCommand)args.command()).arg();

                        assertEquals(newState, argState.toString());
                    }

                    for (String newState : asList("ACTIVE_READ_ONLY", "ACTIVE", "INACTIVE")) {
                        args = parseArgs(asList(cmdL.text(), newState, "--force", "--yes"));

                        checkCommonParametersCorrectlyParsed(cmdL, args, true);

                        ClusterState argState = ((ClusterStateChangeCommand)args.command()).arg();

                        assertEquals(newState, argState.toString());
                    }

                    break;
                }
                case BASELINE: {
                    for (String baselineAct : asList("add", "remove", "set")) {
                        args = parseArgs(asList(cmdL.text(), baselineAct, "c_id1,c_id2", "--yes"));

                        checkCommonParametersCorrectlyParsed(cmdL, args, true);

                        BaselineArguments arg = ((BaselineCommand)args.command()).arg();

                        assertEquals(baselineAct, arg.getCmd().text());
                        assertEquals(new HashSet<>(asList("c_id1","c_id2")), new HashSet<>(arg.getConsistentIds()));
                    }

                    break;
                }

                case TX: {
                    args = parseArgs(asList(cmdL.text(), "--xid", "xid1", "--min-duration", "10", "--kill", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmdL, args, true);

                    VisorTxTaskArg txTaskArg = ((TxCommands)args.command()).arg();

                    assertEquals("xid1", txTaskArg.getXid());
                    assertEquals(10_000, txTaskArg.getMinDuration().longValue());
                    assertEquals(VisorTxOperation.KILL, txTaskArg.getOperation());

                    break;
                }

                case CLUSTER_CHANGE_TAG: {
                    args = parseArgs(asList(cmdL.text(), "newTagValue", "--yes"));

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

        assertParseArgsThrows("Expecting --min-duration", "--tx", "--min-duration");
        assertParseArgsThrows("Invalid value for --min-duration: -1", "--tx", "--min-duration", "-1");
        assertParseArgsThrows("Expecting --min-size", "--tx", "--min-size");
        assertParseArgsThrows("Invalid value for --min-size: -1", "--tx", "--min-size", "-1");
        assertParseArgsThrows("--label", "--tx", "--label");
        assertParseArgsThrows("Illegal regex syntax", "--tx", "--label", "tx123[");
        assertParseArgsThrows("Projection can't be used together with list of consistent ids.", "--tx", "--servers", "--nodes", "1,2,3");

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
        assertEquals(asList("1", "2", "3"), arg.getConsistentIds());
    }

    /**
     * Test parsing kill arguments.
     */
    @Test
    public void testKillArguments() {
        assertParseArgsThrows("Expected type of resource to kill.", "--kill");

        String uuid = UUID.randomUUID().toString();

        // Scan command format errors.
        assertParseArgsThrows("Expected query originating node id.", "--kill", "scan");
        assertParseArgsThrows("Expected cache name.", "--kill", "scan", uuid);
        assertParseArgsThrows("Expected query identifier.", "--kill", "scan", uuid, "cache");

        assertParseArgsThrows("Invalid UUID string: not_a_uuid", IllegalArgumentException.class,
            "--kill", "scan", "not_a_uuid");

        assertParseArgsThrows("For input string: \"not_a_number\"", NumberFormatException.class,
            "--kill", "scan", uuid, "my-cache", "not_a_number");

        // Compute command format errors.
        assertParseArgsThrows("Expected compute task id.", "--kill", "compute");

        assertParseArgsThrows("Invalid UUID string: not_a_uuid", IllegalArgumentException.class,
            "--kill", "compute", "not_a_uuid");

        // Service command format errors.
        assertParseArgsThrows("Expected service name.", "--kill", "service");

        // Transaction command format errors.
        assertParseArgsThrows("Expected transaction id.", "--kill", "transaction");

        // SQL command format errors.
        assertParseArgsThrows("Expected SQL query id.", "--kill", "sql");

        assertParseArgsThrows("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT,
            "--kill", "sql", "not_sql_id");

        // Continuous command format errors.
        assertParseArgsThrows("Expected query originating node id.", "--kill", "continuous");

        assertParseArgsThrows("Expected continuous query id.", "--kill", "continuous", UUID.randomUUID().toString());

        assertParseArgsThrows("Invalid UUID string: not_a_uuid", IllegalArgumentException.class,
            "--kill", "continuous", "not_a_uuid");

        assertParseArgsThrows("Invalid UUID string: not_a_uuid", IllegalArgumentException.class,
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
     *                          IllegalArgumentException (The scope should be specified. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].")
     *                     </li>
     *                     <li>
     *                         if unsupported value is used:
     *                          IllegalArgumentException (Invalid scope 'aaa'. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX])
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
     *                          IllegalArgumentException (The scope should be specified. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].")
     *                     </li>
     *                     <li>
     *                         if unsupported value is used:
     *                          IllegalArgumentException (Invalid scope 'aaa'. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX])
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
     *                          IllegalArgumentException (The scope should be specified. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].")
     *                     </li>
     *                     <li>
     *                         if unsupported value is used:
     *                          IllegalArgumentException (Invalid scope 'aaa'. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX])
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
     *                              IllegalArgumentException (The sampling-rate should be specified. Decimal value between 0 and 1 should be used.)
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
     *                              IllegalArgumentException (Invalid supported scope: aaa. The following values can be used: [DISCOVERY, EXCHANGE, COMMUNICATION, TX].)
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
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset", "--scope", "aaa");

        assertParseArgsThrows("The label should be specified.",
            "--tracing-configuration", "reset", "--label");

        // reset all
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset_all", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset_all", "--scope", "aaa");

        // get
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get", "--scope", "aaa");

        assertParseArgsThrows("The label should be specified.",
            "--tracing-configuration", "get", "--label");

        // get all
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get_all", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get_all", "--scope", "aaa");

        // set
        assertParseArgsThrows("The scope should be specified. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "set", "--scope");

        assertParseArgsThrows("Invalid scope 'aaa'. The following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "set", "--scope", "aaa");

        assertParseArgsThrows("The label should be specified.",
            "--tracing-configuration", "set", "--label");

        assertParseArgsThrows("The sampling rate should be specified. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate");

        assertParseArgsThrows("Invalid sampling-rate 'aaa'. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate", "aaa");

        assertParseArgsThrows("Invalid sampling-rate '-1'. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate", "-1");

        assertParseArgsThrows("Invalid sampling-rate '2'. Decimal value between 0 and 1 should be used.",
            "--tracing-configuration", "set", "--sampling-rate", "2");

        assertParseArgsThrows("At least one supported scope should be specified.",
            "--tracing-configuration", "set", "--included-scopes");

        assertParseArgsThrows("Invalid supported scope 'aaa'. The following values can be used: "
                + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "set", "--included-scopes", "TX,aaa");
    }

    /**
     * Positive argument validation test for tracing-configuration command.
     */
    @Test
    public void testTracingConfigurationArgumentsValidationMandatoryArgumentSet() {
        parseArgs(asList("--tracing-configuration"));

        parseArgs(asList("--tracing-configuration", "get_all"));

        assertParseArgsThrows("Scope attribute is missing. Following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "reset");

        assertParseArgsThrows("Scope attribute is missing. Following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "get");

        assertParseArgsThrows("Scope attribute is missing. Following values can be used: "
            + Arrays.toString(Scope.values()) + '.', "--tracing-configuration", "set");
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
            cmd == CommandList.METADATA;
    }
}
