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
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.ignite.internal.commandline.cache.CacheArguments;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.Command.CACHE;
import static org.apache.ignite.internal.commandline.Command.WAL;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.WAL_DELETE;
import static org.apache.ignite.internal.commandline.CommandHandler.WAL_PRINT;
import static org.apache.ignite.internal.commandline.cache.CacheCommand.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests Command Handler parsing arguments.
 */
public class CommandHandlerParsingTest {
    /** */
    @Before
    public void setUp() throws Exception {
        System.setProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, "true");
    }

    /** */
    @After
    public void tearDown() throws Exception {
        System.clearProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND);
    }

    /**
     * validate_indexes command arguments parsing and validation
     */
    @Test
    public void testValidateIndexArguments() {
        CommandHandler hnd = new CommandHandler();

        //happy case for all parameters
        try {
            int expectedCheckFirst = 10;
            int expectedCheckThrough = 11;
            UUID nodeId = UUID.randomUUID();

            CacheArguments args = hnd.parseAndValidate(
                Arrays.asList(
                    CACHE.text(),
                    VALIDATE_INDEXES.text(),
                    "cache1, cache2",
                    nodeId.toString(),
                    CHECK_FIRST.toString(),
                    Integer.toString(expectedCheckFirst),
                    CHECK_THROUGH.toString(),
                    Integer.toString(expectedCheckThrough)
                )
            ).cacheArgs();

            assertEquals("nodeId parameter unexpected value", nodeId, args.nodeId());
            assertEquals("checkFirst parameter unexpected value", expectedCheckFirst, args.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedCheckThrough, args.checkThrough());
        }
        catch (IllegalArgumentException e) {
            fail("Unexpected exception: " + e);
        }

        try {
            int expectedParam = 11;
            UUID nodeId = UUID.randomUUID();

            CacheArguments args = hnd.parseAndValidate(
                Arrays.asList(
                    CACHE.text(),
                    VALIDATE_INDEXES.text(),
                    nodeId.toString(),
                    CHECK_THROUGH.toString(),
                    Integer.toString(expectedParam)
                )
            ).cacheArgs();

            assertNull("caches weren't specified, null value expected", args.caches());
            assertEquals("nodeId parameter unexpected value", nodeId, args.nodeId());
            assertEquals("checkFirst parameter unexpected value", -1, args.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedParam, args.checkThrough());
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            hnd.parseAndValidate(
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
            hnd.parseAndValidate(Arrays.asList(CACHE.text(), VALIDATE_INDEXES.text(), CHECK_THROUGH.toString()));

            fail("Expected exception hasn't been thrown");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test that experimental command (i.e. WAL command) is disabled by default.
     */
    @Test
    public void testExperimentalCommandIsDisabled() {
        System.clearProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND);

        CommandHandler hnd = new CommandHandler();

        try {
            hnd.parseAndValidate(Arrays.asList(WAL.text(), WAL_PRINT));
        }
        catch (Throwable e) {
            e.printStackTrace();

            assertTrue(e instanceof IllegalArgumentException);
        }

        try {
            hnd.parseAndValidate(Arrays.asList(WAL.text(), WAL_DELETE));
        }
        catch (Throwable e) {
            e.printStackTrace();

            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    /**
     * Tests parsing and validation for the SSL arguments.
     */
    @Test
    public void testParseAndValidateSSLArguments() {
        CommandHandler hnd = new CommandHandler();

        for (Command cmd : Command.values()) {
            if (commandHaveRequeredArguments(cmd))
                continue; // --cache subcommand requires its own specific arguments.

            try {
                hnd.parseAndValidate(asList("--truststore"));

                fail("expected exception: Expected truststore");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            Arguments args = hnd.parseAndValidate(asList("--keystore", "testKeystore", "--keystore-password", "testKeystorePassword", "--keystore-type", "testKeystoreType",
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

            assertEquals(cmd, args.command());
        }
    }


    /**
     * Tests parsing and validation for user and password arguments.
     */
    @Test
    public void testParseAndValidateUserAndPassword() {
        CommandHandler hnd = new CommandHandler();

        for (Command cmd : Command.values()) {
            if (commandHaveRequeredArguments(cmd))
                continue; // --cache subcommand requires its own specific arguments.

            try {
                hnd.parseAndValidate(asList("--user"));

                fail("expected exception: Expected user name");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate(asList("--password"));

                fail("expected exception: Expected password");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            Arguments args = hnd.parseAndValidate(asList("--user", "testUser", "--password", "testPass", cmd.text()));

            assertEquals("testUser", args.getUserName());
            assertEquals("testPass", args.getPassword());
            assertEquals(cmd, args.command());
        }
    }

    /**
     * Tests parsing and validation  of WAL commands.
     */
    @Test
    public void testParseAndValidateWalActions() {
        CommandHandler hnd = new CommandHandler();

        Arguments args = hnd.parseAndValidate(Arrays.asList(WAL.text(), WAL_PRINT));

        assertEquals(WAL, args.command());

        assertEquals(WAL_PRINT, args.walAction());

        String nodes = UUID.randomUUID().toString() + "," + UUID.randomUUID().toString();

        args = hnd.parseAndValidate(Arrays.asList(WAL.text(), WAL_DELETE, nodes));

        assertEquals(WAL_DELETE, args.walAction());

        assertEquals(nodes, args.walArguments());

        try {
            hnd.parseAndValidate(Collections.singletonList(WAL.text()));

            fail("expected exception: invalid arguments for --wal command");
        }
        catch (IllegalArgumentException ignored) {
            /* No-op */
        }

        try {
            hnd.parseAndValidate(Arrays.asList(WAL.text(), UUID.randomUUID().toString()));

            fail("expected exception: invalid arguments for --wal command");
        }
        catch (IllegalArgumentException ignored) {
            /* No-op */
        }
    }

    /**
     * Tests that the auto confirmation flag was correctly parsed.
     */
    @Test
    public void testParseAutoConfirmationFlag() {
        CommandHandler hnd = new CommandHandler();

        for (Command cmd : Command.values()) {
            if (!cmd.confirmationRequired())
                continue;

            Arguments args = hnd.parseAndValidate(asList(cmd.text()));

            checkCommonParametersCorrectlyParsed(cmd, args, false);

            switch (cmd) {
                case DEACTIVATE:
                case READ_ONLY_DISABLE:
                case READ_ONLY_ENABLE:
                    args = hnd.parseAndValidate(asList(cmd.text(), "--yes"));

                    checkCommonParametersCorrectlyParsed(cmd, args, true);

                    break;

                case BASELINE:
                    for (String baselineAct : asList("add", "remove", "set")) {
                        args = hnd.parseAndValidate(asList(cmd.text(), baselineAct, "c_id1,c_id2", "--yes"));

                        checkCommonParametersCorrectlyParsed(cmd, args, true);

                        assertEquals(baselineAct, args.baselineArguments().getCmd().text());
                        assertEquals(Arrays.asList("c_id1","c_id2"), args.baselineArguments().getConsistentIds());
                    }

                    break;

                case TX:
                    args = hnd.parseAndValidate(asList(cmd.text(), "--xid", "xid1", "--min-duration", "10", "--kill", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmd, args, true);

                    assertEquals("xid1", args.transactionArguments().getXid());
                    assertEquals(10_000, args.transactionArguments().getMinDuration().longValue());
                    assertEquals(VisorTxOperation.KILL, args.transactionArguments().getOperation());

                    break;
            }
        }
    }

    /** */
    private void checkCommonParametersCorrectlyParsed(Command cmd, Arguments args, boolean autoConfirm) {
        assertEquals(cmd, args.command());
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
        CommandHandler hnd = new CommandHandler();

        for (Command cmd : Command.values()) {
            if (commandHaveRequeredArguments(cmd))
                continue; // --cache subcommand requires its own specific arguments.

            Arguments args = hnd.parseAndValidate(asList(cmd.text()));

            assertEquals(cmd, args.command());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());

            args = hnd.parseAndValidate(asList("--port", "12345", "--host", "test-host", "--ping-interval", "5000",
                "--ping-timeout", "40000", cmd.text()));

            assertEquals(cmd, args.command());
            assertEquals("test-host", args.host());
            assertEquals("12345", args.port());
            assertEquals(5000, args.pingInterval());
            assertEquals(40000, args.pingTimeout());

            try {
                hnd.parseAndValidate(asList("--port", "wrong-port", cmd.text()));

                fail("expected exception: Invalid value for port:");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate(asList("--ping-interval", "-10", cmd.text()));

                fail("expected exception: Ping interval must be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate(asList("--ping-timeout", "-20", cmd.text()));

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
        CommandHandler hnd = new CommandHandler();

        hnd.parseAndValidate(asList("--tx"));

        try {
            hnd.parseAndValidate(asList("--tx", "minDuration"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            hnd.parseAndValidate(asList("--tx", "minDuration", "-1"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            hnd.parseAndValidate(asList("--tx", "minSize"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            hnd.parseAndValidate(asList("--tx", "minSize", "-1"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            hnd.parseAndValidate(asList("--tx", "label"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            hnd.parseAndValidate(asList("--tx", "label", "tx123["));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            hnd.parseAndValidate(asList("--tx", "servers", "nodes", "1,2,3"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        Arguments args = hnd.parseAndValidate(asList("--tx", "--min-duration", "120", "--min-size", "10", "--limit", "100", "--order", "SIZE",
            "--servers"));

        VisorTxTaskArg arg = args.transactionArguments();

        assertEquals(Long.valueOf(120 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(10), arg.getMinSize());
        assertEquals(Integer.valueOf(100), arg.getLimit());
        assertEquals(VisorTxSortOrder.SIZE, arg.getSortOrder());
        assertEquals(VisorTxProjection.SERVER, arg.getProjection());

        args = hnd.parseAndValidate(asList("--tx", "--min-duration", "130", "--min-size", "1", "--limit", "60", "--order", "DURATION",
            "--clients"));

        arg = args.transactionArguments();

        assertEquals(Long.valueOf(130 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(1), arg.getMinSize());
        assertEquals(Integer.valueOf(60), arg.getLimit());
        assertEquals(VisorTxSortOrder.DURATION, arg.getSortOrder());
        assertEquals(VisorTxProjection.CLIENT, arg.getProjection());

        args = hnd.parseAndValidate(asList("--tx", "--nodes", "1,2,3"));

        arg = args.transactionArguments();

        assertNull(arg.getProjection());
        assertEquals(Arrays.asList("1", "2", "3"), arg.getConsistentIds());
    }

    /** */
    private boolean commandHaveRequeredArguments(Command cmd) {
        return cmd == Command.CACHE ||
            cmd == Command.WAL ||
            cmd == Command.READ_ONLY_DISABLE ||
            cmd == Command.READ_ONLY_ENABLE;
    }


    /**
     * Test parsing arguments by find_garbage command.
     */
    @Test
    public void testFindAndDeleteGarbage() {
        CommandHandler hnd = new CommandHandler();

        String nodeId = UUID.randomUUID().toString();
        String delete = FindAndDeleteGarbageArg.DELETE.toString();
        String groups = "group1,grpoup2,group3";

        List<List<String>> lists = generateArgumentList(
            "find_garbage",
            new T2<>(nodeId, false),
            new T2<>(delete, false),
            new T2<>(groups, false)
        );

        for (List<String> list : lists) {
            Arguments arg = hnd.parseAndValidate(list);

            CacheArguments args = arg.cacheArgs();

            if (list.contains(nodeId))
                assertEquals("nodeId parameter unexpected value", nodeId, args.nodeId().toString());
            else
                assertNull(args.nodeId());

            assertEquals(list.contains(delete), args.delete());

            if (list.contains(groups))
                assertEquals(3, args.groups().size());
            else
                assertNull(args.groups());
        }
    }

    /**
     * @param subcommand Cache subcommand.
     * @param optional List of arguments, where second param is this argument is stopping one or not.
     *  Every argument treated as optional.
     *
     * @return List of every possible combinations of the arguments {[], [1], [2], [3], [1,2], [2,1], [1.3], [3,1], [1,2,3]...}.
     */
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

    /**
     * @param source Source of elements.
     * @param stopFunc Stop function.
     */
    private <T> List<List<T>> generateAllCombinations(List<T> source, Predicate<T> stopFunc) {
        List<List<T>> res = new ArrayList<>();

        for (int i = 0; i < source.size(); i++) {
            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            generateAllCombinations(Collections.singletonList(removed), sourceCopy, stopFunc, res);
        }

        return res;
    }


    /**
     * @param res Intermidiate result.
     * @param source Source of left elements.
     * @param stopFunc Stop function.
     * @param acc Ready result accumulator.
     */
    private <T> void generateAllCombinations(List<T> res, List<T> source, Predicate<T> stopFunc, List<List<T>> acc) {
        acc.add(res);

        if (stopFunc != null && stopFunc.test(res.get(res.size() - 1)))
            return;

        if (source.size() == 1) {
            List<T> list = new ArrayList<>(res);

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
}
