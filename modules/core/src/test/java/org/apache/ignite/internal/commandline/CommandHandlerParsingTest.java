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
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.Commands.CACHE;
import static org.apache.ignite.internal.commandline.Commands.WAL;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_DELETE;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_PRINT;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheCommandList.VALIDATE_INDEXES;
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

            CacheArguments args = getArgs(Arrays.asList(
                CACHE.text(),
                VALIDATE_INDEXES.text(),
                "cache1, cache2",
                nodeId.toString(),
                CHECK_FIRST.toString(),
                Integer.toString(expectedCheckFirst),
                CHECK_THROUGH.toString(),
                Integer.toString(expectedCheckThrough)
            )).cacheArgs();

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

            CacheArguments args = getArgs(
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
            getArgs(
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
            getArgs(Arrays.asList(CACHE.text(), VALIDATE_INDEXES.text(), CHECK_THROUGH.toString()));

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
            CacheArguments args = getArgs(list).cacheArgs();

            if (list.contains(nodeId)) {
                assertEquals("nodeId parameter unexpected value", nodeId, args.nodeId().toString());
            }
            else {
                assertNull(args.nodeId());
            }

            assertEquals(list.contains(delete), args.delete());

            if (list.contains(groups)) {
                assertEquals(3, args.groups().size());
            }
            else {
                assertNull(args.groups());
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
     * Test that experimental command (i.e. WAL command) is disabled by default.
     */
    @Test
    public void testExperimentalCommandIsDisabled() {
        System.clearProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND);

        try {
            getArgs(Arrays.asList(WAL.text(), WAL_PRINT));
        }
        catch (Throwable e) {
            e.printStackTrace();

            assertTrue(e instanceof IllegalArgumentException);
        }

        try {
            getArgs(Arrays.asList(WAL.text(), WAL_DELETE));
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
        for (Commands cmd : Commands.values()) {

            if (cmd == Commands.CACHE || cmd == Commands.WAL)
                continue; // --cache subcommand requires its own specific arguments.

            try {
                getArgs(asList("--truststore"));

                fail("expected exception: Expected truststore");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            ConnectionAndSslParameters args = getArgs(asList("--keystore", "testKeystore", "--keystore-password", "testKeystorePassword", "--keystore-type", "testKeystoreType",
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
        for (Commands cmd : Commands.values()) {
            if (cmd == Commands.CACHE || cmd == Commands.WAL)
                continue; // --cache subcommand requires its own specific arguments.

            try {
                getArgs(asList("--user"));

                fail("expected exception: Expected user name");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                getArgs(asList("--password"));

                fail("expected exception: Expected password");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            ConnectionAndSslParameters args = getArgs(asList("--user", "testUser", "--password", "testPass", cmd.text()));

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
        ConnectionAndSslParameters args = getArgs(Arrays.asList(WAL.text(), WAL_PRINT));

        assertEquals(WAL, args.command());

        assertEquals(WAL_PRINT, args.walAction());

        String nodes = UUID.randomUUID().toString() + "," + UUID.randomUUID().toString();

        args = getArgs(Arrays.asList(WAL.text(), WAL_DELETE, nodes));

        assertEquals(WAL_DELETE, args.walAction());

        assertEquals(nodes, args.walArguments());

        try {
            getArgs(Collections.singletonList(WAL.text()));

            fail("expected exception: invalid arguments for --wal command");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            getArgs(Arrays.asList(WAL.text(), UUID.randomUUID().toString()));

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
        for (Commands cmd : Commands.values()) {
            if (cmd != Commands.DEACTIVATE
                && cmd != Commands.BASELINE
                && cmd != Commands.TX)
                continue;

            ConnectionAndSslParameters args = getArgs(asList(cmd.text()));

            assertEquals(cmd, args.command());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());
            assertEquals(false, args.autoConfirmation());

            switch (cmd) {
                case DEACTIVATE: {
                    args = getArgs(asList(cmd.text(), "--yes"));

                    assertEquals(cmd, args.command());
                    assertEquals(DFLT_HOST, args.host());
                    assertEquals(DFLT_PORT, args.port());
                    assertEquals(true, args.autoConfirmation());

                    break;
                }
                case BASELINE: {
                    for (String baselineAct : asList("add", "remove", "set")) {
                        args = getArgs(asList(cmd.text(), baselineAct, "c_id1,c_id2", "--yes"));

                        assertEquals(cmd, args.command());
                        assertEquals(DFLT_HOST, args.host());
                        assertEquals(DFLT_PORT, args.port());
                        assertEquals(baselineAct, args.baselineArguments().getCmd().text());
                        assertEquals(Arrays.asList("c_id1","c_id2"), args.baselineArguments().getConsistentIds());
                        assertEquals(true, args.autoConfirmation());
                    }

                    break;
                }
                case TX: {
                    args = getArgs(asList(cmd.text(), "--xid", "xid1", "--min-duration", "10", "--kill", "--yes"));

                    assertEquals(cmd, args.command());
                    assertEquals(DFLT_HOST, args.host());
                    assertEquals(DFLT_PORT, args.port());
                    assertEquals(true, args.autoConfirmation());

                    assertEquals("xid1", args.transactionArguments().getXid());
                    assertEquals(10_000, args.transactionArguments().getMinDuration().longValue());
                    assertEquals(VisorTxOperation.KILL, args.transactionArguments().getOperation());
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
        for (Commands cmd : Commands.values()) {
            if (cmd == Commands.CACHE || cmd == Commands.WAL)
                continue; // --cache subcommand requires its own specific arguments.

            ConnectionAndSslParameters args = getArgs(asList(cmd.text()));

            assertEquals(cmd, args.command());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());

            args = getArgs(asList("--port", "12345", "--host", "test-host", "--ping-interval", "5000",
                "--ping-timeout", "40000", cmd.text()));

            assertEquals(cmd, args.command());
            assertEquals("test-host", args.host());
            assertEquals("12345", args.port());
            assertEquals(5000, args.pingInterval());
            assertEquals(40000, args.pingTimeout());

            try {
                getArgs(asList("--port", "wrong-port", cmd.text()));

                fail("expected exception: Invalid value for port:");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                getArgs(asList("--ping-interval", "-10", cmd.text()));

                fail("expected exception: Ping interval must be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                getArgs(asList("--ping-timeout", "-20", cmd.text()));

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

        getArgs(asList("--tx"));

        try {
            getArgs(asList("--tx", "minDuration"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            getArgs(asList("--tx", "minDuration", "-1"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            getArgs(asList("--tx", "minSize"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            getArgs(asList("--tx", "minSize", "-1"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            getArgs(asList("--tx", "label"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            getArgs(asList("--tx", "label", "tx123["));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        try {
            getArgs(asList("--tx", "servers", "nodes", "1,2,3"));

            fail("Expected exception");
        }
        catch (IllegalArgumentException ignored) {
        }

        args = getArgs(asList("--tx", "--min-duration", "120", "--min-size", "10", "--limit", "100", "--order", "SIZE",
            "--servers"));

        VisorTxTaskArg arg = args.transactionArguments();

        assertEquals(Long.valueOf(120 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(10), arg.getMinSize());
        assertEquals(Integer.valueOf(100), arg.getLimit());
        assertEquals(VisorTxSortOrder.SIZE, arg.getSortOrder());
        assertEquals(VisorTxProjection.SERVER, arg.getProjection());

        args = getArgs(asList("--tx", "--min-duration", "130", "--min-size", "1", "--limit", "60", "--order", "DURATION",
            "--clients"));

        arg = args.transactionArguments();

        assertEquals(Long.valueOf(130 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(1), arg.getMinSize());
        assertEquals(Integer.valueOf(60), arg.getLimit());
        assertEquals(VisorTxSortOrder.DURATION, arg.getSortOrder());
        assertEquals(VisorTxProjection.CLIENT, arg.getProjection());

        args = getArgs(asList("--tx", "--nodes", "1,2,3"));

        arg = args.transactionArguments();

        assertNull(arg.getProjection());
        assertEquals(Arrays.asList("1", "2", "3"), arg.getConsistentIds());
    }


    private ConnectionAndSslParameters getArgs(List<String> args) {
        return new CommandArgParser(Collections.emptySet(), true, new CommandLogger()).
            parseAndValidate(new CommandArgIterator(args.iterator(), set));
    }
}
