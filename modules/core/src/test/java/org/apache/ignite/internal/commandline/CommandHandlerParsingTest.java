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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import junit.framework.TestCase;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;

import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.Command.WAL;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.WAL_DELETE;
import static org.apache.ignite.internal.commandline.CommandHandler.WAL_PRINT;

/**
 * Tests Command Handler parsing arguments.
 */
public class CommandHandlerParsingTest extends TestCase {
    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        System.setProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, "true");

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        System.clearProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND);

        super.tearDown();
    }

    /**
     * Test that experimental command (i.e. WAL command) is disabled by default.
     */
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
     * Tests parsing and validation for user and password arguments.
     */
    public void testParseAndValidateUserAndPassword() {
        CommandHandler hnd = new CommandHandler();

        for (Command cmd : Command.values()) {
            if (cmd == Command.CACHE || cmd == Command.WAL)
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

            try {
                hnd.parseAndValidate(asList("--user", "testUser", cmd.text()));

                fail("expected exception: Both user and password should be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate(asList("--password", "testPass", cmd.text()));

                fail("expected exception: Both user and password should be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            Arguments args = hnd.parseAndValidate(asList("--user", "testUser", "--password", "testPass", cmd.text()));

            assertEquals("testUser", args.user());
            assertEquals("testPass", args.password());
            assertEquals(cmd, args.command());
        }
    }

    /**
     * Tests parsing and validation  of WAL commands.
     */
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
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            hnd.parseAndValidate(Arrays.asList(WAL.text(), UUID.randomUUID().toString()));

            fail("expected exception: invalid arguments for --wal command");
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tests host and port arguments.
     * Tests connection settings arguments.
     */
    public void testConnectionSettings() {
        CommandHandler hnd = new CommandHandler();

        for (Command cmd : Command.values()) {
            if (cmd == Command.CACHE || cmd == Command.WAL)
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
    @SuppressWarnings("Null")
    public void testTransactionArguments() {
        CommandHandler hnd = new CommandHandler();
        Arguments args;

        args = hnd.parseAndValidate(asList("--tx"));

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

        args = hnd.parseAndValidate(asList("--tx", "minDuration", "120", "minSize", "10", "limit", "100", "order", "SIZE",
            "servers"));

        VisorTxTaskArg arg = args.transactionArguments();

        assertEquals(Long.valueOf(120 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(10), arg.getMinSize());
        assertEquals(Integer.valueOf(100), arg.getLimit());
        assertEquals(VisorTxSortOrder.SIZE, arg.getSortOrder());
        assertEquals(VisorTxProjection.SERVER, arg.getProjection());

        args = hnd.parseAndValidate(asList("--tx", "minDuration", "130", "minSize", "1", "limit", "60", "order", "DURATION",
            "clients"));

        arg = args.transactionArguments();

        assertEquals(Long.valueOf(130 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(1), arg.getMinSize());
        assertEquals(Integer.valueOf(60), arg.getLimit());
        assertEquals(VisorTxSortOrder.DURATION, arg.getSortOrder());
        assertEquals(VisorTxProjection.CLIENT, arg.getProjection());

        args = hnd.parseAndValidate(asList("--tx", "nodes", "1,2,3"));

        arg = args.transactionArguments();

        assertNull(arg.getProjection());
        assertEquals(Arrays.asList("1", "2", "3"), arg.getConsistentIds());
    }
}
