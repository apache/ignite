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
import java.util.List;
import java.util.UUID;
import com.google.common.collect.Lists;
import junit.framework.TestCase;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.Command.WAL;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.WAL_DELETE;
import static org.apache.ignite.internal.commandline.CommandHandler.WAL_PRINT;

/**
 * Tests Command Handler parsing arguments.
 */
public class CommandHandlerParsingTest extends TestCase {
    /**
     * Tests parsing and validation for user and password arguments.
     */
    public void testParseAndValidateUserAndPassword() {
        CommandHandler hnd = new CommandHandler();

        for (Command cmd : Command.values()) {
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
                hnd.parseAndValidate(processArgs(asList("--user", "testUser"), cmd));

                fail("expected exception: Both user and password should be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate(processArgs(asList("--password", "testPass"), cmd));

                fail("expected exception: Both user and password should be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            Arguments args = hnd.parseAndValidate(processArgs(asList("--user", "testUser", "--password", "testPass"),
                    cmd));

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
            Arguments args = hnd.parseAndValidate(processArgs(cmd));

            assertEquals(cmd, args.command());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());

            args = hnd.parseAndValidate(processArgs(asList("--port", "12345", "--host", "test-host", "--ping-interval",
                    "5000", "--ping-timeout", "40000"), cmd));

            assertEquals(cmd, args.command());
            assertEquals("test-host", args.host());
            assertEquals("12345", args.port());
            assertEquals(5000, args.pingInterval());
            assertEquals(40000, args.pingTimeout());

            try {
                hnd.parseAndValidate(processArgs(asList("--port", "wrong-port"), cmd));

                fail("expected exception: Invalid value for port:");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate(processArgs(asList("--ping-interval", "-10"), cmd));

                fail("expected exception: Ping interval must be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate(processArgs(asList("--ping-timeout", "-10"), cmd));

                fail("expected exception: Ping timeout must be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Correctly process Command and args in case of WAL command.
     * @param args Args.
     * @param cmd Command.
     */
    private List<String> processArgs(List<String> args, Command cmd) {
        List<String> ret = Lists.newArrayList(args);

        ret.add(cmd.text());

        if (cmd == WAL)
            ret.add(WAL_PRINT);

        return ret;
    };

    /**
     * Correctly process Command and args in case of WAL command.
     * @param cmd Command.
     */
    private List<String> processArgs(Command cmd) {
        List<String> ret = Lists.newArrayList(cmd.text());

        if (cmd == WAL)
            ret.add(WAL_PRINT);

        return ret;
    };
}
