/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.commandline;

import junit.framework.TestCase;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_PORT;

/**
 * Tests Command Handler parsing arguments.
 */
public class CommandHandlerParsingTest extends TestCase {
    /**
     * Test parsing and validation for user and password arguments.
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
     * Tests connection settings arguments.
     */
    public void testConnectionSettings() {
        CommandHandler hnd = new CommandHandler();

        for (Command cmd : Command.values()) {
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
}
