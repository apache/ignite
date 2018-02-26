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

import static org.apache.ignite.internal.commandline.CommandHandler.CMD_ACTIVATE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_BASE_LINE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_DEACTIVATE;
import static org.apache.ignite.internal.commandline.CommandHandler.CMD_STATE;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_PORT;

/**
 * Tests Command Handler parsing arguments.
 */
public class CommandHandlerParsingTest extends TestCase {
    /** Commands to test. */
    private static final String[] Commands = new String[] {CMD_STATE, CMD_ACTIVATE, CMD_DEACTIVATE, CMD_BASE_LINE};

    /**
     * Test parsing and validation for user and password arguments
     */
    public void testParseAndValidateUserAndPassword() {
        CommandHandler hnd = new CommandHandler();

        for (String cmd : Commands) {
            try {
                hnd.parseAndValidate("--user");

                fail("expected exception: Expected user name");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate("--password");

                fail("expected exception: Expected password");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate("--user", "testUser", cmd);

                fail("expected exception: Both user and password should be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            try {
                hnd.parseAndValidate("--password", "testPass", cmd);

                fail("expected exception: Both user and password should be specified");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }

            Arguments args = hnd.parseAndValidate("--user", "testUser", "--password", "testPass", cmd);

            assertEquals("testUser", args.user());
            assertEquals("testPass", args.password());
            assertEquals(cmd, args.command());
        }
    }

    /**
     * tests host and port arguments
     */
    public void testHostAndPort() {
        CommandHandler hnd = new CommandHandler();

        for (String cmd : Commands) {
            Arguments args = hnd.parseAndValidate(cmd);

            assertEquals(cmd, args.command());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());

            args = hnd.parseAndValidate("--port", "12345", "--host", "test-host", cmd);

            assertEquals(cmd, args.command());
            assertEquals("test-host", args.host());
            assertEquals("12345", args.port());

            try {
                hnd.parseAndValidate("--port", "wrong-port", cmd);

                fail("expected exception: Invalid value for port:");
            }
            catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
    }
}
