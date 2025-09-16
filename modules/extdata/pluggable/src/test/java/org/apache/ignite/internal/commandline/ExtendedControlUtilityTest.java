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

import java.security.Permissions;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandsProviderExtImpl.TEST_COMMAND;
import static org.apache.ignite.internal.commandline.CommandsProviderExtImpl.TEST_COMMAND_ARG;
import static org.apache.ignite.internal.commandline.CommandsProviderExtImpl.TEST_COMMAND_OUTPUT;
import static org.apache.ignite.internal.commandline.CommandsProviderExtImpl.TEST_COMMAND_USAGE;
import static org.apache.ignite.internal.commandline.CommandsProviderExtImpl.TEST_COMPUTE_COMMAND;
import static org.apache.ignite.internal.commandline.SecurityCommandHandlerPermissionsTest.DEFAULT_PWD;
import static org.apache.ignite.internal.commandline.SecurityCommandHandlerPermissionsTest.TEST_LOGIN;
import static org.apache.ignite.internal.management.api.CommandUtils.cmdText;
import static org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor.clearExternalSystemTypes;
import static org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor.registerExternalSystemTypes;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Tests control-utility extension.
 */
public class ExtendedControlUtilityTest extends GridCommandHandlerAbstractTest {
    /**
     * Tests additional command for control-utility works.
     */
    @Test
    public void testAdditionalCommand() throws Exception {
        try (Ignite grid = startGrid(0)) {
            autoConfirmation = false;

            injectTestSystemOut();

            String testVal = "test value";

            assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute(cmdText(TEST_COMMAND)));
            assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute(cmdText(TEST_COMMAND), TEST_COMMAND_ARG));
            assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute(cmdText(TEST_COMMAND), "unknownSubcommand", testVal));

            assertEquals(EXIT_CODE_OK, execute(cmdText(TEST_COMMAND), TEST_COMMAND_ARG, testVal));

            assertContains(log, testOut.toString(), TEST_COMMAND_OUTPUT);
            assertContains(log, testOut.toString(), testVal);
        }
    }

    /**
     * Tests usage help for additional commands.
     */
    @Test
    public void testAdditionalCommandHelp() {
        Assume.assumeTrue(cliCommandHandler());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--help"));

        String testOutStr = testOut.toString();

        assertContains(log, testOutStr, TEST_COMMAND_USAGE);
        assertContains(log, testOutStr, TEST_COMMAND_ARG);
    }

    /** */
    @Test
    public void testAdditionalComputeCommand() throws Exception {
        try (Ignite grid = startGrid(0)) {
            injectTestSystemOut();

            String testVal = "test value";

            assertEquals(EXIT_CODE_OK, execute(cmdText(TEST_COMPUTE_COMMAND), TEST_COMMAND_ARG, testVal));

            assertContains(log, testOut.toString(), testVal);
        }
    }

    /** */
    @Test
    public void testAdditionalComputeCommandSecurityEnabled() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        cfg.setPluginProviders(new TestSecurityPluginProvider(cfg.getIgniteInstanceName(), "", ALL_PERMISSIONS, false,
            new TestSecurityData(TEST_LOGIN, DEFAULT_PWD, ALL_PERMISSIONS, new Permissions())));

        registerExternalSystemTypes(CommandsProviderExtImpl.TestTask.class);

        try (Ignite grid = startGrid(cfg)) {
            injectTestSystemOut();

            String testVal = "test value";

            assertEquals(EXIT_CODE_OK, execute("--user", TEST_LOGIN, "--password", DEFAULT_PWD,
                cmdText(TEST_COMPUTE_COMMAND), TEST_COMMAND_ARG, testVal));

            assertContains(log, testOut.toString(), testVal);
        }
        finally {
            clearExternalSystemTypes();
        }
    }
}
