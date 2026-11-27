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
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsCommand.PerformanceStatisticsStartCommandArg;
import org.apache.ignite.internal.management.performancestatistics.PerformanceStatisticsTask;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_PASSWORD;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_USER;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.SecurityCommandHandlerPermissionsTest.DEFAULT_PWD;
import static org.apache.ignite.internal.commandline.SecurityCommandHandlerPermissionsTest.TEST_LOGIN;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;

/** Tests {@link GridSecurityProcessor#authorizeManagementTask}. */
public class ManagementTaskAuthorizationTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String ERR = "Performance statistics start is forbidden";

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return F.asList(CLI_CMD_HND, JMX_CMD_HND);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TestSecurityData cliData = new TestSecurityData(TEST_LOGIN, DEFAULT_PWD, ALL_PERMISSIONS, new Permissions());

        return super.getConfiguration(igniteInstanceName).setPluginProviders(
            new TestSecurityPluginProvider(igniteInstanceName, DEFAULT_PWD, ALL_PERMISSIONS, false, cliData) {
                @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
                    TestSecurityData nodeData = new TestSecurityData(login, pwd, perms, sandboxPerms);

                    return new TestSecurityProcessor(ctx, nodeData, Arrays.asList(clientData), false) {
                        @Override public void authorizeManagementTask(
                            Class<?> taskCls,
                            Object arg,
                            SecurityContext securityCtx
                        ) {
                            if (PerformanceStatisticsTask.class.equals(taskCls)
                                && ((VisorTaskArgument<?>)arg).getArgument() instanceof PerformanceStatisticsStartCommandArg)
                                throw new SecurityException(ERR);
                        }
                    };
                }
            });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        persistenceEnable(false);

        injectTestSystemOut();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void addExtraArguments(List<String> args) {
        super.addExtraArguments(args);

        args.addAll(asList(CMD_USER, TEST_LOGIN, CMD_PASSWORD, DEFAULT_PWD));
    }

    /** */
    @Test
    public void testManagementTaskAuthorization() throws Exception {
        startGrid(0);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--performance-statistics", "start"));
        assertTrue(testOut.toString().contains(ERR));

        assertEquals(EXIT_CODE_OK, execute("--performance-statistics", "status"));
    }
}
