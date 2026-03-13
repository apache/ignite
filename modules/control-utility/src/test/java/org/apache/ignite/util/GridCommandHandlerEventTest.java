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

package org.apache.ignite.util;

import java.security.Permissions;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.SecurityCommandHandlerPermissionsTest.DEFAULT_PWD;
import static org.apache.ignite.internal.commandline.SecurityCommandHandlerPermissionsTest.TEST_NO_PERMISSIONS_LOGIN;
import static org.apache.ignite.internal.commandline.SecurityCommandHandlerPermissionsTest.enrichWithConnectionArguments;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermission.EVENTS_DISABLE;
import static org.apache.ignite.plugin.security.SecurityPermission.EVENTS_ENABLE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;
import static org.junit.Assume.assumeTrue;

/** */
public class GridCommandHandlerEventTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String LOGIN_EVT_ENABLE = "test_login_evt_enable";

    /** */
    private static final String LOGIN_EVT_DISABLE = "test_login_evt_disable";

    /** */
    private static final String LOGIN_EVT_STATUS = "test_login_evt_status";

    /** */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testEventEnableDIsable() throws Exception {
        startGrids(2);
        startClientGrid("client");

        assertEvents(false, EventType.EVT_CACHE_STARTED, EventType.EVT_CACHE_STOPPED);

        assertEquals(EXIT_CODE_OK, execute("--event", "enable", "EVT_CACHE_STARTED,EVT_CACHE_STOPPED"));

        assertEvents(true, EventType.EVT_CACHE_STARTED, EventType.EVT_CACHE_STOPPED);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--event", "status"));

        String out = testOut.toString();

        assertTrue(out.contains("EVT_CACHE_STARTED"));
        assertTrue(out.contains("EVT_CACHE_STOPPED"));

        assertEquals(EXIT_CODE_OK, execute("--event", "disable", "EVT_CACHE_STARTED,EVT_CACHE_STOPPED"));

        assertEvents(false, EventType.EVT_CACHE_STARTED, EventType.EVT_CACHE_STOPPED);
    }

    /** */
    @Test
    public void testDifferentSetOfEventsOnNodes() throws Exception {
        startGrid(getConfiguration(getTestIgniteInstanceName(0))
            .setIncludeEventTypes(EventType.EVT_TX_STARTED, EventType.EVT_CACHE_STARTED));

        startGrid(getConfiguration(getTestIgniteInstanceName(1))
            .setIncludeEventTypes(EventType.EVT_TX_STARTED, EventType.EVT_CACHE_STOPPED));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--event", "status"));

        String out = testOut.toString();

        assertTrue(out.contains("EVT_TX_STARTED"));
        assertTrue(out.contains("Warning: Event EVT_CACHE_STARTED is enabled only on part of nodes"));
        assertTrue(out.contains("Warning: Event EVT_CACHE_STOPPED is enabled only on part of nodes"));
    }

    /** */
    @Test
    public void testUnknownEvent() throws Exception {
        startGrids(2);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--event", "enable", "EVT_NO_SUCH_EVENT"));
    }

    /** */
    @Test
    public void testEventList() throws Exception {
        startGrids(2);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--event", "list"));

        String out = testOut.toString();

        for (String evt : U.gridEventNames().values())
            assertTrue(out.contains("EVT_" + evt));
    }

    /** */
    @Test
    public void testEventWithSecurity() throws Exception {
        assumeTrue(cliCommandHandler());

        startGrid(getConfigurationWithSecurity(getTestIgniteInstanceName(0)));
        startGrid(getConfigurationWithSecurity(getTestIgniteInstanceName(1)));

        assertExecuteWithSecurity(LOGIN_EVT_ENABLE, "--event", "enable", "EVT_CACHE_STARTED,EVT_CACHE_STOPPED");

        assertEvents(true, EventType.EVT_CACHE_STARTED, EventType.EVT_CACHE_STOPPED);

        assertExecuteWithSecurity(LOGIN_EVT_STATUS, "--event", "status");

        assertExecuteWithSecurity(LOGIN_EVT_DISABLE, "--event", "disable", "EVT_CACHE_STARTED,EVT_CACHE_STOPPED");

        assertEvents(false, EventType.EVT_CACHE_STARTED, EventType.EVT_CACHE_STOPPED);
    }

    /** */
    private IgniteConfiguration getConfigurationWithSecurity(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = getConfiguration(igniteInstanceName);

        cfg.setPluginProviders(
            new TestSecurityPluginProvider(
                igniteInstanceName,
                DEFAULT_PWD,
                ALL_PERMISSIONS,
                false,
                new TestSecurityData(TEST_NO_PERMISSIONS_LOGIN, DEFAULT_PWD, NO_PERMISSIONS, new Permissions()),
                new TestSecurityData(LOGIN_EVT_ENABLE, DEFAULT_PWD, systemPermissions(EVENTS_ENABLE), new Permissions()),
                new TestSecurityData(LOGIN_EVT_DISABLE, DEFAULT_PWD, systemPermissions(EVENTS_DISABLE), new Permissions()),
                new TestSecurityData(LOGIN_EVT_STATUS, DEFAULT_PWD, systemPermissions(ADMIN_OPS), new Permissions()))
        );

        return cfg;
    }

    /** */
    private void assertExecuteWithSecurity(String allowedLogin, String... cmdArgs) {
        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(enrichWithConnectionArguments(F.asList(cmdArgs),
            TEST_NO_PERMISSIONS_LOGIN)));

        assertEquals(EXIT_CODE_OK, execute(enrichWithConnectionArguments(F.asList(cmdArgs), allowedLogin)));
    }

    /** */
    private void assertEvents(boolean enabled, int... evts) {
        for (Ignite ignite : G.allGrids()) {
            for (int i = 0; i < evts.length; i++)
                assertEquals(enabled, ignite.events().isEnabled(evts[i]));
        }
    }
}
