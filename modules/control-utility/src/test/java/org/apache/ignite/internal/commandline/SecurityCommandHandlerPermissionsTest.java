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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.ServicesTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_PASSWORD;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_USER;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.SERVICE_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/** */
public class SecurityCommandHandlerPermissionsTest extends GridCommandHandlerAbstractTest {
    /** */
    public static final String TEST_NO_PERMISSIONS_LOGIN = "no-permissions-login";

    /** */
    public static final String TEST_LOGIN = "cli-admin-login";

    /** */
    public static final String DEFAULT_PWD = "pwd";

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return Collections.singletonList(CLI_CMD_HND);
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

    /** */
    @Test
    public void testClusterTag() throws Exception {
        Ignite ignite = startGrid(0, userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS));

        assertEquals(
            EXIT_CODE_OK,
            execute(enrichWithConnectionArguments(Collections.singleton("--state"), TEST_NO_PERMISSIONS_LOGIN))
        );

        String out = testOut.toString();

        UUID clId = ignite.cluster().id();
        String clTag = ignite.cluster().tag();

        assertTrue(out.contains("Cluster  ID: " + clId));
        assertTrue(out.contains("Cluster tag: " + clTag));
    }

    /** */
    @Test
    public void testCacheScan() throws Exception {
        checkCommandPermissions(asList("--cache", "scan", DEFAULT_CACHE_NAME), cachePermission(CACHE_READ));
    }

    /** */
    @Test
    public void testCacheDestroy() throws Exception {
        checkCommandPermissions(asList("--cache", "destroy", "--caches", DEFAULT_CACHE_NAME), systemPermissions(CACHE_DESTROY));
    }

    /** */
    @Test
    public void testCacheClear() throws Exception {
        checkCommandPermissions(asList("--cache", "clear", "--caches", DEFAULT_CACHE_NAME), cachePermission(CACHE_REMOVE));
    }

    /** */
    @Test
    public void testCacheCreate() throws Exception {
        String ccfgPath = resolveIgnitePath(
            "modules/control-utility/src/test/resources/config/cache/cache-create-correct.xml"
        ).getAbsolutePath();

        checkCommandPermissions(
            asList("--cache", "create", "--springxmlconfig", ccfgPath),
            systemPermissions(CACHE_CREATE)
        );
    }

    /** */
    @Test
    public void testServiceCancel() throws Exception {
        String srvcName = "testService";
        Collection<String> cmdArgs = asList("--kill", "service", srvcName);

        Ignite ignite = startGrid(
            0,
            userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS),
            userData(TEST_LOGIN, servicePermission(srvcName, SERVICE_CANCEL))
        );

        ServiceConfiguration serviceCfg = new ServiceConfiguration();

        serviceCfg.setName(srvcName);
        serviceCfg.setMaxPerNodeCount(1);
        serviceCfg.setTotalCount(1);
        serviceCfg.setService(new ServicesTest.TestService());

        ignite.services().deploy(serviceCfg);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(enrichWithConnectionArguments(cmdArgs, TEST_NO_PERMISSIONS_LOGIN)));
        assertEquals(EXIT_CODE_OK, execute(enrichWithConnectionArguments(cmdArgs, TEST_LOGIN)));
    }

    /** */
    protected IgniteEx startGrid(int idx, TestSecurityData... userData) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setPluginProviders(new TestSecurityPluginProvider(login, DEFAULT_PWD, ALL_PERMISSIONS, false, userData));

        return startGrid(cfg);
    }

    /** */
    private void checkCommandPermissions(Collection<String> cmdArgs, SecurityPermissionSet reqPerms) throws Exception {
        Ignite ignite = startGrid(
            0,
            userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS),
            userData(TEST_LOGIN, reqPerms)
        );

        ignite.createCache(DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(enrichWithConnectionArguments(cmdArgs, TEST_NO_PERMISSIONS_LOGIN)));

        // We are losing command failure cause for --cache clear commnad. See IGNITE-21023 for more details.
        if (!cmdArgs.containsAll(Arrays.asList("--cache", "clear")))
            assertTrue(testOut.toString().contains("Authorization failed"));

        assertEquals(EXIT_CODE_OK, execute(enrichWithConnectionArguments(cmdArgs, TEST_LOGIN)));
    }

    /** */
    public static List<String> enrichWithConnectionArguments(Collection<String> cmdArgs, String login) {
        List<String> args = new ArrayList<>();

        args.add(CMD_USER);
        args.add(login);
        args.add(CMD_PASSWORD);
        args.add(DEFAULT_PWD);

        args.addAll(cmdArgs);

        return args;
    }

    /** */
    private SecurityPermissionSet cachePermission(SecurityPermission... perms) {
        return SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendCachePermissions(DEFAULT_CACHE_NAME, perms)
            .build();
    }

    /** */
    private SecurityPermissionSet servicePermission(String name, SecurityPermission... perms) {
        return SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendServicePermissions(name, perms)
            .build();
    }

    /** */
    private TestSecurityData userData(String login, SecurityPermissionSet perms) {
        return new TestSecurityData(
            login,
            DEFAULT_PWD,
            perms,
            new Permissions()
        );
    }
}
