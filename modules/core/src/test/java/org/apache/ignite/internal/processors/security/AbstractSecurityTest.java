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

package org.apache.ignite.internal.processors.security;

import java.util.Arrays;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Common class for security tests.
 */
public class AbstractSecurityTest extends GridCommonAbstractTest {
    /** Empty array of permissions. */
    protected static final SecurityPermission[] EMPTY_PERMS = new SecurityPermission[0];

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param instanceName Instance name.
     * @param secCfg Security plugin configuration.
     */
    protected IgniteConfiguration getConfiguration(String instanceName,
        TestSecurityPluginConfiguration secCfg) throws Exception {

        return getConfiguration(instanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setPluginConfigurations(secCfg);
    }

    /**
     * @param idx Index.
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     */
    protected IgniteConfiguration getConfiguration(int idx, String login, String pwd,
        SecurityPermissionSet prmSet) throws Exception {
        return getConfiguration(getTestIgniteInstanceName(idx), secPluginCfg(login, pwd, prmSet));
    }

    /**
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     * @return Security plaugin configuration.
     */
    protected TestSecurityPluginConfiguration secPluginCfg(String login, String pwd, SecurityPermissionSet prmSet,
        TestSecurityData... clientData) {
        return ctx -> new TestSecurityProcessor(ctx,
            new TestSecurityData(login, pwd, prmSet),
            Arrays.asList(clientData));
    }

    /**
     * @param login Login.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startGrid(String login, SecurityPermissionSet prmSet) throws Exception {
        return startGrid(login, "", prmSet, false);
    }

    /**
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startGrid(String login, String pwd, SecurityPermissionSet prmSet) throws Exception {
        return startGrid(login, pwd, prmSet, false);
    }

    /**
     * @param login Login.
     * @param prmSet Security permission set.
     * @param isClient Is client.
     */
    protected IgniteEx startGrid(String login, SecurityPermissionSet prmSet, boolean isClient) throws Exception {
        return startGrid(getConfiguration(login, secPluginCfg(login, "", prmSet)).setClientMode(isClient)
        );
    }

    /**
     * @param login Login.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startClient(String login, SecurityPermissionSet prmSet) throws Exception {
        return startGrid(login, prmSet, true);
    }

    /**
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     * @param isClient Is client.
     */
    protected IgniteEx startGrid(String login, String pwd, SecurityPermissionSet prmSet,
        boolean isClient) throws Exception {
        return startGrid(getConfiguration(login, secPluginCfg(login, pwd, prmSet)).setClientMode(isClient)
        );
    }

    /**
     * @param instanceName Instance name.
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startGrid(String instanceName, String login, String pwd,
        SecurityPermissionSet prmSet) throws Exception {
        return startGrid(getConfiguration(instanceName, secPluginCfg(login, pwd, prmSet)));
    }

    /**
     * @param instanceName Instance name.
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     * @param isClient If true then client mode.
     */
    protected IgniteEx startGrid(String instanceName, String login, String pwd, SecurityPermissionSet prmSet,
        boolean isClient) throws Exception {
        return startGrid(getConfiguration(instanceName, secPluginCfg(login, pwd, prmSet)).setClientMode(isClient));
    }

    /**
     * Getting security permission set builder.
     */
    protected SecurityPermissionSetBuilder builder() {
        return SecurityPermissionSetBuilder.create().defaultAllowAll(true);
    }

    /**
     * Getting allow all security permission set.
     */
    protected SecurityPermissionSet allowAllPermissionSet() {
        return builder().build();
    }

    /**
     * Method {@link TestRunnable#run()} should throw {@link SecurityException}.
     *
     * @param r Runnable.
     */
    protected void assertForbidden(Runnable r) {
        GridTestUtils.assertThrowsWithCause(r, SecurityException.class);
    }
}
