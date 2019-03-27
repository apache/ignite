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
import java.util.function.Function;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

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
        return new TestSecurityPluginConfiguration() {
            @Override public Function<GridKernalContext, GridSecurityProcessor> builder() {
                return ctx -> new TestSecurityProcessor(
                    ctx,
                    new TestSecurityData(login, pwd, prmSet),
                    Arrays.asList(clientData)
                );
            }
        };
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
    protected IgniteEx startGrid(String login, SecurityPermissionSet prmSet,
        boolean isClient) throws Exception {
        return startGrid(
            getConfiguration(login, secPluginCfg(login, "", prmSet))
                .setClientMode(isClient)
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
        return startGrid(
            getConfiguration(login, secPluginCfg(login, pwd, prmSet))
                .setClientMode(isClient)
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
    protected IgniteEx startGrid(String instanceName, String login, String pwd,
        SecurityPermissionSet prmSet, boolean isClient) throws Exception {
        return startGrid(getConfiguration(instanceName, secPluginCfg(login, pwd, prmSet))
            .setClientMode(isClient));
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
    protected void assertForbidden(TestRunnable r) {
        assertForbidden(r, SecurityException.class);
    }

    /**
     * @param r Runnable.
     * @param types Array of expected exception types.
     */
    protected void assertForbidden(TestRunnable r, Class... types) {
        try {
            r.run();

            fail("Test should throw one of the following exceptions " + Arrays.toString(types));
        }
        catch (Throwable e) {
            assertThat(cause(e, types), notNullValue());
        }
    }

    /**
     * Gets first cause if passed in {@code 'Throwable'} has one of given classes in {@code 'cause'} hierarchy.
     * <p>
     * Note that this method follows includes {@link Throwable#getSuppressed()} into check.
     *
     * @param t Throwable to check (if {@code null}, {@code null} is returned).
     * @param types Array of cause classes to get cause (if {@code null}, {@code null} is returned).
     * @return First causing exception of passed in class, {@code null} otherwise.
     */
    private Throwable cause(Throwable t, Class... types) {
        for (Throwable th = t; th != null; th = th.getCause()) {
            for (Class cls : types) {
                if (cls.isAssignableFrom(th.getClass()))
                    return th;

                for (Throwable n : th.getSuppressed()) {
                    Throwable found = cause(n, cls);

                    if (found != null)
                        return found;
                }
            }

            if (th.getCause() == th)
                break;
        }

        return null;
    }

    /**
     *
     */
    public interface TestRunnable {
        /**
         *
         */
        void run() throws Exception;
    }
}