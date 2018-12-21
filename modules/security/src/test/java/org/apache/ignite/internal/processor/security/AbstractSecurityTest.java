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

package org.apache.ignite.internal.processor.security;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Common class for security tests.
 */
public class AbstractSecurityTest extends GridCommonAbstractTest {
    /** Test security processor. */
    public static final String TEST_SECURITY_PROCESSOR = "org.apache.ignite.internal.processor.security.TestSecurityProcessor";

    /** Empty array of permissions. */
    protected static final SecurityPermission[] EMPTY_PERMS = new SecurityPermission[0];

    /** Cache name for tests. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** Values. */
    protected AtomicInteger values = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param instanceName Instance name.
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     */
    protected IgniteConfiguration getConfiguration(String instanceName,
        String login, String pwd, SecurityPermissionSet prmSet) throws Exception {

        return getConfiguration(instanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setPluginConfigurations(
                new TestSecurityPluginConfiguration()
                    .setSecurityProcessorClass(TEST_SECURITY_PROCESSOR)
                    .setLogin(login)
                    .setPwd(pwd)
                    .setPermissions(prmSet)
            );
    }

    /**
     * @param idx Index.
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     */
    protected IgniteConfiguration getConfiguration(int idx, String login, String pwd,
        SecurityPermissionSet prmSet) throws Exception {
        return getConfiguration(getTestIgniteInstanceName(idx), login, pwd, prmSet);
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
            getConfiguration(G.allGrids().size(), login, "", prmSet).setClientMode(isClient)
        );
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
            getConfiguration(G.allGrids().size(), login, pwd, prmSet).setClientMode(isClient)
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
        return startGrid(getConfiguration(instanceName, login, pwd, prmSet));
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
        return startGrid(getConfiguration(instanceName, login, pwd, prmSet).setClientMode(isClient));
    }

    /**
     * Getting security permission set builder.
     */
    protected SecurityPermissionSetBuilder builder() {
        return SecurityPermissionSetBuilder.create().defaultAllowAll(false);
    }

    /**
     * Getting allow all security permission set.
     */
    protected SecurityPermissionSet allowAllPermissionSet() {
        return builder().defaultAllowAll(true).build();
    }

    /**
     * @return Cache entry for test.
     */
    protected T2<String, Integer> entry() {
        int val = values.incrementAndGet();

        return new T2<>("key_" + val, -1 * val);
    }

    /**
     * @param c Consumer.
     */
    protected void assertAllowed(Ignite validator, String cacheName, Consumer<T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        c.accept(entry);

        assertThat(validator.cache(cacheName).get(entry.getKey()), is(entry.getValue()));
    }

    /**
     * @param c Consumer.
     */
    protected void assertForbidden(Ignite validator, String cacheName, Consumer<T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        try {
            c.accept(entry);

            fail("Should not happen.");
        }
        catch (Throwable e) {
            assertThat(X.cause(e, SecurityException.class), notNullValue());
        }

        assertThat(validator.cache(cacheName).get(entry.getKey()), nullValue());
    }

    /**
     * Method {@link TestRunnable#run()} should throw {@link SecurityException}.
     *
     * @param r Runnable.
     */
    protected void forbiddenRun(TestRunnable r) {
        forbiddenRun(r, SecurityException.class);
    }

    /**
     * @param r Runnable.
     * @param types Array of expected exception types.
     */
    protected void forbiddenRun(TestRunnable r, Class... types) {
        try {
            r.run();

            fail("Should not happen.");
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