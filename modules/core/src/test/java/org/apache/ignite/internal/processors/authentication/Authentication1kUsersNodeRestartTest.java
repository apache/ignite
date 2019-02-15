/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.authentication;

import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for {@link IgniteAuthenticationProcessor} on unstable topology.
 */
public class Authentication1kUsersNodeRestartTest extends GridCommonAbstractTest {
    /** */
    private static final int USERS_COUNT = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(200L * 1024 * 1024)
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridTestUtils.setFieldValue(User.class, "bCryptGensaltLog2Rounds", 4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        GridTestUtils.setFieldValue(User.class, "bCryptGensaltLog2Rounds", 10);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        AuthorizationContext.clear();

        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test1kUsersNodeRestartServer() throws Exception {
        startGrid(0);

        grid(0).cluster().active(true);

        IgniteAuthenticationProcessor authenticationProcessor = grid(0).context().authentication();

        AuthorizationContext actxDflt = authenticationProcessor.authenticate(User.DFAULT_USER_NAME, "ignite");

        AuthorizationContext.context(actxDflt);

        IntStream.range(0, USERS_COUNT).parallel().forEach(
            i -> {
                AuthorizationContext.context(actxDflt);

                try {
                    authenticationProcessor.addUser("test" + i, "init");
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
                finally {
                    AuthorizationContext.clear();
                }
            }
        );

        IntStream.range(0, USERS_COUNT).parallel().forEach(
            i -> {
                AuthorizationContext.context(actxDflt);

                try {
                    authenticationProcessor.updateUser("test"  + i, "passwd_" + i);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
                finally {
                    AuthorizationContext.clear();
                }
            }
        );

        stopGrid(0);

        startGrid(0);

        authenticationProcessor.authenticate("ignite", "ignite");
    }
}
