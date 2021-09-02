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

package org.apache.ignite.internal.processors.authentication;

import java.util.stream.IntStream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.withSecurityContextOnAllNodes;

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

        IgniteSecurity sec = grid(0).context().security();

        SecurityContext secCtxDflt = authenticate(grid(0), User.DFAULT_USER_NAME, "ignite");

        withSecurityContextOnAllNodes(secCtxDflt);

        IntStream.range(0, USERS_COUNT).parallel().forEach(
            i -> {
                try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
                    sec.createUser("test" + i, "init".toCharArray());
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        );

        IntStream.range(0, USERS_COUNT).parallel().forEach(
            i -> {
                try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
                    sec.alterUser("test" + i, ("passwd_" + i).toCharArray());
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        );

        stopGrid(0);

        startGrid(0);

        authenticate(grid(0), "ignite", "ignite");
    }
}
