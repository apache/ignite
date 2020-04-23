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

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for {@link IgniteAuthenticationProcessor}.
 */
public class AuthenticationOnNotActiveClusterTest extends GridCommonAbstractTest {
    /** Nodes count. */
    protected static final int NODES_COUNT = 4;

    /** Client node. */
    protected static final int CLI_NODE = NODES_COUNT - 1;

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
    public void testDefaultUser() throws Exception {
        startGrids(NODES_COUNT - 1);
        startClientGrid(CLI_NODE);

        for (int i = 0; i < NODES_COUNT; ++i) {
            AuthorizationContext actx = grid(i).context().authentication().authenticate("ignite", "ignite");

            assertNotNull(actx);
            assertEquals("ignite", actx.userName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotDefaultUser() throws Exception {
        startGrids(NODES_COUNT - 1);
        startClientGrid(CLI_NODE);
        startGrid(NODES_COUNT);

        grid(0).cluster().active(true);

        AuthorizationContext actxDflt = grid(0).context().authentication().authenticate(User.DFAULT_USER_NAME, "ignite");

        AuthorizationContext.context(actxDflt);

        for (int i = 0; i < 10; ++i)
            grid(0).context().authentication().addUser("test" + i, "passwd");

        stopAllGrids();

        U.sleep(500);

        startGrids(NODES_COUNT - 1);
        startClientGrid(CLI_NODE);

        for (int i = 0; i < NODES_COUNT; ++i) {
            for (int usrCnt = 0; usrCnt < 10; ++usrCnt) {
                AuthorizationContext actx = grid(i).context().authentication().authenticate("test" + usrCnt, "passwd");

                assertNotNull(actx);
                assertEquals("test" + usrCnt, actx.userName());
            }
        }
    }
}
