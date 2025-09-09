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
package org.apache.ignite.internal.processors.query.calcite.integration;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.withSecurityContextOnAllNodes;
import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;

/**
 *  Integration test for CREATE/ALTER/DROP USER DDL commands.
 */
public class UserDdlIntegrationTest extends AbstractDdlIntegrationTest {
    /** Security context for default user. */
    private SecurityContext secCtxDflt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
            )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        secCtxDflt = authenticate(grid(0), DFAULT_USER_NAME, "ignite");

        assertNotNull(secCtxDflt);
    }

    /**
     * Creates, alter and drops user.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testCreateAlterDropUser() throws Exception {
        withSecurityContextOnAllNodes(secCtxDflt);

        for (Ignite ignite : G.allGrids()) {
            IgniteEx igniteEx = (IgniteEx)ignite;

            sql(igniteEx, "CREATE USER test WITH PASSWORD 'test'");

            SecurityContext secCtx = authenticate(igniteEx, "TEST", "test");

            assertNotNull(secCtx);
            assertEquals("TEST", secCtx.subject().login());

            sql(igniteEx, "ALTER USER test WITH PASSWORD 'newpasswd'");

            secCtx = authenticate(igniteEx, "TEST", "newpasswd");

            assertNotNull(secCtx);
            assertEquals("TEST", secCtx.subject().login());

            sql(igniteEx, "DROP USER test");

            GridTestUtils.assertThrowsWithCause(() -> authenticate(igniteEx, "TEST", "newpasswd"),
                IgniteAccessControlException.class);
        }
    }
}
