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

package org.apache.ignite.internal.processors.cache.authentication;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.authentication.UserManagementException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;

/**
 * Test for leaks JdbcConnection on SqlFieldsQuery execute.
 */
public class SqlUserCommandSelfTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_COUNT = 3;

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

        recreateDefaultDb();

        startGrids(NODES_COUNT - 1);

        startClientGrid(NODES_COUNT - 1);

        grid(0).cluster().state(ClusterState.ACTIVE);
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
    public void testCreateUpdateDropUser() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            doSqlAsRoot(i, "CREATE USER test WITH PASSWORD 'test'");

            SecurityContext secCtx = authenticate(grid(i), "TEST", "test");

            assertNotNull(secCtx);
            assertEquals("TEST", secCtx.subject().login());

            doSqlAsRoot(i, "ALTER USER test WITH PASSWORD 'newpasswd'");

            secCtx = authenticate(grid(i), "TEST", "newpasswd");

            assertNotNull(secCtx);
            assertEquals("TEST", secCtx.subject().login());

            doSqlAsRoot(i, "DROP USER test");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateWithAlreadyExistUser() throws Exception {
        doSqlAsRoot(0, "CREATE USER test WITH PASSWORD 'test'");

        for (int i = 0; i < NODES_COUNT; ++i) {
            final int idx = i;

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSqlAsRoot(idx, "CREATE USER test WITH PASSWORD 'test'");

                    return null;
                }
            }, UserManagementException.class, "User already exists [login=TEST]");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAlterDropNotExistUser() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            final int idx = i;

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSqlAsRoot(idx, "ALTER USER test WITH PASSWORD 'test'");

                    return null;
                }
            }, UserManagementException.class, "User doesn't exist [userName=TEST]");

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSqlAsRoot(idx, "DROP USER test");

                    return null;
                }
            }, UserManagementException.class, "User doesn't exist [userName=TEST]");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotAuthenticateOperation() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            final int idx = i;

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSql(idx, "CREATE USER test WITH PASSWORD 'test'");

                    return null;
                }
            }, IgniteAccessControlException.class,
                "User management operations initiated on behalf of the Ignite node are not expected");

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSql(idx, "ALTER USER test WITH PASSWORD 'test'");

                    return null;
                }
            }, IgniteAccessControlException.class,
                "User management operations initiated on behalf of the Ignite node are not expected");

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSql(idx, "DROP USER test");

                    return null;
                }
            }, IgniteAccessControlException.class,
                "User management operations initiated on behalf of the Ignite node are not expected");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotAuthorizedOperation() throws Exception {
        String login = "USER0";
        String pwd = "user0";

        doSqlAsRoot(0, "CREATE USER " + login + " WITH PASSWORD '" + pwd + "'");

        for (int i = 0; i < NODES_COUNT; ++i) {
            final int idx = i;

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSqlAs(idx, "CREATE USER test WITH PASSWORD 'test'", login, pwd);

                    return null;
                }
            }, IgniteAccessControlException.class, "User management operations are not allowed for user");

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSqlAs(idx, "ALTER USER test WITH PASSWORD 'test'", login, pwd);

                    return null;
                }
            }, IgniteAccessControlException.class, "User management operations are not allowed for user");

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSqlAs(idx, "DROP USER test", login, pwd);

                    return null;
                }
            }, IgniteAccessControlException.class, "User management operations are not allowed for user");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDropDefaultUser() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            final int idx = i;

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doSqlAsRoot(idx, "DROP USER \"ignite\"");

                    return null;
                }
            }, IgniteAccessControlException.class, "Default user cannot be removed");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQuotedUsername() throws Exception {
        doSqlAsRoot(0, "CREATE USER \"test\" with password 'test'");

        doSqlAsRoot(0, "CREATE USER \" test\" with password 'test'");

        doSqlAsRoot(0, "CREATE USER \" test \" with password 'test'");

        doSqlAsRoot(0, "CREATE USER \"test \" with password 'test'");

        doSqlAsRoot(0, "CREATE USER \"111\" with password 'test'");
    }

    /** */
    private void doSqlAsRoot(int nodeIdx, String sql) throws Exception {
        doSqlAs(nodeIdx, sql, DFAULT_USER_NAME, "ignite");
    }

    /** */
    private void doSqlAs(int nodeIdx, String sql, String login, String pwd) throws Exception {
        SecurityContext secCtx = authenticate(grid(0), login, pwd);

        try (Scope ignored = grid(nodeIdx).context().security().withContext(secCtx)) {
            doSql(nodeIdx, sql);
        }
    }

    /** */
    private void doSql(int nodeIdx, String sql) {
        List<List<?>> res = grid(nodeIdx).context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertEquals(0L, res.get(0).get(0));
    }
}
