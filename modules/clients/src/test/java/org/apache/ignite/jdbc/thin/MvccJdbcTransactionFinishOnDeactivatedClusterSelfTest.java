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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** */
@RunWith(JUnit4.class)
public class MvccJdbcTransactionFinishOnDeactivatedClusterSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxCommitAfterDeactivation() throws Exception {
        checkTxFinishAfterDeactivation(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxRollbackAfterDeactivation() throws Exception {
        checkTxFinishAfterDeactivation(false);
    }

    /** */
    public void checkTxFinishAfterDeactivation(boolean commit) throws Exception {
        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);

        try (Connection conn = connect()) {
            execute(conn, "CREATE TABLE t1(a INT, b VARCHAR, PRIMARY KEY(a)) WITH \"atomicity=TRANSACTIONAL_SNAPSHOT,backups=1\"");
        }

        final CountDownLatch enlistedLatch = new CountDownLatch(1);

        assert node0.cluster().active();

        IgniteInternalFuture txFinishedFut = GridTestUtils.runAsync(() -> {
            executeTransaction(commit, enlistedLatch, () -> !node0.context().state().publicApiActiveState(true));

            return null;
        });

        enlistedLatch.await();

        deactivateThroughClient();

        log.info(">>> Cluster deactivated ...");

        try {
            txFinishedFut.get();
        }
        catch (Exception e) {
            e.printStackTrace();

            fail("Exception is not expected here");
        }
    }

    /** */
    private void executeTransaction(boolean commit, CountDownLatch enlistedLatch,
        GridAbsPredicate beforeCommitCondition) throws Exception {
        try (Connection conn = connect()) {
            execute(conn, "BEGIN");

            execute(conn, "INSERT INTO t1 VALUES (1, '1')");

            log.info(">>> Started transaction and enlisted entries");

            enlistedLatch.countDown();

            GridTestUtils.waitForCondition(beforeCommitCondition, 5_000);

            log.info(">>> Attempting to finish transaction");

            execute(conn, commit ? "COMMIT" : "ROLLBACK");
        }
    }

    /** */
    private static Connection connect() throws Exception {
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
    }

    /** */
    private static void execute(Connection conn, String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    /** */
    private void deactivateThroughClient() throws Exception {
        GridClientConfiguration clientCfg = new GridClientConfiguration();

        clientCfg.setServers(Collections.singletonList("127.0.0.1:11211"));

        try (GridClient client = GridClientFactory.start(clientCfg)) {
            GridClientClusterState state = client.state();

            log.info(">>> Try to deactivate ...");

            state.active(false);
        }
    }
}
