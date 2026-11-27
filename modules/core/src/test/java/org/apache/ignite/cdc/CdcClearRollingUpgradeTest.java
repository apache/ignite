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

package org.apache.ignite.cdc;

import java.nio.file.Files;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.management.cdc.CdcClearCommandArg;
import org.apache.ignite.internal.management.cdc.CdcClearTask;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RU_PREPARE_VERSION_FINALIZATION;
import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.nodeIds;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** Checks that CDC clear is available only after the Rolling Upgrade. */
public class CdcClearRollingUpgradeTest extends AbstractRollingUpgradeTest {
    /** */
    private static final String OLD_VER = "2.18.0";

    /** */
    private static final int SERVERS = 3;

    /** Servers and one client. */
    private static final int NODES = SERVERS + 1;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, ver);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setCdcEnabled(true)));

        return cfg;
    }

    /** */
    @Test
    public void testCdcClearFeature() throws Exception {
        startOldCluster();

        upgradeAllNodes();

        finalizeClusterVersion(0, MASTER_VER_REF);

        checkCdcClearSucceeds();
    }

    /** */
    @Test
    public void testParallelRollingUpgradeInProgress() throws Exception {
        startOldCluster();

        upgradeAllNodes();

        // Not a coordinator, which sends the message to itself.
        int srvIdx = SERVERS - 1;

        spi(grid(srvIdx)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage<?> snm &&
            snm.type() == RU_PREPARE_VERSION_FINALIZATION.ordinal());

        IgniteInternalFuture<?> finalizeFut = runAsync(() -> ru(srvIdx).finalizeClusterVersion());

        assertTrue(spi(grid(srvIdx)).waitForBlocked(1, getTestTimeout()));

        checkCdcClearFails();

        spi(grid(srvIdx)).stopBlock();

        finalizeFut.get(getTestTimeout());

        checkVersionUpgradeInactive(MASTER_VER_REF);

        checkCdcClearSucceeds();
    }

    /** */
    private void startOldCluster() throws Exception {
        for (int i = 0; i < NODES; i++)
            startGrid(i, OLD_VER, i >= SERVERS);

        grid(0).cluster().state(ACTIVE);

        checkCdcClearFails();
    }

    /** Upgrades all nodes one by one, the command fails until the version is finalized. */
    private void upgradeAllNodes() throws Exception {
        ru(0).enableVersionUpgrade();

        for (int i = 0; i < NODES; i++) {
            upgradeNodeVersion(i, OLD_VER, MASTER_VER_REF);

            checkCdcClearFails();
        }
    }

    /** */
    private void checkCdcClearFails() {
        for (int i = 0; i < NODES; i++) {
            int initiatorIdx = i;

            assertThrowsAnyCause(log, () -> clearCdc(initiatorIdx), IgniteException.class,
                CdcClearTask.FEATURE_NOT_ACTIVE_ERR_MSG);
        }

        for (int i = 0; i < SERVERS; i++)
            assertFalse(cdcModeExists(i));
    }

    /** */
    private void checkCdcClearSucceeds() throws Exception {
        for (int i = 0; i < NODES; i++)
            clearCdc(i);

        // Saved by the clear job for the default CDC manager.
        for (int i = 0; i < SERVERS; i++)
            assertTrue(cdcModeExists(i));
    }

    /** */
    private Void clearCdc(int initiatorIdx) throws Exception {
        VisorTaskArgument<CdcClearCommandArg> arg = new VisorTaskArgument<>(
            nodeIds(grid(initiatorIdx).cluster().forServers().nodes()), new CdcClearCommandArg(), false);

        return grid(initiatorIdx).compute().execute(CdcClearTask.class, arg).result();
    }

    /** */
    private boolean cdcModeExists(int idx) {
        return Files.exists(grid(idx).context().pdsFolderResolver().fileTree().cdcModeState());
    }
}
