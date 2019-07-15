/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor;

import java.util.UUID;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.node.VisorCacheRebalanceCollectorTask;
import org.apache.ignite.internal.visor.node.VisorCacheRebalanceCollectorTaskArg;
import org.apache.ignite.internal.visor.node.VisorCacheRebalanceCollectorTaskResult;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.visor.node.VisorNodeBaselineStatus.BASELINE_NOT_AVAILABLE;
import static org.apache.ignite.internal.visor.node.VisorNodeBaselineStatus.NODE_IN_BASELINE;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.REBALANCE_COMPLETE;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.REBALANCE_NOT_AVAILABLE;

/**
 * Tests for {@link VisorCacheRebalanceCollectorTask}.
 */
public class VisorCacheRebalanceCollectorTaskSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setPageSize(1024).setWalMode(LOG_ONLY).setWalSegmentSize(8 * 1024 * 1024);

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(500L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);


        return cfg;
    }

    /**
     * @param ignite Ignite.
     * @param nid Node ID.
     * @return Task result.
     */
    private VisorCacheRebalanceCollectorTaskResult executeTask(IgniteEx ignite, UUID nid) {
        return ignite
            .compute()
            .execute(VisorCacheRebalanceCollectorTask.class,
                new VisorTaskArgument<>(nid, new VisorCacheRebalanceCollectorTaskArg(), false));
    }

    /**
     * This test execute internal tasks over grid with custom balancer.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testCollectingRebalance() throws Exception {
        IgniteEx ignite = startGrids(1);

        UUID nid = ignite.localNode().id();

        VisorCacheRebalanceCollectorTaskResult taskRes = executeTask(ignite, nid);

        assertNotNull(taskRes);
        assertFalse(F.isEmpty(taskRes.getBaseline()));
        assertEquals(1, taskRes.getBaseline().size());
        assertEquals(BASELINE_NOT_AVAILABLE, taskRes.getBaseline().get(nid)); // In new cluster node is not in baseline.
        assertEquals(REBALANCE_NOT_AVAILABLE, taskRes.getRebalance().get(nid));

        ignite.cluster().active(true);

        taskRes = executeTask(ignite, nid);

        assertNotNull(taskRes);
        assertEquals(NODE_IN_BASELINE, taskRes.getBaseline().get(nid));
        assertEquals(REBALANCE_COMPLETE, taskRes.getRebalance().get(nid));

        ignite.cluster().active(false);

        taskRes = executeTask(ignite, nid);

        assertNotNull(taskRes);
        assertFalse(F.isEmpty(taskRes.getBaseline()));
        assertEquals(1, taskRes.getBaseline().size());
        assertEquals(NODE_IN_BASELINE, taskRes.getBaseline().get(nid)); // Node in baseline after first activation.
        assertEquals(REBALANCE_NOT_AVAILABLE, taskRes.getRebalance().get(nid));
    }
}
