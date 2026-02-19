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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class IgniteSnapshotRollingUpgradeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** Tests that snapshot creation fails when rolling upgrade is enabled. */
    @Test
    public void testSnapshotCreationFailsDuringRollingUpgrade() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ClusterState.ACTIVE);

        IgniteProductVersion curVer = srv.context().discovery().localNode().version();

        IgniteProductVersion targetVer = IgniteProductVersion.fromString(curVer.major()
            + "." + curVer.minor()
            + "." + curVer.maintenance() + 1);

        srv.context().rollingUpgrade().enable(targetVer, false);

        assertTrue(srv.context().rollingUpgrade().enabled());

        Throwable ex = assertThrowsWithCause(
            () -> srv.snapshot().createSnapshot("test").get(getTestTimeout()),
            IgniteException.class
        );

        assertTrue(ex.getMessage().contains("Failed to start distributed process START_SNAPSHOT: rolling upgrade is enabled"));
    }
}
