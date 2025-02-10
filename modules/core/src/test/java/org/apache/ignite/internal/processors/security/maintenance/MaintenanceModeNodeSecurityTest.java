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

package org.apache.ignite.internal.processors.security.maintenance;

import java.io.File;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class MaintenanceModeNodeSecurityTest extends AbstractSecurityTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(
        String instanceName,
        AbstractTestSecurityPluginProvider pluginProv
    ) throws Exception {
        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true));

        return super.getConfiguration(instanceName, pluginProv)
            .setDataStorageConfiguration(dataStorageConfiguration);
    }

    /** Tests that node can be successfully restarted in maintenance mode with security enabled. */
    @Test
    public void testNodeStartInMaintenanceMode() throws Exception {
        IgniteEx crd = startGridAllowAll(getTestIgniteInstanceName(0));
        IgniteEx srv = startGridAllowAll(getTestIgniteInstanceName(1));

        crd.cluster().baselineAutoAdjustEnabled(false);
        crd.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = crd.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1));

        NodeFileTree ft = srv.context().pdsFolderResolver().fileTree();

        for (int k = 0; k < 1000; k++)
            cache.put(k, k);

        GridCacheDatabaseSharedManager srvDbMgr = (GridCacheDatabaseSharedManager)crd.context().cache().context().database();
        GridCacheDatabaseSharedManager crdDbMgr = (GridCacheDatabaseSharedManager)srv.context().cache().context().database();

        srvDbMgr.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();
        crdDbMgr.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();

        crd.cluster().disableWal(cache.getName());

        for (int k = 1000; k < 2000; k++)
            cache.put(k, k);

        stopGrid(1);

        File[] cpMarkers = ft.checkpoint().listFiles();

        for (File cpMark : cpMarkers) {
            if (cpMark.getName().contains("-END"))
                cpMark.delete();
        }

        assertThrows(log, () -> startGridAllowAll(getTestIgniteInstanceName(1)), Exception.class, null);

        assertTrue(startGridAllowAll(getTestIgniteInstanceName(1)).context().maintenanceRegistry().isMaintenanceMode());
    }
}
