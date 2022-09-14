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

import java.util.Collections;
import java.util.HashSet;
import org.apache.ignite.cdc.conflictresolve.CacheVersionConflictResolverPluginProvider;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Check conflict resolver restored on node restart. */
public class ConflictResolverRestartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheVersionConflictResolverPluginProvider<?> pluginCfg = new CacheVersionConflictResolverPluginProvider<>();

        pluginCfg.setClusterId((byte)1);
        pluginCfg.setCaches(new HashSet<>(Collections.singleton(DEFAULT_CACHE_NAME)));

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)))
            .setPluginProviders(pluginCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** Tests {@code clusterId} value set correctly after node restart. */
    @Test
    public void testClusterIdAfterRestart() throws Exception {
        IgniteEx ign = startGrid(1);

        ign.cluster().state(ClusterState.ACTIVE);

        ign.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        checkSetup(ign);

        stopAllGrids();

        ign = startGrid(1);

        ign.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        checkSetup(ign);
    }

    /** */
    private void checkSetup(IgniteEx srv) {
        assertNotNull(srv);

        assertEquals(1, srv.context().cache().context().versions().dataCenterId());

        assertTrue(srv.context().cache().cache(DEFAULT_CACHE_NAME).context().conflictNeedResolve());

        assertEquals(
            1,
            (byte)GridTestUtils.getFieldValue(srv.context().cache().cache(DEFAULT_CACHE_NAME).context(), "conflictRslvr", "clusterId")
        );
    }

}
