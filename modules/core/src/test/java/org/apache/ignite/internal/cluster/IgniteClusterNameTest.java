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

package org.apache.ignite.internal.cluster;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertNotEquals;

/**
 * Tests that cluster name is defined at all cluster states.
 */
public class IgniteClusterNameTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public boolean persistentEnabled;

    /** */
    @Parameterized.Parameters(name = "persistentEnabled={0}")
    public static Object[] params() {
        return new Object[] {false, true};
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistentEnabled)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testDefaultClusterName() throws Exception {
        IgniteEx srv = startGrid(0);
        String id = srv.cluster().id().toString();

        assertEquals(id, srv.context().cluster().clusterName());

        if (persistentEnabled) {
            srv.cluster().state(ClusterState.ACTIVE);

            assertEquals(id, srv.context().cluster().clusterName());
        }

        srv.cluster().state(ClusterState.INACTIVE);

        assertEquals(id, srv.context().cluster().clusterName());

        stopAllGrids();

        srv = startGrid(0);

        if (persistentEnabled)
            assertEquals(id, srv.context().cluster().clusterName());
        else
            assertNotEquals(id, srv.context().cluster().clusterName());
    }

}
