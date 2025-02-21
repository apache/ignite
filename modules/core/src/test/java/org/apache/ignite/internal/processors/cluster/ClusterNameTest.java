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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.misc.VisorIdAndTagViewTask;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 * Tests that cluster name is defined at all cluster states.
 */
public class ClusterNameTest extends GridCommonAbstractTest {
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

        checkDefaultClusterName();

        if (persistentEnabled) {
            srv.cluster().state(ClusterState.ACTIVE);

            checkDefaultClusterName();
        }

        srv.cluster().state(ClusterState.INACTIVE);

        checkDefaultClusterName();
    }

    /** */
    private void checkDefaultClusterName() throws Exception {
        IgniteEx srv = grid(0);

        String name = srv.compute().execute(VisorIdAndTagViewTask.class,
            new VisorTaskArgument<>(F.asList(srv.cluster().node().id()), null, false)).result().clusterName();

        assertEquals(srv.cluster().id().toString(), name);
    }
}
