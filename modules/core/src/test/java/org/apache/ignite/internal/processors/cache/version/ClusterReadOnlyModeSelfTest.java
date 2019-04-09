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

package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ClusterReadOnlyModeSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setClientMode("client".equals(igniteInstanceName))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testReadOnlyFromClient() throws Exception {
        startGrid(0);
        startGrid("client");

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();

        IgniteEx client = grid("client");

        assertTrue("Should be client!", client.configuration().isClientMode());

        client.cluster().readOnly(true);

        assertTrue(client.cluster().readOnly());

        assertEquals(client.cluster().readOnly(), grid(0).cluster().readOnly());

        client.cluster().readOnly(false);

        assertFalse(client.cluster().readOnly());

        assertEquals(client.cluster().readOnly(), grid(0).cluster().readOnly());
    }

    @Test
    public void testRestart() throws Exception {
        startGrids(2).cluster().active(true);

        awaitPartitionMapExchange();

        grid(0).cluster().readOnly(true);

        stopAllGrids();

        startGrids(2);

        awaitPartitionMapExchange();

        assertTrue(grid(0).cluster().readOnly());
        assertTrue(grid(1).cluster().readOnly());
    }

    @Test
    public void testChangeStatusWhenNodeLeft() throws Exception {
        startGrids(2).cluster().active(true);

        awaitPartitionMapExchange();

        stopGrid(1);

        awaitPartitionMapExchange();

        grid(0).cluster().readOnly(true);

        startGrid(1);

        awaitPartitionMapExchange();

        assertTrue(grid(0).cluster().readOnly());
        assertTrue(grid(1).cluster().readOnly());
    }

}
