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

package org.apache.ignite.internal.processors.cache.warmup;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WarmUpConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Class for testing warm-up.
 */
public class WarmUpSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration());
    }

    /**
     * Test checks that an unknown default warm-up configuration cannot be passed.
     *
     * Steps:
     * 1)Adding an unknown warm-up configuration to {@link DataStorageConfiguration}.
     * 2)Starting node and getting an error.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUnknownDefaultWarmUpConfiguration() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        cfg.getDataStorageConfiguration().setDefaultWarmUpConfiguration(new WarmUpConfiguration() {
        });

        assertThrows(log, () -> startGrid(cfg), IgniteCheckedException.class, null);
    }

    /**
     * Test checks that an unknown data region warm-up configuration cannot be passed.
     *
     * Steps:
     * 1)Adding an unknown warm-up configuration to {@link DataRegionConfiguration}.
     * 2)Starting node and getting an error.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUnknownDataRegionWarmUpConfiguration() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        cfg.getDataStorageConfiguration()
            .setDataRegionConfigurations(
                new DataRegionConfiguration()
                    .setName("error")
                    .setWarmUpConfiguration(new WarmUpConfiguration() {
                    }));

        assertThrows(log, () -> startGrid(cfg), IgniteCheckedException.class, null);
    }
}
