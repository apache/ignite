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

package org.apache.ignite.compatibility.persistence;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.testframework.junits.SkipTestIfIsJdkNewer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.junit.Test;

/**
 * Test for interval metrics configuration migration.
 */
@SkipTestIfIsJdkNewer(17)
public class MetricConfigurationCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    private static final String CONSISTENT_ID = "node";

    /** Test Ignite version. */
    private static final String IGNITE_VERSION = "2.17.0";

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(CONSISTENT_ID);
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /**
     * Test that folder migration is successfull
     */
    @Test
    public void test() throws Exception {
        String metricName = "io.dataregion.default.AllocationRate";

        // Start old version and configure hit rate metric.
        startGrid(1, IGNITE_VERSION, cfg -> {
            cfg.setConsistentId(CONSISTENT_ID);
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
        }, ignite -> {
            ignite.cluster().state(ClusterState.ACTIVE);

            try {
                ((IgniteEx)ignite).context().metric().configureHitRate(metricName, 12345);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        });

        stopAllGrids();

        IgniteEx ignite = startGrid(0);

        HitRateMetric allocationRate = ignite.context().metric().find(metricName, HitRateMetric.class);

        assertEquals(12345, allocationRate.timeInterval());
    }
}
