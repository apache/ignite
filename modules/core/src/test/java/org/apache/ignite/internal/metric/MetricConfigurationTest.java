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

package org.apache.ignite.internal.metric;

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl.DATASTORAGE_METRIC_PREFIX;

/** */
public class MetricConfigurationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testInvalidMetricConfigurationName() throws Exception {
        startGrid(0);

        grid(0).cluster().state(ACTIVE);

        checkMetricConfigurationUpdateFailed("invalid");
        checkMetricConfigurationUpdateFailed(".");
        checkMetricConfigurationUpdateFailed(".invalid");
        checkMetricConfigurationUpdateFailed("invalid.metric.name");
        checkMetricConfigurationUpdateFailed(DATASTORAGE_METRIC_PREFIX);
        checkMetricConfigurationUpdateFailed(DATASTORAGE_METRIC_PREFIX + '.');

        grid(0).cluster().state(INACTIVE);

        stopAllGrids();

        startGrid(0);

        grid(0).cluster().state(ACTIVE);
    }

    /** */
    private void checkMetricConfigurationUpdateFailed(String name) {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                grid(0).context().metric().configureHistogram(name, new long[] {1, 2, 3});

                return null;
            },
            IgniteException.class,
            "Failed to find registered metric with specified name [metricName=" + name + ']'
        );

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                grid(0).context().metric().configureHitRate(name, 1000);

                return null;
            },
            IgniteException.class,
            "Failed to find registered metric with specified name [metricName=" + name + ']'
        );
    }
}
