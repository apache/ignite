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

package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Tests for IgniteStatisticsStorage implementations.
 */
public abstract class StatisticsStorageAbstractTest extends StatisticsAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        cleanPersistenceDir();

        //startGridsMultiThreaded(1);
        startGrid(0);
        startGrid(1);
        grid(0).cluster().state(ClusterState.ACTIVE);

        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        sql("DROP TABLE IF EXISTS small");

        sql("CREATE TABLE small (a INT PRIMARY KEY, b INT, c INT)");

        sql("CREATE INDEX small_b ON small(b)");

        sql("CREATE INDEX small_c ON small(c)");

        for (int i = 0; i < SMALL_SIZE; i++)
            sql("INSERT INTO small(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        stopAllGrids();
    }
}
