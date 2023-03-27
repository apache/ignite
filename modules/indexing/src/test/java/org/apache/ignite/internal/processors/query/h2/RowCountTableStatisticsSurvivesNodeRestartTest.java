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
package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 * Checks if table statistics restored after the node has been restarted.
 */
public class RowCountTableStatisticsSurvivesNodeRestartTest extends TableStatisticsAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGridsMultiThreaded(1);

        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        runSql("DROP TABLE IF EXISTS big");
        runSql("DROP TABLE IF EXISTS small");

        runSql("CREATE TABLE big (a INT PRIMARY KEY, b INT, c INT)");
        runSql("CREATE TABLE small (a INT PRIMARY KEY, b INT, c INT)");

        runSql("CREATE INDEX big_b ON big(b)");
        runSql("CREATE INDEX small_b ON small(b)");

        runSql("CREATE INDEX big_c ON big(c)");
        runSql("CREATE INDEX small_c ON small(c)");

        IgniteCache<Integer, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < BIG_SIZE; i++)
            runSql("INSERT INTO big(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        for (int i = 0; i < SMALL_SIZE; i++)
            runSql("INSERT INTO small(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void statisticsSurvivesRestart() throws Exception {
        String sql = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c " +
            "WHERE t1.b >= 0 AND t2.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big");

        stopGrid(0);

        startGrid(0);

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big");
    }
}
