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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

/**
 * Tests for global statistics view.
 */
public abstract class StatisticsGlobalViewTest extends StatisticsAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        cleanPersistenceDir();

        startGridsMultiThreaded(1);// TODO restore 2
        grid(0).cluster().state(ClusterState.ACTIVE);

        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        createSmallTable(null);
        collectStatistics(StatisticsType.GLOBAL, SMALL_TARGET);
    }

    /**
     * Check that global stats equals on each node in cluster:
     * 1) Check global statistics on each grid nodes.
     * 2) Start new node and check global statistics.
     * 3) Collect statistics configuration and check it on each node.
     */
    @Test
    public void testGlobalStatEquals() throws Exception {
        List<List<Object>> partLines = Arrays.asList(
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "A", (long)SMALL_SIZE, (long)SMALL_SIZE, 0L, 100L, 4, null, null),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "B", (long)SMALL_SIZE, (long)SMALL_SIZE, 0L, 100L, 4, null, null),
            Arrays.asList(SCHEMA, "TABLE", "SMALL", "C", (long)SMALL_SIZE, 10L, 0L, 100L, 4, null, null)
        );

        checkSqlResult("select * from SYS.STATISTICS_GLOBAL_DATA where NAME = 'SMALL'", null, act -> {
            checkContains(partLines, act);
            return true;
        });

        startGrid(2);

        requestGlobalStatistics(SMALL_KEY);

        long minVer = minStatVer(statisticsMgr(0), SMALL_TARGET);

        checkSqlResult("select * from SYS.STATISTICS_GLOBAL_DATA where NAME = 'SMALL' and VERSION >= " + minVer,
            null, list -> !list.isEmpty());

        collectStatistics(StatisticsType.GLOBAL, SMALL_TARGET);

        minVer++;

        checkSqlResult("select * from SYS.STATISTICS_GLOBAL_DATA where NAME = 'SMALL' and VERSION >= " + minVer,
            null, act -> {
            checkContains(partLines, act);
            return true;
        });

    }

    /**
     * Request global statistics by specified key from each node.
     *
     * @param key Key to request global statistics by.
     */
    private void requestGlobalStatistics(StatisticsKey key) {
        for (Ignite ign : G.allGrids()) {
            IgniteStatisticsManagerImpl nodeMgr = statisticsMgr((IgniteEx)ign);

            nodeMgr.getGlobalStatistics(key);
        }
    }
}
