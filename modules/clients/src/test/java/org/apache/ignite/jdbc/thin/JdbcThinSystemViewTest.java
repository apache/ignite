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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests system views on cluster with pds enabled.
 */
@RunWith(Parameterized.class)
public class JdbcThinSystemViewTest extends JdbcThinAbstractSelfTest {
    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** */
    @Parameterized.Parameter
    public boolean isPersistence;

    /** Parameters. */
    @Parameterized.Parameters(name = "isPersistence={0}")
    public static Iterable<Object[]> versions() {
        return Arrays.asList(
            new Boolean[] {true},
            new Boolean[] {false}
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(isPersistence)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllView() throws Exception {
        Set<String> expViews = new HashSet<>(Arrays.asList(
            "SYS.METRICS",
            "SYS.SERVICES",
            "SYS.CACHE_GROUPS",
            "SYS.CACHES",
            "SYS.TASKS",
            "SYS.JOBS",
            "SYS.SQL_QUERIES_HISTORY",
            "SYS.NODES",
            "SYS.SCHEMAS",
            "SYS.NODE_METRICS",
            "SYS.BASELINE_NODES",
            "SYS.BASELINE_NODE_ATTRIBUTES",
            "SYS.INDEXES",
            "SYS.LOCAL_CACHE_GROUPS_IO",
            "SYS.SQL_QUERIES",
            "SYS.SCAN_QUERIES",
            "SYS.NODE_ATTRIBUTES",
            "SYS.TABLES",
            "SYS.CLIENT_CONNECTIONS",
            "SYS.TRANSACTIONS",
            "SYS.VIEWS",
            "SYS.TABLE_COLUMNS",
            "SYS.VIEW_COLUMNS",
            "SYS.CONTINUOUS_QUERIES",
            "SYS.STRIPED_THREADPOOL_QUEUE",
            "SYS.DATASTREAM_THREADPOOL_QUEUE",
            "SYS.CACHE_GROUP_PAGE_LISTS",
            "SYS.DATA_REGION_PAGE_LISTS",
            "SYS.PARTITION_STATES",
            "SYS.BINARY_METADATA",
            "SYS.DISTRIBUTED_METASTORAGE",
            "SYS.DS_QUEUES",
            "SYS.DS_SETS",
            "SYS.DS_ATOMICSEQUENCES",
            "SYS.DS_ATOMICLONGS",
            "SYS.DS_ATOMICREFERENCES",
            "SYS.DS_ATOMICSTAMPED",
            "SYS.DS_COUNTDOWNLATCHES",
            "SYS.DS_SEMAPHORES",
            "SYS.DS_REENTRANTLOCKS",
            "SYS.STATISTICS_LOCAL_DATA",
            "SYS.STATISTICS_PARTITION_DATA",
            "SYS.STATISTICS_CONFIGURATION"
        ));

        if (isPersistence)
            expViews.add("SYS.METASTORAGE");

        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTables(null, null, null, new String[] {"VIEW"});

            Set<String> actualTbls = new HashSet<>(expViews);

            while (rs.next()) {
                actualTbls.add(rs.getString("TABLE_SCHEM") + '.'
                    + rs.getString("TABLE_NAME"));
            }

            assertEquals(expViews, actualTbls);
        }
    }
}
