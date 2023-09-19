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

package org.apache.ignite;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IndexWithSameNameTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /**
     *
     */
    @Test
    public void testRestart() throws Exception {
        startGrid(0).cluster().state(ClusterState.ACTIVE);

        GridQueryProcessor qryProc = grid(0).context().query();

        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE T (k INT PRIMARY KEY, v INT)"), true).getAll();
        qryProc.querySqlFields(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS IDX ON T (k, v)"), true).getAll();

        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE T2 (k INT PRIMARY KEY, v INT)"), true).getAll();
        qryProc.querySqlFields(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS IDX ON T2 (k, v)"), true).getAll();

        grid(0).cluster().state(ClusterState.INACTIVE);

        stopGrid(0);

        startGrid(0);
    }
}
