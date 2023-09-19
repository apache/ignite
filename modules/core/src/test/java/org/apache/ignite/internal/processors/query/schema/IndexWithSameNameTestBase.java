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

package org.apache.ignite.internal.processors.query.schema;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.QueryEngineConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.logging.log4j.Level;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;

/** */
public abstract class IndexWithSameNameTestBase extends GridCommonAbstractTest {
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
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(getEngineConfiguration()))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /**
     *
     */
    protected abstract QueryEngineConfiguration getEngineConfiguration();

    /**
     *
     */
    @Test
    public void testRestart() throws Exception {
        int serversCnt = 3;

        startGrids(serversCnt).cluster().state(ClusterState.ACTIVE);

        resetLog4j(Level.DEBUG, false, "org.apache.ignite.internal.processors.query.GridQueryProcessor");

        GridQueryProcessor qryProc = grid(0).context().query();

        String testIdxName = "TEST_IDX";

        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE T1 (k1 INT PRIMARY KEY, v1 INT)"), true).getAll();
        qryProc.querySqlFields(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS " + testIdxName + " ON T1 (k1, v1)"),
            true).getAll();

        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE T2 (k2 INT PRIMARY KEY, v2 INT)"), true).getAll();
        qryProc.querySqlFields(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS " + testIdxName + " ON T2 (k2, v2)"),
            true).getAll();

        for (int i = 0; i < serversCnt; i++)
            checkIndex(i, testIdxName, F.asList("K1", "V1"));

        grid(0).cluster().state(ClusterState.INACTIVE);
        awaitPartitionMapExchange();

        stopAllGrids();

        startGrids(serversCnt);

        for (int i = 0; i < serversCnt; i++)
            checkIndex(i, testIdxName, F.asList("K1", "V1"));
    }

    /**
     * @param nodeIdx Node index.
     * @param idxName Index name.
     * @param expIdxFields Expected index fields.
     */
    private void checkIndex(int nodeIdx, String idxName, Collection<String> expIdxFields) {
        Collection<IndexDescriptor> indexes = grid(nodeIdx).context().query().schemaManager().allIndexes();

        List<IndexDescriptor> filteredIdxs = indexes.stream()
            .filter(idx -> idxName.equalsIgnoreCase(idx.name()))
            .collect(Collectors.toList());

        assertEquals("There should be only one index", 1, filteredIdxs.size());

        Set<String> actualFields = filteredIdxs.get(0)
            .keyDefinitions()
            .keySet()
            .stream()
            .filter(f -> !KEY_FIELD_NAME.equalsIgnoreCase(f))
            .collect(Collectors.toSet());

        assertEqualsCollectionsIgnoringOrder(expIdxFields, actualFields);
    }
}
