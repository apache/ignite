/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheConfigurations;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheNames;

/**
 * Checks, that enabling cluster read-only mode doesn't break select SQL.
 */
public class ClusterReadOnlyModeDoesNotBreakSqlSelectTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(cacheConfigurations())
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                )
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    /** */
    public void test() throws Exception {
        Ignite crd = startGridsMultiThreaded(2);

        setAndCheckClusterState(crd, ACTIVE);

        for (String cacheName : cacheNames())
            crd.cache(cacheName).put(cacheName.hashCode(), 2 * cacheName.hashCode());

        checkSelect(crd);

        setAndCheckClusterState(crd, ACTIVE_READ_ONLY);

        checkSelect(crd);

        setAndCheckClusterState(crd, INACTIVE);
        setAndCheckClusterState(crd, ACTIVE_READ_ONLY);

        checkSelect(crd);

        setAndCheckClusterState(crd, ACTIVE);

        checkSelect(crd);
    }

    /** */
    private void setAndCheckClusterState(Ignite crd, ClusterState state) {
        crd.cluster().state(state);

        assertEquals(state, crd.cluster().state());
    }

    /** */
    private void checkSelect(Ignite crd) {
        for (String cacheName : cacheNames()) {
            assertEquals(cacheName, 1, crd.cache(cacheName).size());

            try (FieldsQueryCursor<?> cur = crd.cache(cacheName).query(new SqlFieldsQuery("SELECT * FROM Integer"))) {
                List<?> rows = cur.getAll();

                assertEquals(cacheName, 1, rows.size());

                List<?> row = (List<?>)rows.get(0);

                assertEquals(cacheName, 2, row.size());
                assertEquals(cacheName, cacheName.hashCode(), row.get(0));
                assertEquals(cacheName, 2 * cacheName.hashCode(), row.get(1));
            }
        }
    }
}
