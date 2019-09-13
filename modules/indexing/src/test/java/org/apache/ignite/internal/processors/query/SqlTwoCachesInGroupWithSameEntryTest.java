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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class SqlTwoCachesInGroupWithSameEntryTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEYS = 50_000;

    /** Persistence enabled. */
    private boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)));
    }

    /**
     * @throws Exception On error.
     */
    public void test() throws Exception {
        persistenceEnabled = false;
        check(true);
        check(false);

        persistenceEnabled = true;
        check(true);
        check(false);
    }

    /**
     * @param useOnlyPkHashIdx Emulates rebuild index: H2PkHashIndex is used instead of H2TreeIndex.
     * @throws Exception On error.
     */
    @SuppressWarnings("unchecked")
    public void check(boolean useOnlyPkHashIdx) throws Exception {
        try {
            if (persistenceEnabled)
                cleanPersistenceDir();

            IgniteEx ign = startGrid(0);

            ign.cluster().active(true);

            IgniteCache cache0 = ign.createCache(new CacheConfiguration<>("cache0")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setGroupName("grp0")
                .setSqlSchema("CACHE0")
                .setIndexedTypes(Integer.class, Integer.class));

            IgniteCache cache1 = ign.createCache(new CacheConfiguration<>("cache1")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setGroupName("grp0")
                .setSqlSchema("CACHE1")
                .setIndexedTypes(Integer.class, Integer.class));

            for (int i = 0; i < KEYS; ++i) {
                cache0.put(i, i);
                cache1.put(i, i);
            }

            if (useOnlyPkHashIdx) {
                ConcurrentMap<QueryTable, GridH2Table> dataTables = GridTestUtils.getFieldValue(
                    grid(0).context().query().getIndexing(), "dataTables");

                for (GridH2Table t : dataTables.values())
                    GridTestUtils.setFieldValue(t, "rebuildFromHashInProgress", 1);
            }

            assertEquals(KEYS, cache0.size());
            assertEquals(KEYS, cache1.size());
            assertEquals(KEYS, sql("select * FROM cache0.Integer").getAll().size());
            assertEquals(KEYS, sql("select * FROM cache1.Integer").getAll().size());

            cache0.clear();

            assertEquals(0, cache0.size());
            assertEquals(KEYS, cache1.size());
            assertEquals(0, sql("select * FROM cache0.Integer").getAll().size());
            assertEquals(KEYS, sql("select * FROM cache1.Integer").getAll().size());
        }
        finally {
            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object ... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }
}