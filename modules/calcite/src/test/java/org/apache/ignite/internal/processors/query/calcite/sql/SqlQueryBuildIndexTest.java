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

package org.apache.ignite.internal.processors.query.calcite.sql;

import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class SqlQueryBuildIndexTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String TBL = "Person";

    /** */
    private static final String IDX = "TEST_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private boolean persistenceEnabled;

    /** */
    @Parameterized.Parameter
    public String qryNode;

    /** */
    @Parameterized.Parameters(name = "qryNode={0}")
    public static Object[] parameters() {
        return new Object[] {"CRD", "CLN"};
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        QueryEntity qe = new QueryEntity(Long.class.getName(), Integer.class.getName())
            .setTableName(TBL)
            .setKeyFieldName("id")
            .setValueFieldName("fld")
            .setFields(new LinkedHashMap<>(F.asMap("id", Long.class.getName(), "fld", Integer.class.getName())));

        CacheConfiguration<Long, Integer> ccfg = new CacheConfiguration<Long, Integer>()
            .setName(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(F.asList(qe))
            .setBackups(1);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled))
        );

        cfg.setCacheConfiguration(ccfg);

        cfg.setBuildIndexThreadPoolSize(1);

        return cfg;
    }

    /** */
    @Test
    public void testConcurrentCreateIndex() throws Exception {
        persistenceEnabled = true;

        Ignite crd = startGrids(3);

        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Long, Integer> cache = cache();

        insertData();

        CountDownLatch idxBuildGate = new CountDownLatch(1);

        grid(0).context().pools().buildIndexExecutorService().execute(() -> {
            try {
                idxBuildGate.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        multithreadedAsync(() -> {
            SqlFieldsQuery ddl = new SqlFieldsQuery("CREATE INDEX " + IDX + " ON " + TBL + "(fld)");
            cache.query(ddl).getAll();
        }, 1);

        IndexProcessor ip = grid(0).context().indexProcessor();

        IndexName name = new IndexName(cache.getName(), CACHE, TBL.toUpperCase(), IDX);

        boolean seenBuilding = GridTestUtils.waitForCondition(() -> {
            Index idx = ip.index(name);
            return idx != null && idx.rebuildInProgress();
        }, 10_000);

        assertTrue("Index must exist and be in rebuild state", seenBuilding);

        String sql = "SELECT id FROM " + TBL + " USE INDEX(" + IDX + ") " +
            "WHERE fld BETWEEN ? AND ? ORDER BY id";

        SqlFieldsQuery q = new SqlFieldsQuery(sql).setArgs(0, CNT - 1);

        GridTestUtils.assertThrows(null, () -> {
            cache.query(q).getAll();
        }, IgniteException.class, "Failed to parse query. Index \"" + IDX + "\" not found; SQL statement");

        idxBuildGate.countDown();

        crd.cache(CACHE).indexReadyFuture().get();

        boolean done = GridTestUtils.waitForCondition(() -> {
            Index idx = ip.index(name);
            return idx != null && !idx.rebuildInProgress();
        }, 20_000);

        assertTrue("Build must finish", done);

        assertEquals(CNT, cache.query(new SqlFieldsQuery(sql).setArgs(0, CNT - 1)).getAll().size());
    }

    /** */
    private void insertData() {
        try (IgniteDataStreamer<Long, Integer> streamer = grid(0).dataStreamer(CACHE)) {
            for (int i = 0; i < CNT; i++)
                streamer.addData((long)i, i);
        }
    }

    /** */
    private IgniteCache<Long, Integer> cache() throws Exception {
        Ignite n = "CRD".equals(qryNode) ? grid(0) : startClientGrid();
        return n.cache(CACHE);
    }

}
