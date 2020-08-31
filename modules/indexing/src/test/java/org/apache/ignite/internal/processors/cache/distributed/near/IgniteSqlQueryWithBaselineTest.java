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
package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.Serializable;
import java.util.Collection;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteSqlQueryWithBaselineTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setMaxSize(200 * 1024 * 1024)
                                        .setPersistenceEnabled(true)
                        )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public static class C1 implements Serializable {
        /** */
        private static final long serialVersionUID = 1L;

        /** */
        public C1(int id) {
            this.id = id;
        }

        /** */
        @QuerySqlField(index = true)
        protected Integer id;
    }

    /** */
    public static class C2 implements Serializable {
        /** */
        private static final long serialVersionUID = 1L;

        /** */
        C2(int id) {
            this.id = id;
        }

        /** */
        @QuerySqlField(index = true)
        protected Integer id;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithNodeNotInBLT() throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        startGrid(2); //Start extra node.

        doQuery();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryWithoutBLTNode() throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        startGrid(2); //Start extra node.
        stopGrid(1);

        doQuery();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryFromNotBLTNode() throws Exception {
        startGrid(1);

        grid(1).cluster().active(true);

        startGrid(0); //Start extra node.

        doQuery();
    }

    /**
     *
     */
    private void doQuery() {
        CacheConfiguration<Integer, C1> c1Conf = new CacheConfiguration<>("C1");
        c1Conf.setIndexedTypes(Integer.class, C1.class).setBackups(2);

        CacheConfiguration<Integer, C2> c2Conf = new CacheConfiguration<>("C2");
        c2Conf.setIndexedTypes(Integer.class, C2.class).setBackups(2);

        final IgniteCache<Integer, C1> cache = grid(0).getOrCreateCache(c1Conf);

        final IgniteCache<Integer, C2> cache1 = grid(0).getOrCreateCache(c2Conf);

        for (int i = 0; i < 100; i++) {
            cache.put(i, new C1(i));

            cache1.put(i, new C2(i));
        }

        String sql = "SELECT C1.*" +
                " from C1 inner join \"C2\".C2 as D on C1.id = D.id" +
                " order by C1.id asc";

        SqlQuery<Integer, C1> qry = new SqlQuery<>(C1.class, sql);

        qry.setDistributedJoins(true);

        log.info("before query run...");

        Collection<Cache.Entry<Integer, C1>> res = cache.query(qry).getAll();

        log.info("result size: " + res.size());
    }
}
