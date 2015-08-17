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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

/**
 * Tests for correct distributed partitioned queries.
 */
@SuppressWarnings("unchecked")
public class IgniteSqlSplitterSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfig(String name, boolean partitioned, Class<?>... idxTypes) {
        return new CacheConfiguration()
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setIndexedTypes(idxTypes);
    }

    /**
     * Tests offset and limit clauses for query.
     * @throws Exception If failed.
     */
    public void testOffsetLimit() throws Exception {
        IgniteCache<Integer, Integer> c = ignite(0).getOrCreateCache(cacheConfig("ints", true,
            Integer.class, Integer.class));

        try {
            List<Integer> res = new ArrayList<>();

            Random rnd = new GridRandom();

            for (int i = 0; i < 10; i++) {
                int val = rnd.nextInt(100);

                c.put(i, val);
                res.add(val);
            }

            Collections.sort(res);

            String qry = "select _val from Integer order by _val ";

            assertEqualsCollections(res,
                column(0, c.query(new SqlFieldsQuery(qry)).getAll()));

            assertEqualsCollections(res.subList(0, 0),
                column(0, c.query(new SqlFieldsQuery(qry + "limit ?").setArgs(0)).getAll()));

            assertEqualsCollections(res.subList(0, 3),
                column(0, c.query(new SqlFieldsQuery(qry + "limit ?").setArgs(3)).getAll()));

            assertEqualsCollections(res.subList(0, 9),
                column(0, c.query(new SqlFieldsQuery(qry + "limit ? offset ?").setArgs(9, 0)).getAll()));

            assertEqualsCollections(res.subList(3, 7),
                column(0, c.query(new SqlFieldsQuery(qry + "limit ? offset ?").setArgs(4, 3)).getAll()));

            assertEqualsCollections(res.subList(7, 9),
                column(0, c.query(new SqlFieldsQuery(qry + "limit ? offset ?").setArgs(2, 7)).getAll()));

            assertEqualsCollections(res.subList(8, 10),
                column(0, c.query(new SqlFieldsQuery(qry + "limit ? offset ?").setArgs(2, 8)).getAll()));

            assertEqualsCollections(res.subList(9, 10),
                column(0, c.query(new SqlFieldsQuery(qry + "limit ? offset ?").setArgs(1, 9)).getAll()));

            assertEqualsCollections(res.subList(10, 10),
                column(0, c.query(new SqlFieldsQuery(qry + "limit ? offset ?").setArgs(1, 10)).getAll()));

            assertEqualsCollections(res.subList(9, 10),
                column(0, c.query(new SqlFieldsQuery(qry + "limit ? offset abs(-(4 + ?))").setArgs(1, 5)).getAll()));
        }
        finally {
            c.destroy();
        }
    }

    /**
     * @param idx Column index.
     * @param rows Rows.
     * @return Column as list.
     */
    private static List<?> column(int idx, List<List<?>> rows) {
        List res = new ArrayList<>(rows.size());

        for (List<?> row : rows)
            res.add(row.get(idx));

        return res;
    }
}
