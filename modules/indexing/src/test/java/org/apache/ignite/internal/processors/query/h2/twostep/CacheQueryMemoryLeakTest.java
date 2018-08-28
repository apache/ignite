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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class CacheQueryMemoryLeakTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)igniteCfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (igniteInstanceName.equals("client"))
            igniteCfg.setClientMode(true);

        return igniteCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Check, that query results are not accumulated, when result set size is a multiple of a {@link Query#pageSize}.
     *
     * @throws Exception If failed.
     */
    public void testResultIsMultipleOfPage() throws Exception {
        IgniteEx srv = (IgniteEx)startGrid("server");
        Ignite client = startGrid("client");

        IgniteCache<Integer, Person> cache = startPeopleCache(client);

        int pages = 3;
        int pageSize = 1024;

        for (int i = 0; i < pages * pageSize; i++) {
            Person p = new Person("Person #" + i, 25);

            cache.put(i, p);
        }

        for (int i = 0; i < 100; i++) {
            Query<List<?>> qry = new SqlFieldsQuery("select * from people");

            qry.setPageSize(pageSize);

            QueryCursor<List<?>> cursor = cache.query(qry);

            cursor.getAll();

            cursor.close();
        }

        assertTrue("MapNodeResults is not cleared on the map node.", isMapNodeResultsEmpty(srv));
    }

    /**
     * @param node Ignite node.
     * @return {@code True}, if all MapQueryResults are removed from internal node's structures. {@code False}
     * otherwise.
     */
    private boolean isMapNodeResultsEmpty(IgniteEx node) {
        IgniteH2Indexing idx = (IgniteH2Indexing)node.context().query().getIndexing();

        GridMapQueryExecutor mapQryExec = idx.mapQueryExecutor();

        Map<UUID, MapNodeResults> qryRess =
            GridTestUtils.getFieldValue(mapQryExec, GridMapQueryExecutor.class, "qryRess");

        for (MapNodeResults nodeRess : qryRess.values()) {
            Map<MapRequestKey, MapQueryResults> nodeQryRess =
                GridTestUtils.getFieldValue(nodeRess, MapNodeResults.class, "res");

            if (!nodeQryRess.isEmpty())
                return false;
        }

        return true;
    }

    /**
     * @param node Ignite instance.
     * @return Cache.
     */
    private static IgniteCache<Integer, Person> startPeopleCache(Ignite node) {
        CacheConfiguration<Integer, Person> cacheCfg = new CacheConfiguration<>("people");

        QueryEntity qe = new QueryEntity(Integer.class, Person.class);

        qe.setTableName("people");

        cacheCfg.setQueryEntities(Collections.singleton(qe));

        cacheCfg.setSqlSchema("PUBLIC");

        return node.getOrCreateCache(cacheCfg);
    }

    /** */
    @SuppressWarnings("unused")
    public static class Person {
        /** */
        @QuerySqlField
        private String name;

        /** */
        @QuerySqlField
        private int age;

        /**
         * @param name Name.
         * @param age Age.
         */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
