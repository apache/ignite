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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for GROUP_CONCAT aggregate function in collocated mode.
 */
@SuppressWarnings("unchecked")
public class IgniteSqlGroupConcatCollocatedTest extends GridCommonAbstractTest {
    /** */
    private static final int CLIENT = 7;

    /** */
    private static final int KEY_BASE_FOR_DUPLICATES = 100;

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(
            new CacheConfiguration(CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(8))
                .setQueryEntities(Collections.singletonList(new QueryEntity(Key.class, Value.class))))
            .setCacheKeyConfiguration(new CacheKeyConfiguration(Key.class));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3, false);

        Ignition.setClientMode(true);
        try {
            startGrid(CLIENT);
        }
        finally {
            Ignition.setClientMode(false);
        }

        IgniteCache c = grid(CLIENT).cache(CACHE_NAME);

        int k = 0;

        for (int grp = 1; grp < 7; ++grp) {
            for (int i = 0; i < grp; ++i) {
                c.put(new Key(k, grp), new Value(k, Character.toString((char)('A' + k))));

                k++;
            }
        }

        // Add duplicates
        k = 0;
        for (int grp = 1; grp < 7; ++grp) {
            for (int i = 0; i < grp; ++i) {
                c.put(new Key(k + KEY_BASE_FOR_DUPLICATES, grp),
                    new Value(k + KEY_BASE_FOR_DUPLICATES, Character.toString((char)('A' + k))));

                k++;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    public void testGroupConcatSimple() {
        IgniteCache c = ignite(CLIENT).cache(CACHE_NAME);

        List<List<Object>> res = c.query(
            new SqlFieldsQuery("select grp, GROUP_CONCAT(str0) from Value WHERE id < ? group by grp ")
                .setCollocated(true).setArgs(KEY_BASE_FOR_DUPLICATES)).getAll();

        for (List<Object> row : res) {
            int grp = (int)row.get(0);

            String str = (String)row.get(1);

            for (int i = 0; i < grp; ++i) {
                String s = "" + (char)('A' + i + (grp - 1) * grp / 2);

                assertTrue("Invalid group_concat result: string doesn't contain value: " +
                    "[str=" + str + ", val=" + s , str.contains(s));
            }
        }
    }

    /**
     *
     */
    public void testGroupConcatOrderBy() {
        IgniteCache c = ignite(CLIENT).cache(CACHE_NAME);

        HashMap<Integer, String> exp = new HashMap<>();

        exp.put(1, "id#0=A");
        exp.put(2, "id#1=B; id#2=C");
        exp.put(3, "id#3=D; id#4=E; id#5=F");
        exp.put(4, "id#6=G; id#7=H; id#8=I; id#9=J");
        exp.put(5, "id#10=K; id#11=L; id#12=M; id#13=N; id#14=O");
        exp.put(6, "id#15=P; id#16=Q; id#17=R; id#18=S; id#19=T; id#20=U");

        List<List<Object>> res = c.query(
            new SqlFieldsQuery("select grp, GROUP_CONCAT(strId || '=' || str0 ORDER BY id SEPARATOR '; ')" +
                " from Value WHERE id < ? group by grp")
                .setCollocated(true).setArgs(KEY_BASE_FOR_DUPLICATES)).getAll();

        HashMap<Integer, String> resMap = resultMap(res);

        assertEquals(exp, resMap);
    }

    /**
     *
     */
    public void testGroupConcatWithDistinct() {
        IgniteCache c = ignite(CLIENT).cache(CACHE_NAME);

        HashMap<Integer, String> exp = new HashMap<>();

        exp.put(1, "A");
        exp.put(2, "B,C");
        exp.put(3, "D,E,F");
        exp.put(4, "G,H,I,J");
        exp.put(5, "K,L,M,N,O");
        exp.put(6, "P,Q,R,S,T,U");

        List<List<Object>> res = c.query(
            new SqlFieldsQuery("select grp, GROUP_CONCAT(DISTINCT str0 ORDER BY str0) from Value group by grp")
                .setCollocated(true)).getAll();

        HashMap<Integer, String> resMap = resultMap(res);

        assertEquals(exp, resMap);
    }

    /**
     * @param res Result collection.
     * @return Map ro result compare.
     */
    private HashMap<Integer, String> resultMap(List<List<Object>> res) {
        HashMap<Integer, String> map = new HashMap<>();

        for (List<Object> row : res)
            map.put((Integer)row.get(0), (String)row.get(1));

        return  map;
    }

    /**
     *
     */
    public static class Key {
        /** */
        @QuerySqlField
        private int id;

        /** */
        @QuerySqlField
        @AffinityKeyMapped
        private int grp;

        /**
         * @param id Id.
         * @param grp Group.
         */
        public Key(int id, int grp) {
            this.id = id;
            this.grp = grp;
        }
    }

    /**
     *
     */
    public static class Value {
        /** */
        @QuerySqlField
        private String str0;

        /** */
        @QuerySqlField
        private String str1;

        /** */
        @QuerySqlField
        private String strId;

        /**
         * @param id Id.
         * @param str String value.
         */
        public Value(int id, String str) {
            str0 = str;
            str1 = str + "_1";
            strId = "id#" + id;
        }
    }
}
