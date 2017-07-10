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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.*;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheFullTextQuerySelfTest extends GridCommonAbstractTest {
    /** */
    public static final int MAX_ITEM_COUNT = 100;

    /** */
    public static final String PERSON_CACHE = "Person";

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    public GridCacheFullTextQuerySelfTest() {
        super(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setIncludeEventTypes();
        cfg.setConnectorConfiguration(null);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(PERSON_CACHE)
                .setCacheMode(PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setBackups(0)
                .setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testTextQuery1() throws Exception {
        checkTextQuery(true, true);
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testTextQuery2() throws Exception {
        checkTextQuery(true, false);
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testTextQuery3() throws Exception {
        checkTextQuery(false, true);
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testTextQuery4() throws Exception {
        checkTextQuery(false, true);
    }

    /** */
    public void checkTextQuery(boolean loc, boolean keepBinary) throws Exception {
        final IgniteEx ignite = grid(0);

        // 1. Populate cache with data, calculating expected count in parallel.
        Set<Integer> exp = populateCache(ignite, loc, MAX_ITEM_COUNT, new IgnitePredicate<Integer>() {
            @Override
            public boolean apply(Integer x) {
                return String.valueOf(x).startsWith("1");
            }
        });

        // 2. Validate results.
        TextQuery qry = new TextQuery(Person.class, "1*").setLocal(loc);

        ValidateQueryResults(ignite, qry, exp, keepBinary);

        clearCache(ignite);
    }

    /** */
    private static void clearCache(IgniteEx ignite) {
        IgniteCache<Object, Object> cache = ignite.cache(PERSON_CACHE);

        cache.clear();

        List all = cache.query(new TextQuery(Person.class, "1*")).getAll();

        assertTrue(all.isEmpty());
    }

    /** */
    private static Set<Integer> populateCache(IgniteEx ignite, boolean loc, int cnt,
                                              IgnitePredicate<Integer> expectedEntryFilter) throws IgniteCheckedException {
        IgniteInternalCache<Integer, Person> cache = ignite.cachex(PERSON_CACHE);

        Random rand = new Random();

        HashSet<Integer> exp = new HashSet<>();

        Affinity<Integer> aff = cache.affinity();

        ClusterNode localNode = cache.context().localNode();

        for (int i = 0; i < cnt; i++) {
            int val = rand.nextInt(cnt);

            cache.put(val, new Person(String.valueOf(val), val));

            if (expectedEntryFilter.apply(val) && (!loc || aff.isPrimary(localNode, val)))
                exp.add(val);
        }

        return exp;
    }

    /** */
    private static void ValidateQueryResults(IgniteEx ignite, Query qry, Set<Integer> exp,
                                             boolean keepBinary) throws IgniteCheckedException {
        IgniteCache<Integer, Person> cache = ignite.cache(PERSON_CACHE);

        if (keepBinary) {
            IgniteCache<Integer, BinaryObject> cache0 = cache.withKeepBinary();

            try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = cache0.query(qry)) {
                Set<Integer> exp0 = new HashSet<>(exp);
                List<Cache.Entry<Integer, ?>> all = new ArrayList<>();

                for (Cache.Entry<Integer, BinaryObject> entry : cursor.getAll()) {
                    all.add(entry);

                    assertEquals(entry.getKey().toString(), entry.getValue().field("name"));
                    assertEquals(entry.getKey(), entry.getValue().field("age"));

                    exp0.remove(entry.getKey());
                }

                AssertMissingExpectedKeys(ignite, exp0, all);
            }

            try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = cache0.query(qry)) {
                Set<Integer> exp0 = new HashSet<>(exp);
                List<Cache.Entry<Integer, ?>> all = new ArrayList<>();

                for (Cache.Entry<Integer, BinaryObject> entry : cursor.getAll()) {
                    all.add(entry);

                    assertEquals(entry.getKey().toString(), entry.getValue().field("name"));
                    assertEquals(entry.getKey(), entry.getValue().field("age"));

                    exp0.remove(entry.getKey());
                }

                AssertMissingExpectedKeys(ignite, exp0, all);
            }
        } else {
            try (QueryCursor<Cache.Entry<Integer, Person>> cursor = cache.query(qry)) {
                Set<Integer> exp0 = new HashSet<>(exp);
                List<Cache.Entry<Integer, ?>> all = new ArrayList<>();

                for (Cache.Entry<Integer, Person> entry : cursor.getAll()) {
                    all.add(entry);

                    assertEquals(entry.getKey().toString(), entry.getValue().name);
                    assertEquals(entry.getKey(), Integer.valueOf(entry.getValue().age));

                    exp0.remove(entry.getKey());
                }

                AssertMissingExpectedKeys(ignite, exp0, all);
            }

            try (QueryCursor<Cache.Entry<Integer, Person>> cursor = cache.query(qry)) {
                Set<Integer> exp0 = new HashSet<>(exp);
                List<Cache.Entry<Integer, ?>> all = new ArrayList<>();

                for (Cache.Entry<Integer, Person> entry : cursor.getAll()) {
                    all.add(entry);

                    assertEquals(entry.getKey().toString(), entry.getValue().name);
                    assertEquals(entry.getKey().intValue(), entry.getValue().age);

                    exp0.remove(entry.getKey());
                }

                AssertMissingExpectedKeys(ignite, exp0, all);
            }
        }
    }

    /** */
    private static void AssertMissingExpectedKeys(IgniteEx ignite, Collection<Integer> exp,
                                                  List<Cache.Entry<Integer, ?>> all) throws IgniteCheckedException {
        if (exp.size() == 0)
            return;

        IgniteInternalCache<Integer, Person> cache = ignite.cachex(PERSON_CACHE);

        StringBuilder sb = new StringBuilder();
        Affinity<Integer> aff = cache.affinity();

        for (Integer key : exp) {
            Integer part = aff.partition(key);
            sb.append(String.format(
                    "Query did not return expected key '{0}' (exists: {1}), partition '{2}', partition nodes: ",
                    key, cache.get(key) != null, part));

            Collection<ClusterNode> partNodes = aff.mapPartitionToPrimaryAndBackups(part);

            for (ClusterNode node : partNodes)
                sb.append(node).append("  ");

            sb.append(";\n");
        }

        sb.append("Returned keys: ");

        for (Cache.Entry e : all)
            sb.append(e.getKey()).append(" ");

        sb.append(";\n");

        fail(sb.toString());
    }

    /**
     * Test value object.
     */
    private static class ObjectValue implements Serializable {
        /**
         * String value.
         */
        @QueryTextField
        private String strVal;

        /**
         * @param strVal String value.
         */
        ObjectValue(String strVal) {
            this.strVal = strVal;
        }

        /**
         * @return Value.
         */
        public String stringValue() {
            return strVal;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectValue other = (ObjectValue) o;

            return strVal == null ? other.strVal == null : strVal.equals(other.strVal);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return strVal != null ? strVal.hashCode() : 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return S.toString(ObjectValue.class, this);
        }
    }

    /**
     * Test value key.
     */
    private static class ObjectKey implements Serializable {
        /**
         * String key.
         */
        @QueryTextField
        private String strKey;

        /**
         * @param strKey String key.
         */
        ObjectKey(String strKey) {
            this.strKey = strKey;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectKey other = (ObjectKey) o;

            return strKey == null ? other.strKey == null : strKey.equals(other.strKey);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return strKey != null ? strKey.hashCode() : 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return S.toString(ObjectKey.class, this);
        }
    }

    /** */
    public static class Person implements Serializable {
        /** */
        @QueryTextField
        String name;

        /** */
        @QuerySqlField(index = true)
        int age;

        /** */
        @QuerySqlField
        final Date birthday;

        /**
         * Constructor
         */
        public Person(String name, int age) {
            this.name = name;
            this.age = age % 2000;

            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.YEAR, -age);

            birthday = cal.getTime();
        }
    }
}