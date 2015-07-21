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

package org.apache.ignite.internal.processors.cache.reducefields;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Tests for reduce fields queries.
 */
public abstract class GridCacheAbstractReduceFieldsQuerySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Flag indicating if starting node should have cache. */
    protected boolean hasCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (hasCache)
            cfg.setCacheConfiguration(cache(null));
        else
            cfg.setCacheConfiguration();

        cfg.setDiscoverySpi(discovery());

        return cfg;
    }

    /**
     * @return Distribution.
     */
    protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /**
     * @param name Cache name.
     * @return Cache.
     */
    private CacheConfiguration cache(@Nullable String name) {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setName(name);
        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setNearConfiguration(nearConfiguration());
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setRebalanceMode(SYNC);
        cache.setIndexedTypes(
            String.class, Organization.class,
            AffinityKey.class, Person.class
        );

        if (cacheMode() == PARTITIONED)
            cache.setBackups(1);

        return cache;
    }

    /**
     * @return Discovery SPI.
     */
    private static DiscoverySpi discovery() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        hasCache = true;

        startGridsMultiThreaded(gridCount());

        hasCache = false;

        startGrid(gridCount());

        IgniteCache<String, Organization> orgCache = grid(0).cache(null);

        assert orgCache != null;

        orgCache.put("o1", new Organization(1, "A"));
        orgCache.put("o2", new Organization(2, "B"));

        IgniteCache<AffinityKey<String>, Person> personCache = grid(0).cache(null);

        assert personCache != null;

        personCache.put(new AffinityKey<>("p1", "o1"), new Person("John White", 25, 1));
        personCache.put(new AffinityKey<>("p2", "o1"), new Person("Joe Black", 35, 1));
        personCache.put(new AffinityKey<>("p3", "o2"), new Person("Mike Green", 40, 2));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @return cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Number of grids to start.
     */
    protected abstract int gridCount();

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoDataInCache() throws Exception {
        CacheQuery<List<?>> qry = ((IgniteKernal)grid(0))
            .getCache(null).context().queries().createSqlFieldsQuery("select age from Person where orgId = 999", false);

        Collection<IgniteBiTuple<Integer, Integer>> res = qry.execute(new AverageRemoteReducer()).get();

        assertEquals("Result", 0, F.reduce(res, new AverageLocalReducer()).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAverageQuery() throws Exception {
        CacheQuery<List<?>> qry = ((IgniteKernal)grid(0)).getCache(null).context().queries().
            createSqlFieldsQuery("select age from Person", false);

        Collection<IgniteBiTuple<Integer, Integer>> res = qry.execute(new AverageRemoteReducer()).get();

        assertEquals("Average", 33, F.reduce(res, new AverageLocalReducer()).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAverageQueryWithArguments() throws Exception {
        CacheQuery<List<?>> qry = ((IgniteKernal)grid(0)).getCache(null).context().queries().createSqlFieldsQuery(
            "select age from Person where orgId = ?", false);

        Collection<IgniteBiTuple<Integer, Integer>> res = qry.execute(new AverageRemoteReducer(), 1).get();

        assertEquals("Average", 30, F.reduce(res, new AverageLocalReducer()).intValue());
    }

//    /**
//     * @throws Exception If failed.
//     */
//    public void testFilters() throws Exception {
//        GridCacheReduceFieldsQuery<Object, Object, GridBiTuple<Integer, Integer>, Integer> qry = ((IgniteKernal)grid(0)).cache(null)
//            .queries().createReduceFieldsQuery("select age from Person");
//
//        qry = qry.remoteKeyFilter(
//            new GridPredicate<Object>() {
//                @Override public boolean apply(Object e) {
//                    return !"p2".equals(((AffinityKey)e).key());
//                }
//            }
//        ).remoteValueFilter(
//            new P1<Object>() {
//                @Override public boolean apply(Object e) {
//                    return !"Mike Green".equals(((Person)e).name);
//                }
//            }
//        );
//
//        qry = qry.remoteReducer(new AverageRemoteReducer()).localReducer(new AverageLocalReducer());
//
//        Integer avg = qry.reduce().get();
//
//        assertNotNull("Average", avg);
//        assertEquals("Average", 25, avg.intValue());
//    }

//    /**
//     * @throws Exception If failed.
//     */
//    public void testOnProjectionWithFilter() throws Exception {
//        P2<AffinityKey<String>, Person> p = new P2<AffinityKey<String>, Person>() {
//            @Override public boolean apply(AffinityKey<String> key, Person val) {
//                return val.orgId == 1;
//            }
//        };
//
//        InternalCache<AffinityKey<String>, Person> cachePrj =
//            grid(0).<AffinityKey<String>, Person>cache(null).projection(p);
//
//        GridCacheReduceFieldsQuery<AffinityKey<String>, Person, GridBiTuple<Integer, Integer>, Integer> qry =
//            cachePrj.queries().createReduceFieldsQuery("select age from Person");
//
//        qry = qry.remoteValueFilter(
//            new P1<Person>() {
//                @Override public boolean apply(Person e) {
//                    return !"Joe Black".equals(e.name);
//                }
//            });
//
//        qry = qry.remoteReducer(new AverageRemoteReducer()).localReducer(new AverageLocalReducer());
//
//        Integer avg = qry.reduce().get();
//
//        assertNotNull("Average", avg);
//        assertEquals("Average", 25, avg.intValue());
//    }

    /**
     * @return true if cache mode is replicated, false otherwise.
     */
    private boolean isReplicatedMode() {
        return cacheMode() == REPLICATED;
    }

    /**
     * Person.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Person implements Serializable {
        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /** Age. */
        @QuerySqlField(index = true)
        private final int age;

        /** Organization ID. */
        @QuerySqlField(index = true)
        private final int orgId;

        /**
         * @param name Name.
         * @param age Age.
         * @param orgId Organization ID.
         */
        private Person(String name, int age, int orgId) {
            assert !F.isEmpty(name);
            assert age > 0;
            assert orgId > 0;

            this.name = name;
            this.age = age;
            this.orgId = orgId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return age == person.age && orgId == person.orgId && name.equals(person.name);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = name.hashCode();

            res = 31 * res + age;
            res = 31 * res + orgId;

            return res;
        }
    }

    /**
     * Organization.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Organization implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        private Organization(int id, String name) {
            assert id > 0;
            assert !F.isEmpty(name);

            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Organization that = (Organization)o;

            return id == that.id && name.equals(that.name);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = id;

            res = 31 * res + name.hashCode();

            return res;
        }
    }

    /**
     * Average remote reducer factory.
     */
    protected static class AverageRemoteReducer implements IgniteReducer<List<?>, IgniteBiTuple<Integer, Integer>> {
        /** */
        private int sum;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public boolean collect(List<?> e) {
            sum += (Integer)e.get(0);

            cnt++;

            return true;
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<Integer, Integer> reduce() {
            return F.t(sum, cnt);
        }
    }

    /**
     * Average local reducer factory.
     */
    protected static class AverageLocalReducer implements IgniteReducer<IgniteBiTuple<Integer, Integer>, Integer> {
        /** */
        private int sum;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public boolean collect(IgniteBiTuple<Integer, Integer> t) {
            sum += t.get1();
            cnt += t.get2();

            return true;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce() {
            return cnt == 0 ? 0 : sum / cnt;
        }
    }
}
