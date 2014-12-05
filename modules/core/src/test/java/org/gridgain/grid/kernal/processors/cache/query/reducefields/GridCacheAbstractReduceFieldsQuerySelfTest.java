/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.reducefields;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

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
        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * @return Distribution.
     */
    protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /**
     * @param name Cache name.
     * @return Cache.
     */
    private GridCacheConfiguration cache(@Nullable String name) {
        GridCacheConfiguration cache = defaultCacheConfiguration();

        cache.setName(name);
        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setDistributionMode(distributionMode());
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setPreloadMode(SYNC);

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

        GridCache<String, Organization> orgCache = grid(0).cache(null);

        assert orgCache != null;

        assert orgCache.putx("o1", new Organization(1, "A"));
        assert orgCache.putx("o2", new Organization(2, "B"));

        GridCache<GridCacheAffinityKey<String>, Person> personCache = grid(0).cache(null);

        assert personCache != null;

        assert personCache.putx(new GridCacheAffinityKey<>("p1", "o1"), new Person("John White", 25, 1));
        assert personCache.putx(new GridCacheAffinityKey<>("p2", "o1"), new Person("Joe Black", 35, 1));
        assert personCache.putx(new GridCacheAffinityKey<>("p3", "o2"), new Person("Mike Green", 40, 2));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @return cache mode.
     */
    protected abstract GridCacheMode cacheMode();

    /**
     * @return Number of grids to start.
     */
    protected abstract int gridCount();

    /**
     * @return Cache atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoDataInCache() throws Exception {
        GridCacheQuery<List<?>> qry = grid(0)
            .cache(null).queries().createSqlFieldsQuery("select age from Person where orgId = 999");

        Collection<IgniteBiTuple<Integer, Integer>> res = qry.execute(new AverageRemoteReducer()).get();

        assertEquals("Result", 0, F.reduce(res, new AverageLocalReducer()).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAverageQuery() throws Exception {
        GridCacheQuery<List<?>> qry = grid(0).cache(null).queries().createSqlFieldsQuery("select age from Person");

        Collection<IgniteBiTuple<Integer, Integer>> res = qry.execute(new AverageRemoteReducer()).get();

        assertEquals("Average", 33, F.reduce(res, new AverageLocalReducer()).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAverageQueryWithArguments() throws Exception {
        GridCacheQuery<List<?>> qry = grid(0).cache(null).queries().createSqlFieldsQuery(
            "select age from Person where orgId = ?");

        Collection<IgniteBiTuple<Integer, Integer>> res = qry.execute(new AverageRemoteReducer(), 1).get();

        assertEquals("Average", 30, F.reduce(res, new AverageLocalReducer()).intValue());
    }

//    /**
//     * @throws Exception If failed.
//     */
//    public void testFilters() throws Exception {
//        GridCacheReduceFieldsQuery<Object, Object, GridBiTuple<Integer, Integer>, Integer> qry = grid(0).cache(null)
//            .queries().createReduceFieldsQuery("select age from Person");
//
//        qry = qry.remoteKeyFilter(
//            new GridPredicate<Object>() {
//                @Override public boolean apply(Object e) {
//                    return !"p2".equals(((GridCacheAffinityKey)e).key());
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

    /**
     * @throws Exception If failed.
     */
    public void testOnProjection() throws Exception {
        P2<GridCacheAffinityKey<String>, Person> p = new P2<GridCacheAffinityKey<String>, Person>() {
            @Override public boolean apply(GridCacheAffinityKey<String> key, Person val) {
                return val.orgId == 1;
            }
        };

        GridCacheProjection<GridCacheAffinityKey<String>, Person> cachePrj =
            grid(0).<GridCacheAffinityKey<String>, Person>cache(null).projection(p);

        GridCacheQuery<List<?>> qry = cachePrj.queries().createSqlFieldsQuery("select age from Person");

        Collection<IgniteBiTuple<Integer, Integer>> res = qry.execute(new AverageRemoteReducer()).get();

        assertEquals("Average", 30, F.reduce(res, new AverageLocalReducer()).intValue());
    }

//    /**
//     * @throws Exception If failed.
//     */
//    public void testOnProjectionWithFilter() throws Exception {
//        P2<GridCacheAffinityKey<String>, Person> p = new P2<GridCacheAffinityKey<String>, Person>() {
//            @Override public boolean apply(GridCacheAffinityKey<String> key, Person val) {
//                return val.orgId == 1;
//            }
//        };
//
//        GridCacheProjection<GridCacheAffinityKey<String>, Person> cachePrj =
//            grid(0).<GridCacheAffinityKey<String>, Person>cache(null).projection(p);
//
//        GridCacheReduceFieldsQuery<GridCacheAffinityKey<String>, Person, GridBiTuple<Integer, Integer>, Integer> qry =
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
        @GridCacheQuerySqlField(index = false)
        private final String name;

        /** Age. */
        @GridCacheQuerySqlField(index = true)
        private final int age;

        /** Organization ID. */
        @GridCacheQuerySqlField(index = true)
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
        @GridCacheQuerySqlField
        private final int id;

        /** Name. */
        @GridCacheQuerySqlField(index = false)
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

        @Override public boolean collect(List<?> e) {
            sum += (Integer)e.get(0);

            cnt++;

            return true;
        }

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

        @Override public boolean collect(IgniteBiTuple<Integer, Integer> t) {
            sum += t.get1();
            cnt += t.get2();

            return true;
        }

        @Override public Integer reduce() {
            return cnt == 0 ? 0 : sum / cnt;
        }
    }
}
