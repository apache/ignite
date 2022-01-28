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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Collections.reverseOrder;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;
import static java.util.Comparator.nullsLast;

/**
 * Query parallelism sorting tests.
 */
public class IgniteCacheParallelismQuerySortOrderTest extends GridCommonAbstractTest {

    /** Test data */
    private static final Map<Long, Person> CACHE_DATA = new HashMap<>();

    /** Sorted ID by city */
    private static final List<Long> CITY_SORTED_ASC;

    /** Sorted ID by city DESC */
    private static final List<Long> CITY_SORTED_DESC;

    /** ID where city is null */
    private static final List<Long> NULLS_ID = Arrays.asList(3L, 5L, 6L);

    /**
     * data initialization block
     */
    static {
        CACHE_DATA.put(1L, new Person(1L, "Andrew", "London"));
        CACHE_DATA.put(2L, new Person(2L, "John", "Amsterdam"));
        CACHE_DATA.put(3L, new Person(3L, "Tom", null));
        CACHE_DATA.put(4L, new Person(4L, "Ben", "Washington"));
        CACHE_DATA.put(5L, new Person(5L, "Stan", null));
        CACHE_DATA.put(6L, new Person(6L, "Leonard", null));
        CACHE_DATA.put(7L, new Person(7L, "Richard", "Ryazan"));
        CACHE_DATA.put(8L, new Person(8L, "Tom", "Paris"));
        CACHE_DATA.put(9L, new Person(9L, "Andrew", "Moscow"));

        CITY_SORTED_ASC = CACHE_DATA.values().stream()
            .filter(person -> person.city() != null)
            .sorted(Comparator.comparing(Person::city))
            .map(Person::id)
            .collect(Collectors.toList());

        CITY_SORTED_DESC = new ArrayList<>(CITY_SORTED_ASC);
        Collections.reverse(CITY_SORTED_DESC);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        startGrids(1);
        IgniteCache<Long, Person> cache = jcache(Long.class, Person.class);
        cache.putAll(CACHE_DATA);
    }

    /**
     * @param clsK Key class.
     * @param clsV Value class.
     * @return cache instance
     */
    protected <K, V> IgniteCache<K, V> jcache(Class<K> clsK, Class<V> clsV) {
        return jcache(grid(0), cacheConfiguration(), clsK, clsV);
    }

    /**
     * @return cache configuration
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();
        cc.setQueryParallelism(4);
        return cc;
    }

    /**
     * ASC NULLS LAST sorting test
     */
    @Test
    public void testAscNullsLast() {
        IgniteCache<Long, Person> cache = jcache(Long.class, Person.class);

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM Person ORDER BY city ASC NULLS LAST")).getAll();

        List<Long> resList = res.stream().map(val -> (Long)val.get(0)).collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEqualCollection(NULLS_ID, resList.subList(resList.size() - NULLS_ID.size(), resList.size())));
        Assert.assertEquals(CITY_SORTED_ASC, resList.subList(0, resList.size() - NULLS_ID.size()));
    }

    /**
     * DESC NULLS LAST sorting test
     */
    @Test
    public void testDescNullsLast() {
        IgniteCache<Long, Person> cache = jcache(Long.class, Person.class);

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM Person ORDER BY city DESC NULLS LAST")).getAll();

        List<Long> resList = res.stream().map(val -> (Long)val.get(0)).collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEqualCollection(NULLS_ID, resList.subList(resList.size() - NULLS_ID.size(), resList.size())));
        Assert.assertEquals(CITY_SORTED_DESC, resList.subList(0, resList.size() - NULLS_ID.size()));
    }

    /**
     * ASC NULLS FIRST sorting test
     */
    @Test
    public void testAscNullsFirst() {
        IgniteCache<Long, Person> cache = jcache(Long.class, Person.class);

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM Person ORDER BY city ASC NULLS FIRST")).getAll();

        List<Long> resList = res.stream().map(val -> (Long)val.get(0)).collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEqualCollection(NULLS_ID, resList.subList(0, NULLS_ID.size())));
        Assert.assertEquals(CITY_SORTED_ASC, resList.subList(NULLS_ID.size(), resList.size()));
    }

    /**
     * DESC NULLS FIRST sorting test
     */
    @Test
    public void testDescNullsFirst() {
        IgniteCache<Long, Person> cache = jcache(Long.class, Person.class);

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM Person ORDER BY city DESC NULLS FIRST")).getAll();

        List<Long> resList = res.stream().map(val -> (Long)val.get(0)).collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEqualCollection(NULLS_ID, resList.subList(0, NULLS_ID.size())));
        Assert.assertEquals(CITY_SORTED_DESC, resList.subList(NULLS_ID.size(), resList.size()));
    }

    /**
     * Sorting by two fields (ASC, DESC NULLS FIRST)
     */
    @Test
    public void fewFieldAscNullsFirstSortTest() {
        IgniteCache<Long, Person> cache = jcache(Long.class, Person.class);

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM Person ORDER BY name, city DESC NULLS FIRST")).getAll();

        List<Long> resList = res.stream().map(val -> (Long)val.get(0)).collect(Collectors.toList());
        List<Long> sorted = CACHE_DATA.values().stream()
            .sorted(Comparator.comparing(Person::name).thenComparing(Person::city, nullsFirst(reverseOrder())))
            .map(Person::id)
            .collect(Collectors.toList());
        Assert.assertEquals(sorted, resList);

    }

    /**
     * Sorting by two fields (ASC NULLS LAST, DESC)
     */
    @Test
    public void fewFieldsNullsLastDescSortTest() {
        IgniteCache<Long, Person> cache = jcache(Long.class, Person.class);

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM Person ORDER BY city ASC NULLS LAST, name DESC")).getAll();

        List<Long> resList = res.stream().map(val -> (Long)val.get(0)).collect(Collectors.toList());
        List<Long> sorted = CACHE_DATA.values().stream()
            .sorted(Comparator.comparing(Person::city, nullsLast(naturalOrder())).thenComparing(Person::name, reverseOrder()))
            .map(Person::id)
            .collect(Collectors.toList());
        Assert.assertEquals(sorted, resList);
    }

    /**
     *
     */
    static class Person {

        /** Id. */
        @QuerySqlField(index = true)
        private final Long id;

        /** Name. */
        @QuerySqlField
        private final String name;

        /** City. */
        @QuerySqlField
        private final String city;

        /**
         * Constructor.
         *
         * @param id   Long value.
         * @param name String value.
         * @param city String value.
         */
        Person(Long id, String name, String city) {
            this.id = id;
            this.name = name;
            this.city = city;
        }

        /**
         * Gets id.
         *
         * @return id.
         */
        public Long id() {
            return id;
        }

        /**
         * Gets name.
         *
         * @return name.
         */
        public String name() {
            return name;
        }

        /**
         * Gets city.
         *
         * @return city.
         */
        public String city() {
            return city;
        }
    }
}
