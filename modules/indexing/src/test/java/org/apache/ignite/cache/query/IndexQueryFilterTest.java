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

package org.apache.ignite.cache.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
public class IndexQueryFilterTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String IDX = "IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static final int MAX_AGE = 100;

    /** */
    private static IgniteCache<Integer, Person> cache;

    /** */
    private static final Map<Integer, Person> persons = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite crd = startGrids(2);

        cache = crd.cache(CACHE);

        Random r = new Random();

        for (int i = 0; i < CNT; i++) {
            Person p = new Person(i, r.nextInt(MAX_AGE), "name_" + i);

            persons.put(i, p);
            cache.put(i, p);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg1 = new CacheConfiguration<>()
            .setName(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(ccfg1);

        return cfg;
    }

    /** */
    @Test
    public void testNonIndexedFieldFilter() {
        IgniteBiPredicate<Integer, Person> nameFilter = (k, v) -> v.name.contains("0");

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter(nameFilter);

        check(cache.query(qry), nameFilter);

        qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", 18))
            .setFilter(nameFilter);

        check(cache.query(qry), (k, v) -> v.age < 18 && nameFilter.apply(k, v));
    }

    /** */
    @Test
    public void testIndexedFieldFilter() {
        IgniteBiPredicate<Integer, Person> ageFilter = (k, v) -> v.age > 18;

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter(ageFilter);

        check(cache.query(qry), ageFilter);

        qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", 18))
            .setFilter(ageFilter);

        assertTrue(cache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testKeyFilter() {
        IgniteBiPredicate<Integer, Person> keyFilter = (k, v) -> k > CNT / 2;

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter(keyFilter);

        check(cache.query(qry), keyFilter);

        qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", 18))
            .setFilter(keyFilter);

        check(cache.query(qry), (k, v) -> v.age < 18 && keyFilter.apply(k, v));
    }

    /** */
    @Test
    public void testValueFilter() {
        IgniteBiPredicate<Integer, Person> valFilter = (k, v) ->
            v.equals(persons.values().stream().findFirst().orElse(null));

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter(valFilter);

        check(cache.query(qry), valFilter);
    }

    /** */
    @Test
    public void testAllowOrDisallowAll() {
        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter((k, v) -> true);

        assertEquals(CNT, cache.query(qry).getAll().size());

        qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", 18))
            .setFilter((k, v) -> true);

        check(cache.query(qry), (k, v) -> v.age < 18);

        qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter((k, v) -> false);

        assertTrue(cache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testFilterException() {
        IgniteBiPredicate<Integer, Person> nameFilter = (k, v) -> {
            throw new RuntimeException();
        };

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter(nameFilter);

        GridTestUtils.assertThrows(null, () -> {
            cache.query(qry).getAll();

            return null;
        }, CacheException.class, "Failed to execute query on node");
    }

    /** */
    private void check(QueryCursor<Cache.Entry<Integer, Person>> cursor, IgniteBiPredicate<Integer, Person> filter) {
        Map<Integer, Person> expected = persons.entrySet().stream()
            .filter(e -> filter.apply(e.getKey(), e.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<Cache.Entry<Integer, Person>> all = cursor.getAll();

        assertEquals(expected.size(), all.size());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Integer, Person> entry = all.get(i);

            Person p = expected.remove(entry.getKey());

            assertNotNull(p);

            assertEquals(p, entry.getValue());
        }

        assertTrue(expected.isEmpty());
    }

    /** */
    private static class Person {
        /** */
        final int id;

        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = IDX, order = 0))
        final int age;

        /** */
        final String name;

        /** */
        Person(int id, int age, String name) {
            this.id = id;
            this.age = age;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person) o;

            return Objects.equals(id, person.id) && Objects.equals(age, person.age) && Objects.equals(name, person.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, age, name);
        }
    }
}
