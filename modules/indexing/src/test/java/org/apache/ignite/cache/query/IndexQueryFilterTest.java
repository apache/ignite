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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
@RunWith(Parameterized.class)
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
    @Parameterized.Parameter
    public String idxName;

    /** */
    @Parameterized.Parameters(name = "idxName={0}")
    public static List<String> params() {
        return F.asList(null, IDX);
    }

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

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter(nameFilter);

        check(qry, nameFilter);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", 18))
            .setFilter(nameFilter);

        check(qry, (k, v) -> v.age < 18 && nameFilter.apply(k, v));

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setFilter(nameFilter);

        check(qry, nameFilter);
    }

    /** */
    @Test
    public void testIndexedFieldFilter() {
        IgniteBiPredicate<Integer, Person> ageFilter = (k, v) -> v.age > 18;

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter(ageFilter);

        check(qry, ageFilter);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", 18))
            .setFilter(ageFilter);

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setFilter(ageFilter);

        check(qry, ageFilter);
    }

    /** */
    @Test
    public void testKeyFilter() {
        IgniteBiPredicate<Integer, Person> keyFilter = (k, v) -> k > CNT / 2;

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter(keyFilter);

        check(qry, keyFilter);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", 18))
            .setFilter(keyFilter);

        check(qry, (k, v) -> v.age < 18 && keyFilter.apply(k, v));

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setFilter(keyFilter);

        check(qry, keyFilter);
    }

    /** */
    @Test
    public void testValueFilter() {
        IgniteBiPredicate<Integer, Person> valFilter = (k, v) ->
            v.equals(persons.values().stream().findFirst().orElse(null));

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter(valFilter);

        check(qry, valFilter);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setFilter(valFilter);

        check(qry, valFilter);
    }

    /** */
    @Test
    public void testAllowOrDisallowAll() {
        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter((k, v) -> true);

        assertEquals(CNT, cache.query(qry).getAll().size());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", 18))
            .setFilter((k, v) -> true);

        check(qry, (k, v) -> v.age < 18);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt("age", MAX_AGE))
            .setFilter((k, v) -> false);

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setFilter((k, v) -> false);

        assertTrue(cache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testFilterException() {
        IgniteBiPredicate<Integer, Person> nameFilter = (k, v) -> {
            throw new RuntimeException();
        };

        GridTestUtils.assertThrows(null, () -> {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
                .setCriteria(lt("age", MAX_AGE))
                .setFilter(nameFilter);

            cache.query(qry).getAll();

            return null;
        }, CacheException.class, "Failed to execute query on node");
    }

    /** */
    private void check(IndexQuery<Integer, Person> qry, IgniteBiPredicate<Integer, Person> filter) {
        boolean pk = qry.getIndexName() == null && F.isEmpty(qry.getCriteria());

        TreeMap<Integer, Set<Person>> expected = pk ? pkPersons(filter) : ageIndexedPersons(filter);

        List<Cache.Entry<Integer, Person>> all = cache.query(qry).getAll();

        for (int i = 0; i < all.size(); i++) {
            Map.Entry<Integer, Set<Person>> exp = expected.firstEntry();

            Cache.Entry<Integer, Person> entry = all.get(i);

            assertTrue(exp.getValue().remove(entry.getValue()));

            if (exp.getValue().isEmpty())
                expected.remove(exp.getKey());
        }

        assertTrue(expected.isEmpty());
    }

    /** */
    private TreeMap<Integer, Set<Person>> ageIndexedPersons(IgniteBiPredicate<Integer, Person> filter) {
        return persons.entrySet().stream()
            .filter(e -> filter.apply(e.getKey(), e.getValue()))
            .collect(TreeMap::new, (m, e) -> {
                int age = e.getValue().age;

                m.computeIfAbsent(age, a -> new HashSet<>());

                m.get(age).add(e.getValue());

            }, (l, r) -> {
                r.forEach((k, v) -> {
                    int age = ((Person)v).age;

                    l.computeIfAbsent(age, a -> new HashSet<>());

                    l.get(age).add((Person)v);
                });
            });
    }

    /** */
    private TreeMap<Integer, Set<Person>> pkPersons(IgniteBiPredicate<Integer, Person> filter) {
        return persons.entrySet().stream()
            .filter(e -> filter.apply(e.getKey(), e.getValue()))
            .collect(
                TreeMap::new,
                (m, e) -> m.put(e.getKey(), new HashSet<>(Collections.singleton(e.getValue()))),
                TreeMap::putAll
            );
    }

    /** */
    private static class Person {
        /** */
        @GridToStringInclude
        final int id;

        /** */
        @GridToStringInclude
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = IDX, order = 0))
        final int age;

        /** */
        @GridToStringInclude
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

            Person person = (Person)o;

            return Objects.equals(id, person.id) && Objects.equals(age, person.age) && Objects.equals(name, person.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, age, name);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
