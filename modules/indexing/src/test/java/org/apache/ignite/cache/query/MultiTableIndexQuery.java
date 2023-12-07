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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gte;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

/** */
@RunWith(Parameterized.class)
public class MultiTableIndexQuery extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String IDX = "PERSON_ID_IDX";

    /** */
    private static final String SEC_IDX = "SECONDPERSON_ID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static IgniteCache<Long, Object> cache;

    /** */
    @Parameterized.Parameter(value = 0)
    public String qryPersIdx;

    /** */
    @Parameterized.Parameter(value = 1)
    public String qrySecPersIdx;

    /** */
    @Parameterized.Parameter(value = 2)
    public String qryKeyPkIdx;

    /** */
    @Parameterized.Parameters(name = "IDX={0}, SEC_IDX={1}, PK={2}")
    public static Collection<Object[]> params() {
        return F.asList(
            new Object[] {null, null, null},
            new Object[] {IDX, SEC_IDX, "_key_PK"});
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite crd = startGrids(4);

        cache = crd.cache(CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        cache.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>()
            .setName(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Long.class, Person.class, Long.class, SecondPerson.class)
            .setQueryParallelism(4);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void testEmptyCache() {
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryPersIdx)
            .setCriteria(lt("id", Integer.MAX_VALUE));

        QueryCursor<Cache.Entry<Long, Person>> cursor = cache.query(qry);

        assertTrue(cursor.getAll().isEmpty());

        IndexQuery<Long, SecondPerson> secQry = new IndexQuery<Long, SecondPerson>(SecondPerson.class, qrySecPersIdx)
            .setCriteria(lt("id", Integer.MAX_VALUE));

        QueryCursor<Cache.Entry<Long, SecondPerson>> secCursor = cache.query(secQry);

        assertTrue(secCursor.getAll().isEmpty());
    }

    /** */
    @Test
    public void testLtQuery() {
        insertData(cache);

        int pivot = new Random().nextInt(CNT);

        // Lt.
        IndexQuery<Long, SecondPerson> secQry = new IndexQuery<Long, SecondPerson>(SecondPerson.class, qrySecPersIdx)
            .setCriteria(lt("id", CNT + pivot));

        checkSecondPerson(cache.query(secQry), CNT, CNT + pivot);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryPersIdx)
            .setCriteria(lt("id", pivot));

        checkPerson(cache.query(qry), 0, pivot);

        // Lte.
        secQry = new IndexQuery<Long, SecondPerson>(SecondPerson.class, qrySecPersIdx)
            .setCriteria(lte("id", CNT + pivot));

        checkSecondPerson(cache.query(secQry), CNT, CNT + pivot + 1);

        qry = new IndexQuery<Long, Person>(Person.class, qryPersIdx)
            .setCriteria(lte("id", pivot));

        checkPerson(cache.query(qry), 0, pivot + 1);

        // Gt.
        secQry = new IndexQuery<Long, SecondPerson>(SecondPerson.class, qrySecPersIdx)
            .setCriteria(gt("id", CNT + pivot));

        checkSecondPerson(cache.query(secQry), CNT + pivot + 1, 2 * CNT);

        qry = new IndexQuery<Long, Person>(Person.class, qryPersIdx)
            .setCriteria(gt("id", pivot));

        checkPerson(cache.query(qry), pivot + 1, CNT);

        // Gte.
        secQry = new IndexQuery<Long, SecondPerson>(SecondPerson.class, qrySecPersIdx)
            .setCriteria(gte("id", CNT + pivot));

        checkSecondPerson(cache.query(secQry), CNT + pivot, 2 * CNT);

        qry = new IndexQuery<Long, Person>(Person.class, qryPersIdx)
            .setCriteria(gte("id", pivot));

        checkPerson(cache.query(qry), pivot, CNT);
    }

    /** */
    @Test
    public void testKeyPK() {
        insertData(cache);

        int pivot = new Random().nextInt(CNT);

        // Lt.
        IndexQuery<Long, SecondPerson> secQry = new IndexQuery<Long, SecondPerson>(SecondPerson.class, qryKeyPkIdx)
            .setCriteria(lt("_KEY", (long)(CNT + pivot)));

        checkSecondPerson(cache.query(secQry), CNT, CNT + pivot);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryKeyPkIdx)
            .setCriteria(lt("_KEY", (long)pivot));

        checkPerson(cache.query(qry), 0, pivot);

        // Lte.
        secQry = new IndexQuery<Long, SecondPerson>(SecondPerson.class, qryKeyPkIdx)
            .setCriteria(lte("_KEY", (long)(CNT + pivot)));

        checkSecondPerson(cache.query(secQry), CNT, CNT + pivot + 1);

        qry = new IndexQuery<Long, Person>(Person.class, qryKeyPkIdx)
            .setCriteria(lte("_KEY", (long)pivot));

        checkPerson(cache.query(qry), 0, pivot + 1);

        // Gt.
        secQry = new IndexQuery<Long, SecondPerson>(SecondPerson.class, qryKeyPkIdx)
            .setCriteria(gt("_KEY", (long)(CNT + pivot)));

        checkSecondPerson(cache.query(secQry), CNT + pivot + 1, 2 * CNT);

        qry = new IndexQuery<Long, Person>(Person.class, qryKeyPkIdx)
            .setCriteria(gt("_KEY", (long)pivot));

        checkPerson(cache.query(qry), pivot + 1, CNT);

        // Gte.
        secQry = new IndexQuery<Long, SecondPerson>(SecondPerson.class, qryKeyPkIdx)
            .setCriteria(gte("_KEY", (long)(CNT + pivot)));

        checkSecondPerson(cache.query(secQry), CNT + pivot, 2 * CNT);

        qry = new IndexQuery<Long, Person>(Person.class, qryKeyPkIdx)
            .setCriteria(gte("_KEY", (long)pivot));

        checkPerson(cache.query(qry), pivot, CNT);
    }

    /** */
    private void insertData(IgniteCache<Long, Object> cache) {
        for (int i = 0; i < CNT; i++) {
            cache.put((long)i, new Person(i));
            cache.put((long)CNT + i, new SecondPerson(CNT + i));
        }
    }

    /** */
    private void checkPerson(QueryCursor<Cache.Entry<Long, Person>> cursor, int left, int right) {
        List<Cache.Entry<Long, Person>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        Set<Long> expKeys = LongStream.range(left, right).boxed().collect(Collectors.toSet());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Long, Person> entry = all.get(i);

            assertTrue(expKeys.remove(entry.getKey()));

            assertEquals(new Person(entry.getKey().intValue()), all.get(i).getValue());
        }
    }

    /** */
    private void checkSecondPerson(QueryCursor<Cache.Entry<Long, SecondPerson>> cursor, int left, int right) {
        List<Cache.Entry<Long, SecondPerson>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Long, SecondPerson> entry = all.get(i);

            assertEquals(left + i, entry.getKey().intValue());

            assertEquals(new SecondPerson(entry.getKey().intValue()), all.get(i).getValue());
        }
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        int id;

        /** */
        Person(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person[id=" + id + "]";
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return Objects.equals(id, person.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }

    /** */
    private static class SecondPerson {
        /** */
        @QuerySqlField(index = true)
        int id;

        /** */
        SecondPerson(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "SecondPerson[id=" + id + "]";
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SecondPerson person = (SecondPerson)o;

            return Objects.equals(id, person.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }
}
