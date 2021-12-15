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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
@RunWith(Parameterized.class)
public class IndexQueryQueryEntityTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String CACHE_TBL_NAME = "TEST_CACHE_TBL_NAME";

    /** */
    private static final String TABLE = "TEST_TABLE";

    /** */
    private static final String ID_IDX = "ID_IDX";

    /** */
    private static final String DESC_ID_IDX = "DESC_ID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** Query index, {@code null} or index name. */
    @Parameterized.Parameter
    public String qryIdx;

    /** Query desc index, {@code null} or index name. */
    @Parameterized.Parameter(1)
    public String qryDescIdx;

    /** */
    @Parameterized.Parameters(name = "qryIdx={0}, qryDescIdx={1}")
    public static List<Object[]> params() {
        return F.asList(
            new Object[] {null, null},
            new Object[] {ID_IDX, DESC_ID_IDX}
        );
    }

    /** */
    private static IgniteCache<Long, Person> cache;

    /** */
    private static IgniteCache<Long, Person> cacheTblName;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite crd = startGrids(4);

        cache = crd.cache(CACHE);
        cacheTblName = crd.cache(CACHE_TBL_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache.clear();
        cacheTblName.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        QueryIndex idIdx = new QueryIndex("id", true, ID_IDX);
        QueryIndex descIdIdx = new QueryIndex("descId", false, DESC_ID_IDX);

        QueryEntity e = new QueryEntity(Long.class.getName(), Person.class.getName())
            .setFields(new LinkedHashMap<>(
                F.asMap("id", Integer.class.getName(), "descId", Integer.class.getName()))
            )
            .setIndexes(Arrays.asList(idIdx, descIdIdx));

        CacheConfiguration<?, ?> ccfg1 = new CacheConfiguration<>()
            .setName(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(Collections.singletonList(e));

        QueryEntity entTableName = new QueryEntity(e);
        entTableName.setTableName(TABLE);

        CacheConfiguration<?, ?> ccfg2 = new CacheConfiguration<>()
            .setName(CACHE_TBL_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(Collections.singletonList(entTableName));

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /** */
    @Test
    public void testEmptyCache() {
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
            .setCriteria(lt("id", Integer.MAX_VALUE));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Long, Person>(Person.class, qryDescIdx)
            .setCriteria(lt("descId", Integer.MAX_VALUE));

        assertTrue(cache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testEmptyCacheTableName() {
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
            .setCriteria(lt("id", Integer.MAX_VALUE));

        assertTrue(cacheTblName.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Long, Person>(Person.class, qryDescIdx)
            .setCriteria(lt("descId", Integer.MAX_VALUE));

        assertTrue(cacheTblName.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
            .setCriteria(lt("id", Integer.MAX_VALUE));

        assertTrue(cacheTblName.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Long, Person>(Person.class, qryDescIdx)
            .setCriteria(lt("descId", Integer.MAX_VALUE));

        assertTrue(cacheTblName.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testRangeQueries() {
        insertData(cache);

        int pivot = new Random().nextInt(CNT);

        // Lt.
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
            .setCriteria(lt("id", pivot));

        check(cache.query(qry), 0, pivot, false);

        // Lt, desc index.
        IndexQuery<Long, Person> descQry = new IndexQuery<Long, Person>(Person.class, qryDescIdx)
            .setCriteria(lt("descId", pivot));

        check(cache.query(descQry), 0, pivot, true);
    }

    /** */
    @Test
    public void testRangeQueriesCacheTableName() {
        insertData(cacheTblName);

        int pivot = new Random().nextInt(CNT);

        // Lt.
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
            .setCriteria(lt("id", pivot));

        check(cacheTblName.query(qry), 0, pivot, false);

        // Lt, desc index.
        IndexQuery<Long, Person> descQry = new IndexQuery<Long, Person>(Person.class, qryDescIdx)
            .setCriteria(lt("descId", pivot));

        check(cacheTblName.query(descQry), 0, pivot, true);
    }

    /**
     * @param left First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private void check(QueryCursor<Cache.Entry<Long, Person>> cursor, int left, int right, boolean desc) {
        List<Cache.Entry<Long, Person>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Long, Person> entry = all.get(i);

            int exp = desc ? right - i - 1 : left + i;

            assertEquals(exp, entry.getKey().intValue());

            assertEquals(new Person(entry.getKey().intValue()), all.get(i).getValue());
        }
    }

    /** */
    private void insertData(IgniteCache<Long, Person> cache) {
        for (int i = 0; i < CNT; i++)
            cache.put((long)i, new Person(i));
    }

    /** */
    private static class Person {
        /** */
        final int id;

        /** */
        final int descId;

        /** */
        Person(int id) {
            this.id = id;
            descId = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return Objects.equals(id, person.id) && Objects.equals(descId, person.descId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, descId);
        }
    }
}
