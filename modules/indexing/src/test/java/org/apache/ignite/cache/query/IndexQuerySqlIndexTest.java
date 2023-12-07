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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

/** */
@RunWith(Parameterized.class)
public class IndexQuerySqlIndexTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String CACHE_TABLE = "TEST_CACHE_TABLE";

    /** */
    private static final String TABLE = "TEST_TABLE";

    /** */
    private static final String VALUE_TYPE = "MY_VALUE_TYPE";

    /** */
    private static final String DESC_ID_IDX = "DESC_ID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** Query index, {@code null} of index name. */
    @Parameterized.Parameter()
    public String qryDescIdxName;

    /** */
    @Parameterized.Parameters(name = "qryIdxName={0}")
    public static Collection<?> testParams() {
        return Arrays.asList(null, DESC_ID_IDX);
    }

    /** */
    private IgniteCache<Object, Object> cache;

    /** */
    private IgniteCache<Object, Object> tblCache;

    /** */
    private Ignite crd;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        crd = startGrids(2);

        cache = crd.createCache(new CacheConfiguration<>().setName(CACHE));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testEmptyCache() {
        prepareTable(Person.class.getName(), DESC_ID_IDX, "descId", false);

        tblCache = crd.cache(CACHE_TABLE);

        IndexQuery<Long, Object> qry = new IndexQuery<Long, Object>(Person.class.getName(), qryDescIdxName)
            .setCriteria(lte("descId", Integer.MAX_VALUE));

        assertTrue(tblCache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<>(Person.class.getName(), qryDescIdxName);

        assertTrue(tblCache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testWrongQueries() {
        prepareTable(Person.class.getName(), DESC_ID_IDX, "descId", false);

        tblCache = crd.cache(CACHE_TABLE);

        // Wrong fields in query.
        if (qryDescIdxName != null) {
            GridTestUtils.assertThrowsAnyCause(null, () -> {
                IndexQuery<Long, Object> wrongQry = new IndexQuery<Long, Object>(Person.class.getName(), qryDescIdxName)
                    .setCriteria(lt("id", Integer.MAX_VALUE));

                return tblCache.query(wrongQry).getAll();

            }, IgniteCheckedException.class, "Index doesn't match criteria.");
        }

        // Wrong cache.
        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Object> wrongQry = new IndexQuery<Long, Object>(Person.class.getName(), qryDescIdxName)
                .setCriteria(lt("descId", Integer.MAX_VALUE));

            return cache.query(wrongQry).getAll();

        }, IgniteCheckedException.class, "No table found for type: " + Person.class.getName());
    }

    /** Should support both fields: normalized and original. */
    @Test
    public void testRangeQueries() {
        prepareTable(Person.class.getName(), DESC_ID_IDX, "descId", true);

        int pivot = new Random().nextInt(CNT);

        tblCache = crd.cache(CACHE_TABLE);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class.getName(), qryDescIdxName)
            .setCriteria(lt("descId", pivot));

        check(qry, 0, pivot);

        qry = new IndexQuery<Long, Person>(Person.class.getName(), qryDescIdxName)
            .setCriteria(lt("DESCID", pivot));

        check(qry, 0, pivot);

        qry = new IndexQuery<>(Person.class.getName(), qryDescIdxName);

        check(qry, 0, CNT);
    }

    /** Should support only original field. */
    @Test
    public void testEscapedColumnName() {
        prepareTable(Person.class.getName(), DESC_ID_IDX, "\"descId\"", true);

        int pivot = new Random().nextInt(CNT);

        tblCache = crd.cache(CACHE_TABLE);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class.getName(), qryDescIdxName)
            .setCriteria(lt("descId", pivot));

        check(qry, 0, pivot);

        String errMsg = qryDescIdxName != null ? "Index doesn't match criteria." : "No index found for criteria.";

        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Object> wrongQry = new IndexQuery<Long, Object>(Person.class.getName(), qryDescIdxName)
                .setCriteria(lt("DESCID", Integer.MAX_VALUE));

            return tblCache.query(wrongQry).getAll();

        }, IgniteCheckedException.class, errMsg);
    }

    /** Should support only original field. */
    @Test
    public void testEscapedIndexName() {
        prepareTable(Person.class.getName(), "\"" + DESC_ID_IDX.toLowerCase() + "\"", "descId", true);

        int pivot = new Random().nextInt(CNT);

        tblCache = crd.cache(CACHE_TABLE);

        String idx = qryDescIdxName == null ? qryDescIdxName : qryDescIdxName.toLowerCase();

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class.getName(), idx)
            .setCriteria(lt("descId", pivot));

        check(qry, 0, pivot);

        if (qryDescIdxName != null) {
            GridTestUtils.assertThrowsAnyCause(null, () -> {
                IndexQuery<Long, Object> wrongQry = new IndexQuery<Long, Object>(Person.class.getName(), qryDescIdxName)
                    .setCriteria(lt("descId", Integer.MAX_VALUE));

                return tblCache.query(wrongQry).getAll();

            }, IgniteCheckedException.class, "No index found");
        }
    }

    /** */
    @Test
    public void testRangeQueriesWithKeepBinary() {
        prepareTable(Person.class.getName(), DESC_ID_IDX, "descId", true);

        int pivot = new Random().nextInt(CNT);

        tblCache = crd.cache(CACHE_TABLE).withKeepBinary();

        IndexQuery<Long, BinaryObject> qry = new IndexQuery<Long, BinaryObject>(Person.class.getName(), qryDescIdxName)
            .setCriteria(lt("descId", pivot));

        checkBinary(tblCache.query(qry), 0, pivot);
    }

    /** */
    @Test
    public void testRangeQueriesWithValueType() {
        prepareTable(VALUE_TYPE, DESC_ID_IDX, "descId", true);

        int pivot = new Random().nextInt(CNT);

        tblCache = crd.cache(CACHE_TABLE).withKeepBinary();

        IndexQuery<Long, BinaryObject> qry = new IndexQuery<Long, BinaryObject>(VALUE_TYPE, qryDescIdxName)
            .setCriteria(lt("descId", pivot));

        checkBinary(tblCache.query(qry), 0, pivot);
    }

    /** */
    @Test
    public void testReverseFieldOrder() {
        prepareTable(Person.class.getName(), DESC_ID_IDX, "descId", true);

        int pivot = new Random().nextInt(CNT);

        tblCache = crd.cache(CACHE_TABLE);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class.getName(), qryDescIdxName)
            .setCriteria(eq("_KEY", (long)pivot), lte("descId", pivot));

        check(qry, pivot, pivot + 1);
    }

    /**
     * @param left First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private void check(IndexQuery<Long, Person> qry, int left, int right) {
        boolean pk = qry.getIndexName() == null && F.isEmpty(qry.getCriteria());

        List<Cache.Entry<Long, Person>> all = tblCache.query(qry).getAll();

        assertEquals(right - left, all.size());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Long, Person> entry = all.get(i);

            int exp = pk ? left + i : right - i - 1;

            assertEquals(exp, entry.getKey().intValue());

            assertEquals(new Person(entry.getKey().intValue()), all.get(i).getValue());
        }
    }

    /**
     * @param left First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private void checkBinary(QueryCursor<Cache.Entry<Long, BinaryObject>> cursor, int left, int right) {
        List<Cache.Entry<Long, BinaryObject>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Long, BinaryObject> entry = all.get(i);

            assertEquals(right - 1 - i, entry.getKey().intValue());
            assertEquals(entry.getKey().intValue(), (int)entry.getValue().field("id"));
            assertEquals(entry.getKey().intValue(), (int)entry.getValue().field("descId"));
        }
    }

    /** */
    private void prepareTable(String valType, String idxName, String descIdFld, boolean insert) {
        SqlFieldsQuery qry = new SqlFieldsQuery("create table " + TABLE + " (prim_id long PRIMARY KEY, id int, " + descIdFld + " int)" +
            " with \"VALUE_TYPE=" + valType + ",CACHE_NAME=" + CACHE_TABLE + "\";");

        cache.query(qry);

        qry = new SqlFieldsQuery("create index " + idxName + " on " + TABLE + " (" + descIdFld + " DESC);");

        cache.query(qry);

        if (insert) {
            qry = new SqlFieldsQuery("insert into " + TABLE + " (prim_id, id, " + descIdFld + ") values(?, ?, ?);");

            for (int i = 0; i < CNT; i++) {
                qry.setArgs((long)i, i, i);

                cache.query(qry);
            }
        }
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

            return Objects.equals(id, person.id)
                && Objects.equals(descId, person.descId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, descId);
        }
    }
}
