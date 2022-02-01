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

import java.util.List;
import java.util.Objects;
import java.util.Random;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
public class IndexQueryKeepBinaryTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String IDX = "PERSON_ID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static IgniteCache<Long, Person> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite crd = startGrids(2);

        cache = crd.cache(CACHE);

        insertData(crd, cache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Long, Person> ccfg = new CacheConfiguration<Long, Person>()
            .setName(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Long.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** Should return full data. */
    @Test
    public void testServerNodeReplicatedCache() {
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(lt("id", CNT / 2));

        check(cache.withKeepBinary().query(qry), 0, CNT / 2);
    }

    /** Should return full data. */
    @Test
    public void testBinaryFilter() {
        IndexQuery<Long, BinaryObject> qry = new IndexQuery<Long, BinaryObject>(Person.class, IDX)
            .setCriteria(lt("id", CNT / 2))
            .setFilter((k, v) -> (int)v.field("id") > CNT / 4);

        check(cache.withKeepBinary().query(qry), CNT / 4 + 1, CNT / 2);
    }

    /** */
    @Test
    public void testComplexSqlPrimaryKey() {
        String valType = "MY_VALUE_TYPE";
        String tblCacheName = "MY_TABLE_CACHE";

        SqlFieldsQuery qry = new SqlFieldsQuery("create table my_table (id1 int, id2 int, id3 int," +
            " PRIMARY KEY(id1, id2)) with \"VALUE_TYPE=" + valType + ",CACHE_NAME=" + tblCacheName + "\";");

        cache.query(qry);

        qry = new SqlFieldsQuery("insert into my_table(id1, id2, id3) values(?, ?, ?);");

        for (int i = 0; i < CNT; i++) {
            qry.setArgs(i, i, i);

            cache.query(qry);
        }

        int pivot = new Random().nextInt(CNT);

        IgniteCache<BinaryObject, BinaryObject> tblCache = grid(0).cache(tblCacheName);

        IndexQuery<BinaryObject, BinaryObject> idxQry = new IndexQuery<BinaryObject, BinaryObject>(valType)
            .setCriteria(lt("id1", pivot));

        checkBinary(tblCache.withKeepBinary().query(idxQry), 0, pivot);
    }

    /**
     * @param left First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private void check(QueryCursor cursor, int left, int right) {
        List<Cache.Entry<Long, BinaryObject>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Long, ?> entry = all.get(i);

            assertEquals(left + i, entry.getKey().intValue());

            BinaryObject o = all.get(i).getValue();

            assertEquals(new Person(entry.getKey().intValue()), o.deserialize());
        }
    }

    /** */
    private void checkBinary(QueryCursor cursor, int left, int right) {
        List<Cache.Entry<BinaryObject, BinaryObject>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<BinaryObject, BinaryObject> entry = all.get(i);

            assertEquals(left + i, (int)entry.getKey().field("id1"));
            assertEquals(left + i, (int)entry.getKey().field("id2"));
            assertEquals(left + i, (int)entry.getValue().field("id3"));
        }
    }

    /** */
    private void insertData(Ignite ignite, IgniteCache cache) {
        try (IgniteDataStreamer<Long, Person> streamer = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < CNT; i++)
                streamer.addData((long)i, new Person(i));
        }
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        final int id;

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
}
