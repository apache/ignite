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

import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
public class IndexQueryLocalTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String IDX = "PERSON_ID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static Ignite crd;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        crd = startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        crd.cache(CACHE).destroy();
    }

    /** Should return full data. */
    @Test
    public void testServerNodeReplicatedCache() {
        IgniteCache<Long, Person> cache = crd.createCache(ccfg(CacheMode.REPLICATED));

        insertData(crd, cache);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(lt("id", CNT / 2));

        for (int i = 0; i < 4; i++) {
            cache = grid(i).cache(CACHE);

            int result = cache.query(qry.setLocal(true)).getAll().size();

            assertEquals(CNT / 2, result);
        }
    }

    /** Should return part of data only. */
    @Test
    public void testServerNodePartitionedCache() {
        IgniteCache<Long, Person> cache = crd.createCache(ccfg(CacheMode.PARTITIONED));

        insertData(crd, cache);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(lt("id", CNT / 2));

        int resultSize = 0;

        for (int i = 0; i < 4; i++) {
            cache = grid(i).cache(CACHE);

            int result = cache.query(qry.setLocal(true)).getAll().size();

            resultSize += result;

            assertTrue(CNT / 2 > result);
        }

        assertEquals(CNT / 2, resultSize);
    }

    /** Should fail as no data on nodes. */
    @Test
    public void testClientNodeReplicatedCache() throws Exception {
        Ignite cln = startClientGrid(5);

        IgniteCache<Long, Person> cache = cln.createCache(ccfg(CacheMode.REPLICATED));

        insertData(cln, cache);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(lt("id", CNT / 2));

        GridTestUtils.assertThrows(null, () -> cache.query(qry.setLocal(true)).getAll(),
            IgniteException.class, "Failed to execute local index query on a client node.");
    }

    /** Should fail as the local node is a client node and the value type specified for query doesn't exist. */
    @Test
    public void testClientNodeNoValueType() throws Exception {
        Ignite cln = startClientGrid(6);

        IndexQuery qry = new IndexQuery("ValType");

        IgniteCache cache = cln.getOrCreateCache(DEFAULT_CACHE_NAME);

        GridTestUtils.assertThrows(null, () -> cache.query(qry.setLocal(true)).getAll(),
            IgniteException.class, "Failed to execute local index query on a client node.");
    }

    /** */
    private void insertData(Ignite ignite, IgniteCache<Long, Person> cache) {
        try (IgniteDataStreamer<Long, Person> streamer = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < CNT; i++)
                streamer.addData((long)i, new Person(i));
        }
    }

    /** */
    private CacheConfiguration<Long, Person> ccfg(CacheMode cacheMode) {
        return new CacheConfiguration<Long, Person>()
            .setName(CACHE)
            .setIndexedTypes(Long.class, Person.class)
            .setCacheMode(cacheMode)
            .setBackups(2);
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
