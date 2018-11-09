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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for iterator data link erasure after closing or completing
 */
public class H2ResultSetIteratorNullifyOnEndSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_COUNT = 2;

    /** */
    private static final int PERSON_COUNT = 20;

    /** */
    private static final String SELECT_ALL_SQL = "SELECT p.* FROM Person p ORDER BY p.salary";

    /** */
    private static final String SELECT_MAX_SAL_SQLF = "select max(salary) from Person";

    /**
     * Non local SQL check nullification after close
     */
    public void testSqlQueryClose() {
        SqlQuery<String, Person> qry = new SqlQuery<>(Person.class, SELECT_ALL_SQL);

        QueryCursor<Cache.Entry<String, Person>> qryCurs = cache().query(qry);

        qryCurs.iterator();

        qryCurs.close();

        H2ResultSetIterator h2It = extractIteratorInnerGridIteratorInnerH2ResultSetIterator(qryCurs);

        checkIterator(h2It);
    }

    /**
     * Non local SQL check nullification after complete
     */
    public void testSqlQueryComplete() {
        SqlQuery<String, Person> qry = new SqlQuery<>(Person.class, SELECT_ALL_SQL);

        QueryCursor<Cache.Entry<String, Person>> qryCurs = cache().query(qry);

        qryCurs.getAll();

        H2ResultSetIterator h2It = extractIteratorInnerGridIteratorInnerH2ResultSetIterator(qryCurs);

        checkIterator(h2It);
    }

    /**
     * Local SQL check nullification after close
     */
    public void testSqlQueryLocalClose() {
        SqlQuery<String, Person> qry = new SqlQuery<>(Person.class, SELECT_ALL_SQL);

        qry.setLocal(true);

        QueryCursor<Cache.Entry<String, Person>> qryCurs = cache().query(qry);

        qryCurs.iterator();

        qryCurs.close();

        H2ResultSetIterator h2It = extractIterableInnerH2ResultSetIterator(qryCurs);

        checkIterator(h2It);
    }

    /**
     * Local SQL check nullification after complete
     */
    public void testSqlQueryLocalComplete() {
        SqlQuery<String, Person> qry = new SqlQuery<>(Person.class, SELECT_ALL_SQL);

        qry.setLocal(true);

        QueryCursor<Cache.Entry<String, Person>> qryCurs = cache().query(qry);

        qryCurs.getAll();

        H2ResultSetIterator h2It = extractIterableInnerH2ResultSetIterator(qryCurs);

        checkIterator(h2It);
    }

    /**
     * Non local SQL Fields check nullification after close
     */
    public void testSqlFieldsQueryClose() {
        SqlFieldsQuery qry = new SqlFieldsQuery(SELECT_MAX_SAL_SQLF);

        QueryCursor<List<?>> qryCurs = cache().query(qry);

        qryCurs.iterator();

        qryCurs.close();

        H2ResultSetIterator h2It = extractGridIteratorInnerH2ResultSetIterator(qryCurs);

        checkIterator(h2It);
    }

    /**
     * Non local SQL Fields check nullification after complete
     */
    public void testSqlFieldsQueryComplete() {
        SqlFieldsQuery qry = new SqlFieldsQuery(SELECT_MAX_SAL_SQLF);

        QueryCursor<List<?>> qryCurs = cache().query(qry);

        qryCurs.getAll();

        H2ResultSetIterator h2It = extractGridIteratorInnerH2ResultSetIterator(qryCurs);

        checkIterator(h2It);
    }

    /**
     * Local SQL Fields check nullification after close
     */
    public void testSqlFieldsQueryLocalClose() {
        SqlFieldsQuery qry = new SqlFieldsQuery(SELECT_MAX_SAL_SQLF);

        qry.setLocal(true);

        QueryCursor<List<?>> qryCurs = cache().query(qry);

        qryCurs.iterator();

        qryCurs.close();

        H2ResultSetIterator h2It = extractGridIteratorInnerH2ResultSetIterator(qryCurs);

        checkIterator(h2It);
    }

    /**
     * Local SQL Fields check nullification after complete
     */
    public void testSqlFieldsQueryLocalComplete() {
        SqlFieldsQuery qry = new SqlFieldsQuery(SELECT_MAX_SAL_SQLF);

        qry.setLocal(true);

        QueryCursor<List<?>> qryCurs = cache().query(qry);

        qryCurs.getAll();

        H2ResultSetIterator h2It = extractGridIteratorInnerH2ResultSetIterator(qryCurs);

        checkIterator(h2It);
    }

    /**
     * Common Assertion
     * @param h2it target iterator
     */
    private void checkIterator(H2ResultSetIterator h2it){
        if (Objects.nonNull(h2it))
            assertNull(GridTestUtils.getFieldValue(h2it, H2ResultSetIterator.class, "data"));
        else
            fail();
    }

    /**
     * Extract H2ResultSetIterator by reflection for non local SQL cases
     * @param qryCurs source cursor
     * @return target iterator or null of not extracted
     */
    private H2ResultSetIterator extractIteratorInnerGridIteratorInnerH2ResultSetIterator(
        QueryCursor<Cache.Entry<String, Person>> qryCurs) {
        if (QueryCursorImpl.class.isAssignableFrom(qryCurs.getClass())) {
            Iterator inner = GridTestUtils.getFieldValue(qryCurs, QueryCursorImpl.class, "iter");

            GridQueryCacheObjectsIterator it = GridTestUtils.getFieldValue(inner, inner.getClass(), "val$iter0");

            Iterator<List<?>> h2RsIt = GridTestUtils.getFieldValue(it, GridQueryCacheObjectsIterator.class, "iter");

            if (H2ResultSetIterator.class.isAssignableFrom(h2RsIt.getClass()))
                return (H2ResultSetIterator)h2RsIt;
        }
        return null;
    }

    /**
     * Extract H2ResultSetIterator by reflection for local SQL cases.
     *
     * @param qryCurs source cursor
     * @return target iterator or null of not extracted
     */
    private H2ResultSetIterator extractIterableInnerH2ResultSetIterator(
        QueryCursor<Cache.Entry<String, Person>> qryCurs) {
        if (QueryCursorImpl.class.isAssignableFrom(qryCurs.getClass())) {
            Iterable iterable = GridTestUtils.getFieldValue(qryCurs, QueryCursorImpl.class, "iterExec");

            Iterator h2RsIt = GridTestUtils.getFieldValue(iterable, iterable.getClass(), "val$i");

            if (H2ResultSetIterator.class.isAssignableFrom(h2RsIt.getClass()))
                return (H2ResultSetIterator)h2RsIt;
        }
        return null;
    }

    /**
     * Extract H2ResultSetIterator by reflection for SQL Fields cases.
     *
     * @param qryCurs source cursor
     * @return target iterator or null of not extracted
     */
    private H2ResultSetIterator extractGridIteratorInnerH2ResultSetIterator(QueryCursor<List<?>> qryCurs) {
        if (QueryCursorImpl.class.isAssignableFrom(qryCurs.getClass())) {
            GridQueryCacheObjectsIterator it = GridTestUtils.getFieldValue(qryCurs, QueryCursorImpl.class, "iter");

            Iterator<List<?>> h2RsIt = GridTestUtils.getFieldValue(it, GridQueryCacheObjectsIterator.class, "iter");

            if (H2ResultSetIterator.class.isAssignableFrom(h2RsIt.getClass()))
                return (H2ResultSetIterator)h2RsIt;
        }
        return null;
    }

    /**
     * "onClose" should remove links to data.
     */
    public void testOnClose() {
        try {
            GridCloseableIterator it = indexing().queryLocalSql(
                indexing().schema(cache().getName()),
                cache().getName(),
                SELECT_ALL_SQL,
                null,
                Collections.emptySet(),
                "Person",
                null,
                null);

            if (H2ResultSetIterator.class.isAssignableFrom(it.getClass())) {
                H2ResultSetIterator h2it = (H2ResultSetIterator)it;

                h2it.onClose();

                assertNull(GridTestUtils.getFieldValue(h2it, H2ResultSetIterator.class, "data"));
            }
            else
                fail();
        }
        catch (IgniteCheckedException e) {
            fail(e.getMessage());
        }
    }

    /**
     * Complete iterate should remove links to data.
     */
    public void testOnComplete() {
        try {
            GridCloseableIterator it = indexing().queryLocalSql(
                indexing().schema(cache().getName()),
                cache().getName(),
                SELECT_ALL_SQL,
                null,
                Collections.emptySet(),
                "Person",
                null,
                null);

            if (H2ResultSetIterator.class.isAssignableFrom(it.getClass())) {
                H2ResultSetIterator h2it = (H2ResultSetIterator)it;

                while (h2it.onHasNext())
                    h2it.onNext();

                assertNull(GridTestUtils.getFieldValue(h2it, H2ResultSetIterator.class, "data"));
            }
            else
                fail();
        }
        catch (IgniteCheckedException e) {
            fail(e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_COUNT);

        ignite(0).createCache(
            new CacheConfiguration<String, Person>("pers").setIndexedTypes(String.class, Person.class)
        );

        awaitPartitionMapExchange();

        populateDataIntoPerson();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @return H2 indexing instance.
     */
    private IgniteH2Indexing indexing() {
        GridQueryProcessor qryProcessor = grid(0).context().query();

        return GridTestUtils.getFieldValue(qryProcessor, GridQueryProcessor.class, "idx");
    }

    /**
     * @return Cache.
     */
    private IgniteCache<String, Person> cache() {
        return grid(0).cache("pers");
    }

    /**
     * Populate person cache with test data
     */
    private void populateDataIntoPerson() {
        IgniteCache<String, Person> cache = cache();

        int personId = 0;

        for (int j = 0; j < PERSON_COUNT; j++) {
            Person prsn = new Person();

            prsn.setId("pers" + personId);
            prsn.setName("Person name #" + personId);

            cache.put(prsn.getId(), prsn);

            personId++;
        }
    }

    /**
     *
     */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        @QuerySqlField(index = true)
        private int salary;

        /** */
        public String getId() {
            return id;
        }

        /** */
        public void setId(String id) {
            this.id = id;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }

        /** */
        public int getSalary() {
            return salary;
        }

        /** */
        public void setSalary(int salary) {
            this.salary = salary;
        }
    }
}
