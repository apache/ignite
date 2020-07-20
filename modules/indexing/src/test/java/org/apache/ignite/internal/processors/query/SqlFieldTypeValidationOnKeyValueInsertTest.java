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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
@RunWith(Parameterized.class)
public class SqlFieldTypeValidationOnKeyValueInsertTest extends AbstractIndexingCommonTest {
    /** */
    private static final String SQL_TEXT = "select id, name from Person where id=1";

    /** */
    private static final String ERROR = "Type for a column 'NAME' is not compatible with table definition.";

    /** */
    private static boolean validate;

    /** */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(2)
    public boolean near;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setValidationEnabled(validate);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setCacheMode(cacheMode)
                .setAtomicityMode(atomicityMode)
                .setNearConfiguration(near ? new NearCacheConfiguration<>().setNearStartSize(1) : null)
                .setQueryEntities(Collections.singletonList(
                        new QueryEntity()
                                .setKeyFieldName("id")
                                .setValueType("Person")
                                .setFields(new LinkedHashMap<>(
                                        F.asMap("id", "java.lang.Integer",
                                                "name", "java.util.UUID"))))));

        cfg.setBinaryConfiguration(new BinaryConfiguration().setNameMapper(new BinaryBasicNameMapper(true)));

        return cfg;
    }

    /** */
    @Parameterized.Parameters(name = "{index} {0} {1} {2}")
    public static List<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[] {REPLICATED, TRANSACTIONAL, true});
        res.add(new Object[] {REPLICATED, TRANSACTIONAL, false});
        res.add(new Object[] {REPLICATED, ATOMIC, true});
        res.add(new Object[] {REPLICATED, ATOMIC, false});
        res.add(new Object[] {PARTITIONED, TRANSACTIONAL, true});
        res.add(new Object[] {PARTITIONED, TRANSACTIONAL, false});
        res.add(new Object[] {PARTITIONED, ATOMIC, true});
        res.add(new Object[] {PARTITIONED, ATOMIC, false});

        return res;
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testError() throws Exception {
        validate = true;

        startGrids(2);

        IgniteEx clnt = startClientGrid(2);

        IgniteCache<Object, Object> cache = clnt.cache(DEFAULT_CACHE_NAME);

        BinaryObject obj = clnt.binary().builder("Person")
                .setField("id", 1)
                .setField("name", UUID.randomUUID().toString())
                .build();

        doInsertsError(cache, obj);
    }

    /** */
    @Test
    public void testSuccess() throws Exception {
        validate = true;

        startGrids(2);

        IgniteEx clnt = startClientGrid(2);

        IgniteCache<Object, Object> cache = clnt.cache(DEFAULT_CACHE_NAME).withKeepBinary();

        BinaryObject obj = clnt.binary().builder("Person")
                .setField("id", 1)
                .setField("name", UUID.randomUUID())
                .build();

        doInsertSuccess(cache, obj);

        assertEquals(obj, cache.withKeepBinary().get(1));

        grid(0).context().query()
                .querySqlFields(
                new SqlFieldsQuery(SQL_TEXT).setSchema(DEFAULT_CACHE_NAME), true)
                .getAll();
    }

    /** */
    @Test
    public void testSkipValidation() throws Exception {
        validate = false;

        startGrids(2);

        IgniteEx clnt = startClientGrid(2);

        IgniteCache<Object, Object> cache = clnt.cache(DEFAULT_CACHE_NAME).withKeepBinary();

        BinaryObject obj = clnt.binary().builder("Person")
                .setField("id", 1)
                .setField("name", UUID.randomUUID().toString())
                .build();

        doInsertSuccess(cache, obj);

        assertEquals(obj, cache.withKeepBinary().get(1));
    }

    /** */
    @Test
    public void testClassInstanceError() throws Exception {
        validate = true;

        startGrids(2);

        IgniteEx clnt = startClientGrid(2);

        IgniteCache<Object, Object> cache = clnt.cache(DEFAULT_CACHE_NAME);

        doInsertsError(cache, new Person(UUID.randomUUID().toString()));
    }

    /** */
    @Test
    public void testClassInstanceSkipValidation() throws Exception {
        validate = false;

        startGrids(2);

        IgniteEx clnt = startClientGrid(2);

        IgniteCache<Object, Object> cache = clnt.cache(DEFAULT_CACHE_NAME);

        Person obj = new Person(UUID.randomUUID().toString());

        doInsertSuccess(cache, obj);

        assertEquals(obj, cache.get(1));
    }

    /** */
    private void doInsertsError(IgniteCache<Object, Object> cache, Object obj) {
        assertThrows(() -> cache.put(1, obj), ERROR);

        assertThrows(() -> cache.putAll(F.asMap(1, obj, 2, obj)), ERROR);

        assertThrows(() -> cache.putIfAbsent(1, obj), ERROR);

        assertThrows(() -> cache.invoke(1, new TestEntryProcessor(obj)), ERROR);

        assertThrows(() -> cache.invokeAll(
                F.asMap(1, new TestEntryProcessor(obj), 2, new TestEntryProcessor(obj))).get(1).get(),
                ERROR);

        assertThrows(() -> cache.replace(1, obj), ERROR);

        assertThrows(() -> cache.getAndPut(1, obj), ERROR);

        assertThrows(() -> cache.getAndPutIfAbsent(1, obj), ERROR);

        assertThrows(() -> cache.getAndReplace(1, obj), ERROR);
    }

    /** */
    private void assertThrows(GridTestUtils.RunnableX runx, String msg) {
        try {
            runx.runx();
        }
        catch (Exception e) {
            if (X.hasCause(e, msg, Throwable.class))
                return;

            throw new AssertionError("Unexpected exception ", e);
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /** */
    private void doInsertSuccess(IgniteCache<Object, Object> cache, Object obj) {
        cache.put(1, obj);

        cache.putAll(F.asMap(1, obj, 2, obj));

        cache.putIfAbsent(1, obj);

        cache.invoke(1, new TestEntryProcessor(obj));

        Map<Object, EntryProcessorResult<Void>> m =
            cache.invokeAll(F.asMap(1, new TestEntryProcessor(obj), 2, new TestEntryProcessor(obj)));

        assertTrue(m == null || m.isEmpty());

        cache.replace(1, obj);

        cache.getAndPut(1, obj);

        cache.getAndPutIfAbsent(1, obj);

        cache.getAndReplace(1, obj);
    }

    /** */
    public static class Person {
        /** */
        private final Object name;

        /** */
        public Person(Object name) {
            this.name = name;
        }

        /** */
        public Object name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof Person && F.eq(((Person) obj).name, name);
        }
    }

    /** */
    public static class TestEntryProcessor implements EntryProcessor<Object, Object, Void> {
        /** */
        private final Object val;

        /** */
        public TestEntryProcessor(Object val) {
            this.val = val;
        }

        /** */
        @Override public Void process(MutableEntry<Object, Object> entry, Object... args) {
            entry.setValue(val);

            return null;
        }
    }
}
