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

package org.apache.ignite.internal.processors.cache.index;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.H2TableDescriptor.AFFINITY_KEY_IDX_NAME;
import static org.apache.ignite.internal.processors.query.h2.H2TableDescriptor.PK_IDX_NAME;

/**
 * Basic tests for different types of indexed data
 * for tables created through Cache API.
 */
public class BasicJavaTypesIndexTest extends AbstractIndexingCommonTest {
    /** Count of entries that should be preloaded to the cache. */
    private static final int DATSET_SIZE = 1_000;

    /** Table ID counter. */
    private static final AtomicInteger TBL_ID = new AtomicInteger();

    /** Template of SELECT query with filtering by range and ordering by indexed column. */
    private static final String SELECT_ORDERED_RANGE_TEMPLATE =
        "SELECT val FROM \"%s\" USE INDEX(\"%s\") WHERE %s <= ? ORDER BY idxVal ASC";

    /** Template of SELECT query with filtering by indexed column. */
    private static final String SELECT_VALUE_TEMPLATE =
        "SELECT val, idxVal FROM \"%s\" USE INDEX(\"%s\") WHERE %s = ?";

    /** Max length for generator of string values. */
    private int maxStrLen;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(1, 2);

        startClientGrid(0);
    }

    /** */
    @Before
    public void clearState() {
        maxStrLen = 40;
    }

    /** */
    @Test
    public void testJavaBooleanIndex() {
        createPopulateAndVerify(Boolean.class, Boolean::compareTo, null);
        createPopulateAndVerify(Boolean.class, Boolean::compareTo, TestKeyWithAff.class);
        createPopulateAndVerify(Boolean.class, Boolean::compareTo, TestKeyWithIdx.class);
    }

    /** */
    @Test
    public void testJavaByteIndex() {
        createPopulateAndVerify(Byte.class, Byte::compareTo, null);
        createPopulateAndVerify(Byte.class, Byte::compareTo, TestKeyWithAff.class);
        createPopulateAndVerify(Byte.class, Byte::compareTo, TestKeyWithIdx.class);
    }

    /** */
    @Test
    public void testJavaShortIndex() {
        createPopulateAndVerify(Short.class, Short::compareTo, null);
        createPopulateAndVerify(Short.class, Short::compareTo, TestKeyWithAff.class);
        createPopulateAndVerify(Short.class, Short::compareTo, TestKeyWithIdx.class);
    }

    /** */
    @Test
    public void testJavaIntegerIndex() {
        createPopulateAndVerify(Integer.class, Integer::compareTo, null);
        createPopulateAndVerify(Integer.class, Integer::compareTo, TestKeyWithAff.class);
        createPopulateAndVerify(Integer.class, Integer::compareTo, TestKeyWithIdx.class);
    }

    /** */
    @Test
    public void testJavaLongIndex() {
        createPopulateAndVerify(Long.class, Long::compareTo, null);
        createPopulateAndVerify(Long.class, Long::compareTo, TestKeyWithAff.class);
        createPopulateAndVerify(Long.class, Long::compareTo, TestKeyWithIdx.class);
    }

    /** */
    @Test
    public void testJavaFloatIndex() {
        createPopulateAndVerify(Float.class, Float::compareTo, null);
        createPopulateAndVerify(Float.class, Float::compareTo, TestKeyWithAff.class);
        createPopulateAndVerify(Float.class, Float::compareTo, TestKeyWithIdx.class);
    }

    /** */
    @Test
    public void testJavaDoubleIndex() {
        createPopulateAndVerify(Double.class, Double::compareTo, null);
        createPopulateAndVerify(Double.class, Double::compareTo, TestKeyWithAff.class);
        createPopulateAndVerify(Double.class, Double::compareTo, TestKeyWithIdx.class);
    }

    /** */
    @Test
    public void testJavaBigDecimalIndex() {
        createPopulateAndVerify(BigDecimal.class, BigDecimal::compareTo, null);
        createPopulateAndVerify(BigDecimal.class, BigDecimal::compareTo, TestKeyWithAff.class);
        createPopulateAndVerify(BigDecimal.class, BigDecimal::compareTo, TestKeyWithIdx.class);
    }

    /** */
    @Test
    public void testJavaStringIndex() {
        createPopulateAndVerify(String.class, String::compareTo, null);
        createPopulateAndVerify(String.class, String::compareTo, TestKeyWithAff.class);
        createPopulateAndVerify(String.class, String::compareTo, TestKeyWithIdx.class);
    }

    /** */
    @Test
    public void testJavaPojoIndex() {
        createPopulateAndVerify(TestPojo.class, null, null);
        createPopulateAndVerify(TestPojo.class, null, TestKeyWithAff.class);
        createPopulateAndVerify(TestPojo.class, null, TestKeyWithIdx.class);
    }

    /**
     * Executes test scenario: <ul>
     * <li>Create cache</li>
     * <li>Populate cache with random data</li>
     * <li>Verify range query on created table</li>
     * <li>Verify that table stores the same data as the generated dataset</li>
     * <li>Destroy cache</li>
     * </ul>
     *
     * @param idxCls Index type class.
     * @param comp Comparator to sort data set to perform range check.
     * If {@code null} range check will not be performed.
     * @param keyCls Key type class. Will be used to generate KEY object
     * for cache operations. If {@code null} idxCls will be used.
     * @param <Key> Type of the key in terms of the cache.
     * @param <Idx> Type of the indexed field.
     */
    private <Key extends ClassWrapper, Idx> void createPopulateAndVerify(Class<Idx> idxCls,
        @Nullable Comparator<Idx> comp, @Nullable Class<Key> keyCls) {
        Ignite ign = grid(0);

        String tblName = idxCls.getSimpleName().toUpperCase() + "_TBL" + TBL_ID.incrementAndGet();

        try {
            // Create cache
            LinkedHashMap<String, String> fields = new LinkedHashMap<>(2);

            fields.put("idxVal", idxCls.getName());
            fields.put("val", Integer.class.getName());

            QueryEntity qe = new QueryEntity(keyCls == null ? idxCls.getName() : keyCls.getName(), Integer.class.getName())
                .setTableName(tblName)
                .setValueFieldName("val")
                .setFields(fields);

            String idxName;
            String idxFieldName;

            if (keyCls == null) {
                qe.setKeyFieldName("idxVal");

                idxName = PK_IDX_NAME;
                idxFieldName = KEY_FIELD_NAME;
            }
            else {
                idxFieldName = "idxVal";

                qe.setKeyFields(Collections.singleton(idxFieldName));

                if (keyCls.equals(TestKeyWithAff.class))
                    idxName = AFFINITY_KEY_IDX_NAME;
                else {
                    idxName = "IDXVAL_IDX";

                    qe.setIndexes(Collections.singleton(new QueryIndex(idxFieldName, true, idxName)));
                }
            }

            IgniteCache<Object, Integer> cache = ign.createCache(
                new CacheConfiguration<Object, Integer>(tblName + "_CACHE")
                    .setKeyConfiguration(new CacheKeyConfiguration((keyCls != null ? keyCls : idxCls).getName(), "idxVal"))
                    .setQueryEntities(Collections.singletonList(qe)).setSqlSchema("PUBLIC"));

            // Then populate it with random data
            Map<Idx, Integer> data = new TreeMap<>(comp);

            if (keyCls == null)
                populateTable(data, cache, idxCls);
            else
                populateTable(data, cache, keyCls, idxCls);

            // Perform necessary verifications
            if (comp != null)
                verifyRange(data, tblName, idxFieldName, idxName, comp);

            verifyEach(data, tblName, idxFieldName, idxName);
        }
        finally {
            // Destroy cache
            ign.destroyCache(tblName + "_CACHE");
        }
    }

    /**
     * Populate given cache with random data.
     *
     * @param data Map which will be used to store all generated data.
     * @param cache Cache that should be populated.
     * @param keyCls Class of the key object. Used for generating random value
     * of the required type.
     * @param idxCls Class of the indexed value. Used for generating random value
     * of the required type.
     * @param <Key> Type of the key object.
     * @param <Idx> Type of the indexed value.
     */
    private <Key, Idx> void populateTable(Map<Idx, Integer> data, IgniteCache<Object, Integer> cache, Class<Key> keyCls,
        Class<Idx> idxCls) {
        Map<Idx, Key> idxToKey = new HashMap<>();

        for (int i = 0; i < DATSET_SIZE; i++) {
            Key key = nextVal(keyCls, idxCls);
            int val = nextVal(Integer.class, null);

            try {
                Field f = keyCls.getDeclaredField("idxVal");

                f.setAccessible(true);

                Idx idx = idxCls.cast(f.get(key));

                Key old = idxToKey.put(idx, key);

                if (old != null)
                    cache.remove(old);

                data.put(idx, val);
                cache.put(key, val);
            }
            catch (Exception ex) {
                fail("Unable to populate table: " + ex);
            }
        }
    }

    /**
     * Populate given cache with random data.
     *
     * @param data Map which will be used to store all generated data.
     * @param cache Cache that should be populated.
     * @param keyCls Class of the key object. Used for generating random value
     * of the required type.
     * @param <T> Type of the key object.
     */
    private <T> void populateTable(Map<T, Integer> data, IgniteCache<Object, Integer> cache, Class<T> keyCls) {
        for (int i = 0; i < DATSET_SIZE; i++) {
            T key = nextVal(keyCls, null);
            int val = nextVal(Integer.class, null);

            data.put(key, val);
            cache.put(key, val);
        }
    }

    /**
     * Generates random value for the given class.
     *
     * @param cls Class of the required value.
     * @param innerCls Class of the value for inner object. May be null.
     * @param <T> Type of the required value.
     * @param <InnerT> Type of the inner value for complex objects.
     *
     * @return Generated value.
     */
    private <T, InnerT> T nextVal(Class<T> cls, @Nullable Class<InnerT> innerCls) {
        if (cls.isAssignableFrom(Boolean.class))
            return cls.cast(ThreadLocalRandom.current().nextBoolean());

        if (cls.isAssignableFrom(Byte.class))
            return cls.cast((byte)ThreadLocalRandom.current().nextInt());

        if (cls.isAssignableFrom(Short.class))
            return cls.cast((short)ThreadLocalRandom.current().nextInt());

        if (cls.isAssignableFrom(Integer.class))
            return cls.cast(ThreadLocalRandom.current().nextInt());

        if (cls.isAssignableFrom(Long.class))
            return cls.cast(ThreadLocalRandom.current().nextLong());

        if (cls.isAssignableFrom(Float.class))
            return cls.cast(ThreadLocalRandom.current().nextFloat());

        if (cls.isAssignableFrom(Double.class))
            return cls.cast(ThreadLocalRandom.current().nextDouble());

        if (cls.isAssignableFrom(BigDecimal.class))
            return cls.cast(new BigDecimal(ThreadLocalRandom.current().nextDouble()));

        if (cls.isAssignableFrom(String.class))
            return cls.cast(GridTestUtils.randomString(ThreadLocalRandom.current(), 1, maxStrLen));

        if (cls.isAssignableFrom(UUID.class))
            return cls.cast(new UUID(
                ThreadLocalRandom.current().nextLong(),
                ThreadLocalRandom.current().nextLong()
            ));

        if (cls.isAssignableFrom(TestPojo.class))
            return cls.cast(new TestPojo(ThreadLocalRandom.current().nextInt()));

        if (cls.isAssignableFrom(TestKeyWithIdx.class))
            return cls.cast(new TestKeyWithIdx<>(
                ThreadLocalRandom.current().nextInt(),
                innerCls != null ? nextVal(innerCls, null) : null)
            );

        if (cls.isAssignableFrom(TestKeyWithAff.class))
            return cls.cast(new TestKeyWithAff<>(
                ThreadLocalRandom.current().nextInt(),
                innerCls != null ? nextVal(innerCls, null) : null)
            );

        throw new IllegalStateException("There is no generator for class=" + cls.getSimpleName());
    }

    /**
     * Verifies range querying.
     *
     * @param data Testing dataset.
     * @param tblName Name of the table from which values should be queried.
     * @param idxName Name of the index.
     * @param comp Comparator.
     * @param <T> Java type mapping of the indexed column.
     */
    private <T> void verifyRange(Map<T, Integer> data, String tblName,
        String idxFieldName, String idxName, Comparator<T> comp) {
        T val = getRandom(data.keySet());

        List<List<?>> res = execSql(String.format(SELECT_ORDERED_RANGE_TEMPLATE, tblName, idxName, idxFieldName), val);

        List<Integer> exp = data.entrySet().stream()
            .filter(e -> comp.compare(e.getKey(), val) <= 0)
            .sorted((e1, e2) -> comp.compare(e1.getKey(), e2.getKey()))
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());

        List<Integer> act = res.stream()
            .flatMap(List::stream)
            .map(e -> (Integer)e)
            .collect(Collectors.toList());

        Assert.assertEquals(exp, act);
    }

    /**
     * Verifies that table content equals to generated dataset.
     *
     * @param data Testing dataset.
     * @param tblName Name of the table from which values should be queried.
     * @param idxName Name of the index.
     * @param <T> Java type mapping of the indexed column.
     */
    private <T> void verifyEach(Map<T, Integer> data, String tblName, String idxFieldName, String idxName) {
        for (Map.Entry<T, Integer> entry : data.entrySet()) {
            List<List<?>> res = execSql(
                String.format(SELECT_VALUE_TEMPLATE, tblName, idxName, idxFieldName), entry.getKey()
            );

            Assert.assertFalse("Result should not be empty", res.isEmpty());
            Assert.assertFalse("Result should contain at least one column", res.get(0).isEmpty());
            Assert.assertEquals(entry.getValue(), res.get(0).get(0));
        }
    }

    /**
     * Returns random element from collection.
     *
     * @param col Collection from which random element should be returned.
     * @param <T> Java type mapping of the indexed column.
     * @return Random element from given collection.
     */
    private <T> T getRandom(Collection<T> col) {
        int rndIdx = ThreadLocalRandom.current().nextInt(col.size());

        int i = 0;

        for (T el : col) {
            if (i++ == rndIdx)
                return el;
        }

        return null;
    }

    /**
     * @param qry Query.
     * @param args Args.
     */
    private List<List<?>> execSql(String qry, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(qry).setArgs(args), false).getAll();
    }

    /**
     * Test class for verify index over Java Object.
     */
    static class TestPojo implements Comparable<TestPojo> {
        /** Value. */
        private final int val;

        /** */
        public TestPojo(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull BasicJavaTypesIndexTest.TestPojo o) {
            if (o == null)
                return 1;

            return Integer.compare(val, o.val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestPojo pojo = (TestPojo)o;

            return val == pojo.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(val);
        }
    }

    /**
     * Interface to limit scope of allowed values
     * for {@link #createPopulateAndVerify(Class, Comparator, Class)}
     */
    interface ClassWrapper {
    }

    /**
     * Test class for use like cache key with alternate affinity column.
     *
     * @param <T> Type of the affinity column.
     */
    static class TestKeyWithAff<T> implements ClassWrapper {
        /** Value. */
        private final int val;

        /** Affinity key. */
        @AffinityKeyMapped
        private final T idxVal;

        /** */
        public TestKeyWithAff(int val, T idxVal) {
            this.val = val;
            this.idxVal = idxVal;
        }
    }

    /**
     * Test class for use like cache key with additional indexed column.
     *
     * @param <T> Type of the additional indexed column.
     */
    static class TestKeyWithIdx<T> implements ClassWrapper {
        /** Value. */
        private final int val;

        /** Indexed value. */
        private final T idxVal;

        /** */
        public TestKeyWithIdx(int val, T idxVal) {
            this.val = val;
            this.idxVal = idxVal;
        }
    }
}
