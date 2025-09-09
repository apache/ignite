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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Function;
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

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gte;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.in;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

/** */
@RunWith(Parameterized.class)
public class IndexQueryAllTypesTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static IgniteCache<Long, Person> cache;

    /** Whether to specify index name in IndexQuery. */
    @Parameterized.Parameter
    public boolean useIdxName;

    /** */
    @Parameterized.Parameters(name = "useIdxName={0}")
    public static List<Boolean> params() {
        return F.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite crd = startGrids(2);

        cache = crd.cache(CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache.clear();
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

    /** */
    @Test
    public void testRangeWithNulls() {
        Function<Integer, Person> persGen = i -> {
            Integer val = i < CNT / 10 ? null : i;

            return person("intNullId", val);
        };

        insertData(i -> i, persGen, CNT);

        int pivot = CNT / 5;

        String intNullIdx = idxName("intNullId", false);

        // Should include all.
        IndexQuery<Long, Person> qry = new IndexQuery<>(Person.class, intNullIdx);

        check(cache.query(qry), 0, CNT, i -> i, persGen);

        // Should include nulls.
        qry = new IndexQuery<Long, Person>(Person.class, intNullIdx)
            .setCriteria(lt("intNullId", pivot));

        check(cache.query(qry), 0, CNT / 5, i -> i, persGen);

        // Should exclude nulls.
        qry = new IndexQuery<Long, Person>(Person.class, intNullIdx)
            .setCriteria(gte("intNullId", 0));

        check(cache.query(qry), CNT / 10, CNT, i -> i, persGen);

        // Should return only nulls.
        qry = new IndexQuery<Long, Person>(Person.class, intNullIdx)
            .setCriteria(lt("intNullId", 0));

        check(cache.query(qry), 0, CNT / 10, i -> i, persGen);

        // Should return only nulls.
        qry = new IndexQuery<Long, Person>(Person.class, intNullIdx)
            .setCriteria(lte("intNullId", null));

        check(cache.query(qry), 0, CNT / 10, i -> i, persGen);

        // Should return all non nulls.
        qry = new IndexQuery<Long, Person>(Person.class, intNullIdx)
            .setCriteria(gt("intNullId", null));

        check(cache.query(qry), CNT / 10, CNT, i -> i, persGen);

        // Should return all items.
        qry = new IndexQuery<Long, Person>(Person.class, intNullIdx)
            .setCriteria(gte("intNullId", null));

        check(cache.query(qry), 0, CNT, i -> i, persGen);

        // Should return only nulls.
        qry = new IndexQuery<Long, Person>(Person.class, intNullIdx)
            .setCriteria(in("intNullId", Collections.singleton(null)));

        check(cache.query(qry), 0, CNT / 10, i -> i, persGen);
    }

    /** */
    @Test
    public void testRangeByteField() {
        testRangeField(Integer::byteValue, "byteId", Byte.MAX_VALUE);
    }

    /** */
    @Test
    public void testInByteField() {
        testInSecondField(Integer::byteValue, "byteId", Byte.MAX_VALUE);
    }

    /** */
    @Test
    public void testRangeShortField() {
        testRangeField(Integer::shortValue, "shortId");
    }

    /** */
    @Test
    public void testInShortField() {
        testInSecondField(Integer::shortValue, "shortId");
    }

    /** */
    @Test
    public void testRangeIntField() {
        testRangeField(i -> i, "intId");
    }

    /** */
    @Test
    public void testRangeLongField() {
        testRangeField(Integer::longValue, "longId");
    }

    /** */
    @Test
    public void testInLongField() {
        testInSecondField(Integer::longValue, "longId");
    }

    /** */
    @Test
    public void testRangeDecimalField() {
        testRangeField(BigDecimal::valueOf, "decimalId");
    }

    /** */
    @Test
    public void testInDecimalField() {
        testInSecondField(BigDecimal::valueOf, "decimalId");
    }

    /** */
    @Test
    public void testRangeDoubleField() {
        testRangeField(Integer::doubleValue, "doubleId");
    }

    /** */
    @Test
    public void testInDoubleField() {
        testInSecondField(Integer::doubleValue, "doubleId");
    }

    /** */
    @Test
    public void testRangeFloatField() {
        testRangeField(Integer::floatValue, "floatId");
    }

    /** */
    @Test
    public void testInFloatField() {
        testInSecondField(Integer::floatValue, "floatId");
    }

    /** */
    @Test
    public void testRangeTimeField() {
        testRangeField(i -> new Time(i.longValue()), "timeId");
    }

    /** */
    @Test
    public void testInTimeField() {
        testInSecondField(i -> new Time(i.longValue()), "timeId");
    }

    /** */
    @Test
    public void testRangeDateField() {
        testRangeField(i -> new java.util.Date(i.longValue()), "dateId");
    }

    /** */
    @Test
    public void testInDateField() {
        testInSecondField(i -> new java.util.Date(i.longValue()), "dateId");
    }

    /** */
    @Test
    public void testRangePojoField() {
        testRangeField(PojoField::new, "pojoId");
    }

    /** */
    @Test
    public void testInPojoField() {
        testInSecondField(PojoField::new, "pojoId");
    }

    /** */
    @Test
    public void testRangeSqlDateField() {
        long dayMs = 24 * 60 * 60 * 1_000;

        testRangeField(i -> new java.sql.Date(dayMs * i), "sqlDateId");
    }

    /** */
    @Test
    public void testRangeTimestampField() {
        testRangeField(i -> new Timestamp(i.longValue()), "timestampId");
    }

    /** */
    @Test
    public void testInTimestampField() {
        testInSecondField(i -> new Timestamp(i.longValue()), "timestampId");
    }

    /** */
    @Test
    public void testRangeBytesField() {
        testRangeField(i -> ByteBuffer.allocate(4).putInt(i).array(), "bytesId", 4);
    }

    /** */
    @Test
    public void testInBytesField() {
        testInSecondField(i -> ByteBuffer.allocate(4).putInt(i).array(), "bytesId", 4);
    }

    /** */
    @Test
    public void testRangeUuidField() {
        testRangeField(i -> {
            String formatted = String.format("%04d", i);
            String uuid = "2af83a15-" + formatted + "-4c13-871d-b14f0d37fe2e";

            return UUID.fromString(uuid);

        }, "uuidId");
    }

    /** */
    @Test
    public void testInUuidField() {
        testInSecondField(i -> {
            String formatted = String.format("%04d", i);
            String uuid = "2af83a15-" + formatted + "-4c13-871d-b14f0d37fe2e";

            return UUID.fromString(uuid);

        }, "uuidId");
    }

    /** */
    @Test
    public void testRangeStringField() {
        testRangeField(i -> String.format("%04d", i), "strId");
    }

    /** */
    @Test
    public void testInStringField() {
        testInSecondField(i -> String.format("%04d", i), "strId");
    }

    /** Also checks duplicate indexed values. */
    @Test
    public void testBoolField() {
        Function<Integer, Boolean> valGen = i -> i > CNT / 2;

        Function<Boolean, Person> persGen = i -> person("boolId", i);

        insertData(valGen, persGen, CNT);

        String boolIdx = idxName("boolId", false);

        IndexQuery<Long, Person> qry = new IndexQuery<>(Person.class, boolIdx);

        // All.
        check(cache.query(qry), 0, CNT, valGen, persGen);

        // Eq true.
        qry = new IndexQuery<Long, Person>(Person.class, boolIdx)
            .setCriteria(eq("boolId", true));

        check(cache.query(qry), CNT / 2 + 1, CNT, valGen, persGen);

        // Eq false.
        qry = new IndexQuery<Long, Person>(Person.class, boolIdx)
            .setCriteria(eq("boolId", false));

        check(cache.query(qry), 0, CNT / 2 + 1, valGen, persGen);

        // In.
        qry = new IndexQuery<Long, Person>(Person.class, boolIdx)
            .setCriteria(in("boolId", Collections.singleton(false)));

        check(cache.query(qry), 0, CNT / 2 + 1, valGen, persGen);
    }

    /** */
    @Test
    public void testRangeCrossTypeConvertion() {
        int cnt = 100;

        Map<String, Function<Integer, Object>> funcs = new HashMap<>();

        funcs.put("byte", Integer::byteValue);
        funcs.put("short", Integer::shortValue);
        funcs.put("int", Integer::intValue);
        funcs.put("long", Integer::longValue);
        funcs.put("float", Integer::floatValue);
        funcs.put("double", Integer::doubleValue);
        funcs.put("decimal", BigDecimal::valueOf);

        for (Map.Entry<String, Function<Integer, Object>> cacheRow : funcs.entrySet()) {
            for (Map.Entry<String, Function<Integer, Object>> searchRow : funcs.entrySet()) {
                log.info("Checking " + cacheRow.getKey() + " cache row type with " + searchRow.getKey() +
                    " search row type");

                testRangeField(cacheRow.getValue(), searchRow.getValue(), cacheRow.getKey() + "Id", cnt);
            }
        }

        funcs.clear();

        Calendar cal = Calendar.getInstance(TimeZone.getDefault());

        funcs.put("sqlDate", i -> {
            cal.clear();
            cal.set(2000 + i / 100, (i / 10) % 10, i % 10);
            return new java.sql.Date(cal.getTimeInMillis());
        });
        funcs.put("timestamp", i -> {
            cal.clear();
            cal.set(2000 + i / 100, (i / 10) % 10, i % 10);
            return new Timestamp(cal.getTimeInMillis());
        });

        for (Map.Entry<String, Function<Integer, Object>> cacheRow : funcs.entrySet()) {
            for (Map.Entry<String, Function<Integer, Object>> searchRow : funcs.entrySet()) {
                log.info("Checking " + cacheRow.getKey() + " cache row type with " + searchRow.getKey() +
                    " search row type");

                testRangeField(cacheRow.getValue(), searchRow.getValue(), cacheRow.getKey() + "Id", cnt);
            }
        }
    }

    /** */
    @Test
    public void testCrossTypeComparisonWithOutOfBoundsFilter() {
        int cnt = 100;

        Map<String, Function<Integer, Object>> funcs = new HashMap<>();

        funcs.put("bool", i -> i != 0);
        funcs.put("byte", Integer::byteValue);

        for (Map.Entry<String, Function<Integer, Object>> func : funcs.entrySet()) {
            String fieldName = func.getKey() + "Id";

            Function<Object, Person> persGen = i -> person(fieldName, i);

            insertData(func.getValue(), persGen, cnt);

            for (Object filterVal : F.asList(1_000, 1_000L, 1_000.0, 1_000.0d, BigDecimal.valueOf(1_000))) {
                log.info("Checking " + func.getKey() + " cache row type with " +
                    filterVal.getClass().getSimpleName() + " search row type");

                IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, idxName(fieldName, false))
                    .setCriteria(lte(fieldName, filterVal));

                check(cache.query(qry), 0, cnt, func.getValue(), persGen);

                qry = new IndexQuery<Long, Person>(Person.class, idxName(fieldName, false))
                    .setCriteria(gt(fieldName, filterVal));

                assertTrue(cache.query(qry).getAll().isEmpty());
            }
        }
    }

    /** */
    private <T> void testRangeField(Function<Integer, T> valGen, String fieldName) {
        testRangeField(valGen, fieldName, CNT);
    }

    /** */
    private <T> void testRangeField(Function<Integer, T> valGen, String fieldName, int cnt) {
        testRangeField(valGen, valGen, fieldName, cnt);
    }

    /** */
    private <T, S> void testRangeField(
        Function<Integer, T> valGen,
        Function<Integer, S> searchRowGen,
        String fieldName,
        int cnt
    ) {
        Function<T, Person> persGen = i -> person(fieldName, i);

        insertData(valGen, persGen, cnt);

        int pivot = new Random().nextInt(cnt);

        S val = searchRowGen.apply(pivot);

        // All.
        IndexQuery<Long, Person> qry = new IndexQuery<>(Person.class, idxName(fieldName, false));

        check(cache.query(qry), 0, cnt, valGen, persGen);

        // Lt.
        qry = new IndexQuery<Long, Person>(Person.class, idxName(fieldName, false))
            .setCriteria(lt(fieldName, val));

        check(cache.query(qry), 0, pivot, valGen, persGen);

        // Lte.
        qry = new IndexQuery<Long, Person>(Person.class, idxName(fieldName, false))
            .setCriteria(lte(fieldName, val));

        check(cache.query(qry), 0, pivot + 1, valGen, persGen);

        // In.
        qry = new IndexQuery<Long, Person>(Person.class, idxName(fieldName, false))
            .setCriteria(in(fieldName, Collections.singleton(val)));

        check(cache.query(qry), pivot, pivot + 1, valGen, persGen);
    }

    /** */
    private <T, S> void testInSecondField(Function<Integer, T> valGen, String fieldName) {
        testInSecondField(valGen, valGen, fieldName, CNT);
    }

    /** */
    private <T, S> void testInSecondField(Function<Integer, T> valGen, String fieldName, int cnt) {
        testInSecondField(valGen, valGen, fieldName, cnt);
    }

    /** */
    private <T, S> void testInSecondField(
        Function<Integer, T> valGen,
        Function<Integer, S> searchRowGen,
        String fieldName,
        int cnt
    ) {
        Function<T, Person> persGen = i -> {
            Person p = person(fieldName, i);

            p.intId = 0;

            return p;
        };

        insertData(valGen, persGen, cnt);

        cache.query(new SqlFieldsQuery(
            "create index if not exists IDX_intid_" + fieldName + " on Person(intId, " + fieldName + ");"
        )).getAll();

        int pivot = new Random().nextInt(cnt);

        S val = searchRowGen.apply(pivot);

        // In.
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, idxName(fieldName, true))
            .setCriteria(eq("intId", 0), in(fieldName, Collections.singleton(val)));

        check(cache.query(qry), pivot, pivot + 1, valGen, persGen);
    }

    /** */
    private String idxName(String field, boolean inTest) {
        if (!useIdxName)
            return null;

        return inTest ? "IDX_intid_" + field : ("Person_" + field + "_idx").toUpperCase();
    }

    /** */
    private Person person(String field, Object val) {
        try {
            Field f = Person.class.getField(field);

            Person p = new Person();
            f.set(p, val);

            return p;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param left  First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private <T> void check(QueryCursor<Cache.Entry<Long, Person>> cursor, int left, int right,
        Function<Integer, T> valGen, Function<T, Person> persGen) {

        List<Cache.Entry<Long, Person>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        Set<Long> expKeys = LongStream.range(left, right).boxed().collect(Collectors.toSet());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Long, Person> entry = all.get(i);

            assertTrue(expKeys.remove(entry.getKey()));

            assertEquals(persGen.apply(valGen.apply(left + i)), all.get(i).getValue());
        }

        assertTrue(expKeys.isEmpty());
    }

    /** */
    private <T> void insertData(Function<Integer, T> valGen, Function<T, Person> persGen, int cnt) {
        for (int i = 0; i < cnt; i++)
            cache.put((long)i, persGen.apply(valGen.apply(i)));
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        public boolean boolId;

        /** */
        @QuerySqlField(index = true)
        public byte byteId;

        /** */
        @QuerySqlField(index = true)
        public short shortId;

        /** */
        @QuerySqlField(index = true)
        public int intId;

        /** */
        @QuerySqlField(index = true)
        public long longId;

        /** */
        @QuerySqlField(index = true)
        public BigDecimal decimalId;

        /** */
        @QuerySqlField(index = true)
        public double doubleId;

        /** */
        @QuerySqlField(index = true)
        public float floatId;

        /** */
        @QuerySqlField(index = true)
        public Time timeId;

        /** */
        @QuerySqlField(index = true)
        public java.util.Date dateId;

        /** */
        @QuerySqlField(index = true)
        public java.sql.Date sqlDateId;

        /** */
        @QuerySqlField(index = true)
        public Timestamp timestampId;

        /** */
        @QuerySqlField(index = true)
        public byte[] bytesId;

        /** */
        @QuerySqlField(index = true)
        public String strId;

        /** */
        @QuerySqlField(index = true)
        public PojoField pojoId;

        /** */
        @QuerySqlField(index = true)
        public UUID uuidId;

        /** */
        @QuerySqlField(index = true)
        public Integer intNullId;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return boolId == person.boolId
                && byteId == person.byteId
                && shortId == person.shortId
                && intId == person.intId
                && longId == person.longId
                && Double.compare(person.doubleId, doubleId) == 0
                && Float.compare(person.floatId, floatId) == 0
                && Objects.equals(decimalId, person.decimalId)
                && Objects.equals(timeId, person.timeId)
                && Objects.equals(dateId, person.dateId)
                && Objects.equals(sqlDateId, person.sqlDateId)
                && Objects.equals(timestampId, person.timestampId)
                && Arrays.equals(bytesId, person.bytesId)
                && Objects.equals(strId, person.strId)
                && Objects.equals(pojoId, person.pojoId)
                && Objects.equals(uuidId, person.uuidId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(boolId, byteId, shortId, intId, longId, decimalId,
                doubleId, floatId, timeId, dateId, sqlDateId, timestampId, strId, pojoId, uuidId);

            result = 31 * result + Arrays.hashCode(bytesId);
            return result;
        }
    }

    /** */
    public static class PojoField implements Serializable {
        /** */
        private int intVal;

        /** */
        private String strVal;

        /** */
        private Timestamp tsVal;

        /** */
        PojoField(int i) {
            intVal = i;
            strVal = String.valueOf(i);
            tsVal = new Timestamp(i);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(intVal, strVal, tsVal);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object other) {
            if (this == other)
                return true;

            if (other == null || getClass() != other.getClass())
                return false;

            PojoField pojo = (PojoField)other;

            return intVal == pojo.intVal
                && Objects.equals(strVal, pojo.strVal)
                && Objects.equals(tsVal, pojo.tsVal);
        }

        /** Enable comparison of PojoField objects by fields. */
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeInt(intVal);

            out.writeInt(strVal.length());
            for (int i = 0; i < strVal.length(); i++)
                out.writeChar(strVal.charAt(i));

            out.writeObject(tsVal);
        }

        /** */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            intVal = in.readInt();

            int strLen = in.readInt();

            StringBuilder bld = new StringBuilder();
            for (int i = 0; i < strLen; i++)
                bld.append(in.readChar());

            strVal = bld.toString();

            tsVal = (Timestamp)in.readObject();
        }
    }
}
