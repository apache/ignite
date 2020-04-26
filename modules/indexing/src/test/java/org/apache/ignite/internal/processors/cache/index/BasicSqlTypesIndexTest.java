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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.GridTestUtils;
import org.h2.util.DateTimeUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.index.BasicSqlTypesIndexTest.IndexType.PK;
import static org.apache.ignite.internal.processors.cache.index.BasicSqlTypesIndexTest.IndexType.SECONDARY_ASC;
import static org.apache.ignite.internal.processors.cache.index.BasicSqlTypesIndexTest.IndexType.SECONDARY_DESC;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.H2TableDescriptor.PK_IDX_NAME;

/**
 * Basic tests for different types of indexed data
 * for tables created through SQL API.
 */
public class BasicSqlTypesIndexTest extends AbstractIndexingCommonTest {
    /** Count of entries that should be preloaded to the table. */
    private static final int DATSET_SIZE = 1_000;

    /** Table ID counter. */
    private static final AtomicInteger TBL_ID = new AtomicInteger();

    /** Template of CREATE TABLE query with pk and value columns. */
    private static final String CREATE_TBL_PK_ONLY_TEMPLATE =
        "CREATE TABLE \"%s\" (idxVal %s PRIMARY KEY, val INT) WITH \"%s\"";

    /** Template of CREATE TABLE query with pk, value and additional column for indexing. */
    private static final String CREATE_TBL_SECONDARY_INDEX_TEMPLATE =
        "CREATE TABLE \"%s\" (id INT PRIMARY KEY, idxVal %s, val INT) WITH \"%s\"";

    /** Template of CREATE INDEX query for additional column. */
    private static final String CREATE_SECONDARY_INDEX_TEMPLATE = "CREATE INDEX \"%s\" ON \"%s\"(idxVal %s)";

    /** Template of SELECT query with filtering by range and ordering by indexed column. */
    private static final String SELECT_ORDERED_RANGE_TEMPLATE =
        "SELECT val FROM \"%s\" USE INDEX(\"%s\") WHERE %s <= ? ORDER BY %s ASC";

    /** Template of SELECT query with filtering by indexed column. */
    private static final String SELECT_VALUE_TEMPLATE =
        "SELECT val, idxVal FROM \"%s\" USE INDEX(\"%s\") WHERE %s = ?";

    /** Template of INSERT query for two-column table. See {@link #CREATE_TBL_PK_ONLY_TEMPLATE}. */
    private static final String INSERT_PK_ONLY_TEMPLATE = "INSERT INTO \"%s\" (idxVal, val) VALUES (?, ?)";

    /** Template of INSERT query for three-column table. See {@link #CREATE_TBL_SECONDARY_INDEX_TEMPLATE}. */
    private static final String INSERT_SECONDARY_TEMPLATE = "INSERT INTO \"%s\" (id, idxVal, val) VALUES (?, ?, ?)";

    /** Template of UPDATE query for updating value by indexed column. */
    private static final String UPDATE_VAL_TEMPLATE = "UPDATE \"%s\" SET val=? WHERE idxVal=?";

    /** Precision for generator of decimal values. */
    private int decimalPrecision;

    /** Max length for generator of string values. */
    private int maxStrLen;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);
    }

    /** */
    @Before
    public void clearState() {
        decimalPrecision = -1;
        maxStrLen = 40;
    }

    /** */
    @Test
    public void testSqlBooleanTypeIndex() {
        String idxTypeStr = "BOOLEAN";
        Class<Boolean> idxCls = Boolean.class;

        createPopulateAndVerify(idxTypeStr, idxCls, Boolean::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Boolean::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, Boolean::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Boolean::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlBigintTypeIndex() {
        String idxTypeStr = "BIGINT";
        Class<Long> idxCls = Long.class;

        createPopulateAndVerify(idxTypeStr, idxCls, Long::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Long::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, Long::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Long::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlDecimalTypeIndex() {
        String idxTypeStr = "DECIMAL";
        Class<BigDecimal> idxCls = BigDecimal.class;

        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlDecimalFixedPrecisionTypeIndex() {
        decimalPrecision = 3;

        String idxTypeStr = "DECIMAL(20,3)";
        Class<BigDecimal> idxCls = BigDecimal.class;

        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlNumericTypeIndex() {
        decimalPrecision = 0;

        String idxTypeStr = "NUMERIC(10)";
        Class<BigDecimal> idxCls = BigDecimal.class;

        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, BigDecimal::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlDoubleTypeIndex() {
        String idxTypeStr = "DOUBLE";
        Class<Double> idxCls = Double.class;

        createPopulateAndVerify(idxTypeStr, idxCls, Double::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Double::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, Double::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Double::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlIntTypeIndex() {
        String idxTypeStr = "INT";
        Class<Integer> idxCls = Integer.class;

        createPopulateAndVerify(idxTypeStr, idxCls, Integer::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Integer::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, Integer::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Integer::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlRealTypeIndex() {
        String idxTypeStr = "REAL";
        Class<Float> idxCls = Float.class;

        createPopulateAndVerify(idxTypeStr, idxCls, Float::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Float::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, Float::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Float::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlTinyintTypeIndex() {
        String idxTypeStr = "TINYINT";
        Class<Byte> idxCls = Byte.class;

        createPopulateAndVerify(idxTypeStr, idxCls, Byte::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Byte::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, Byte::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Byte::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlSmallintTypeIndex() {
        String idxTypeStr = "SMALLINT";
        Class<Short> idxCls = Short.class;

        createPopulateAndVerify(idxTypeStr, idxCls, Short::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Short::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, Short::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Short::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlVarcharTypeIndex() {
        String idxTypeStr = "VARCHAR";
        Class<String> idxCls = String.class;

        createPopulateAndVerify(idxTypeStr, idxCls, String::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, String::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, String::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, String::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlCharTypeIndex() {
        maxStrLen = 10;

        String idxTypeStr = "CHAR(10)";
        Class<String> idxCls = String.class;

        createPopulateAndVerify(idxTypeStr, idxCls, String::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, String::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, String::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, String::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlDateTypeIndex() {
        String idxTypeStr = "DATE";
        Class<Date> idxCls = Date.class;

        // TODO: https://issues.apache.org/jira/browse/IGNITE-8552
//        createPopulateAndVerify(idxTypeStr, idxCls, Date::compareTo, PK, "BACKUPS=1");
//        createPopulateAndVerify(idxTypeStr, idxCls, Date::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, Date::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Date::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlTimeTypeIndex() {
        String idxTypeStr = "TIME";
        Class<Time> idxCls = Time.class;

        Comparator<Time> comp = new Comparator<Time>() {
            @Override public int compare(Time o1, Time o2) {
                GregorianCalendar cal = new GregorianCalendar();

                long l1 = DateTimeUtils.convertTime(o1, cal).getNanos();
                long l2 = DateTimeUtils.convertTime(o2, cal).getNanos();

                return Long.compare(l1, l2);
            }
        };

        createPopulateAndVerify(idxTypeStr, idxCls, comp, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, comp, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, comp, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, comp, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlTimestampTypeIndex() {
        String idxTypeStr = "TIMESTAMP";
        Class<Timestamp> idxCls = Timestamp.class;

        createPopulateAndVerify(idxTypeStr, idxCls, Timestamp::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Timestamp::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, Timestamp::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, Timestamp::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlBynaryTypeIndex() {
        String idxTypeStr = "BINARY";
        Class<byte[]> idxCls = byte[].class;

        Comparator<byte[]> comp = new Comparator<byte[]>() {
            @Override public int compare(byte[] o1, byte[] o2) {
                int res;
                int len = Math.min(o1.length, o2.length);

                for (int i = 0; i < len; i++) {
                    if ((res = Byte.compare(o1[i], o2[i])) != 0)
                        return res;
                }

                return Integer.compare(o1.length, o2.length);
            }
        };

        // TODO: https://issues.apache.org/jira/browse/IGNITE-12313
//        createPopulateAndVerify(idxTypeStr, idxCls, comp, PK, "BACKUPS=1");
//        createPopulateAndVerify(idxTypeStr, idxCls, comp, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, comp, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, comp, SECONDARY_ASC, "BACKUPS=1");
    }

    /** */
    @Test
    public void testSqlUuidTypeIndex() {
        String idxTypeStr = "UUID";
        Class<UUID> idxCls = UUID.class;

        createPopulateAndVerify(idxTypeStr, idxCls, UUID::compareTo, PK, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, UUID::compareTo, PK, "BACKUPS=1,AFFINITY_KEY=idxVal");
        createPopulateAndVerify(idxTypeStr, idxCls, UUID::compareTo, SECONDARY_DESC, "BACKUPS=1");
        createPopulateAndVerify(idxTypeStr, idxCls, UUID::compareTo, SECONDARY_ASC, "BACKUPS=1");
    }

    /**
     * Executes test scenario: <ul>
     *     <li>Create table</li>
     *     <li>Create necessary indexes</li>
     *     <li>Populate table with random data</li>
     *     <li>Verify range query on created table</li>
     *     <li>Verify that table stores the same data as the generated dataset</li>
     *     <li>Drop table</li>
     * </ul>
     *
     *
     * @param idxTypeStr Index type in string (e.g. 'INT').
     * @param idxTypeCls Index type class.
     * @param idxType Whether index should be primary key or not.
     * @param benefits Benefits will be used for table creation
     * (i.e. {@code CREATE TABLE friends() WITH "<benefits>"}).
     * @param <T> Java type mapping of the indexed column.
     */
    private <T> void createPopulateAndVerify(String idxTypeStr, Class<T> idxTypeCls, Comparator<T> comp,
        IndexType idxType, @Nullable String benefits) {
        String tblName = idxTypeStr + "_TBL" + TBL_ID.incrementAndGet();

        try {
            String createTblSql = String.format(
                idxType == PK ? CREATE_TBL_PK_ONLY_TEMPLATE : CREATE_TBL_SECONDARY_INDEX_TEMPLATE,
                tblName, idxTypeStr, benefits != null ? benefits : ""
            );

            execSql(createTblSql);

            String idxName;
            String idxFieldName;

            if (idxType != PK) {
                idxName = "idxVal_idx";
                idxFieldName = "idxVal";

                execSql(String.format(CREATE_SECONDARY_INDEX_TEMPLATE, idxName, tblName,
                    idxType == SECONDARY_ASC ? "ASC" : "DESC"));
            }
            else {
                idxName = PK_IDX_NAME;
                idxFieldName = KEY_FIELD_NAME;
            }

            Map<T, Integer> data = new TreeMap<>(comp);

            populateTable(data, tblName, idxTypeCls, idxType == PK);

            verifyRange(data, tblName, idxFieldName, idxName, comp);
            verifyEach(data, tblName, idxFieldName, idxName);
        }
        finally {
            execSql("DROP TABLE IF EXISTS \"" + tblName + "\"");
        }
    }

    /**
     * Populate table with random data.
     *
     * @param data Map which will be used to store all generated data.
     * @param tblName Table name that should be populated.
     * @param idxValCls Class of indexed value. Used for generating random value
     * of the required type.
     * @param pk Whether indexed value is primary key or not.
     * @param <T> Java type mapping of the indexed column.
     */
    private <T> void populateTable(Map<T, Integer> data, String tblName, Class<T> idxValCls, boolean pk) {
        for (int i = 0; i < DATSET_SIZE; i++) {
            int id = nextVal(Integer.class);
            T idxVal = nextVal(idxValCls);
            int val = nextVal(Integer.class);

            if (data.put(idxVal, val) == null) {
                if (pk)
                    execSql(String.format(INSERT_PK_ONLY_TEMPLATE, tblName), idxVal, val);
                else
                    execSql(String.format(INSERT_SECONDARY_TEMPLATE, tblName), id, idxVal, val);
            }
            else
                execSql(String.format(UPDATE_VAL_TEMPLATE, tblName), val, idxVal);
        }
    }

    /**
     * Generates random value for the given class.
     *
     * @param cls Class of the required value.
     * @param <T> Java type mapping of the indexed column.
     *
     * @return Generated value.
     */
    private <T> T nextVal(Class<T> cls) {
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

        if (cls.isAssignableFrom(BigDecimal.class)) {
            BigDecimal bd = new BigDecimal(ThreadLocalRandom.current().nextDouble());

            if (decimalPrecision >= 0)
                bd = bd.setScale(decimalPrecision, RoundingMode.HALF_UP);

            return cls.cast(bd);
        }

        if (cls.isAssignableFrom(String.class))
            return cls.cast(GridTestUtils.randomString(ThreadLocalRandom.current(), 1, maxStrLen));

        if (cls.isAssignableFrom(Date.class))
            return cls.cast(new Date(ThreadLocalRandom.current().nextLong()));

        if (cls.isAssignableFrom(Time.class))
            return cls.cast(new Time(ThreadLocalRandom.current().nextLong()));

        if (cls.isAssignableFrom(Timestamp.class))
            return cls.cast(new Timestamp(ThreadLocalRandom.current().nextLong()));

        if (cls.isAssignableFrom(UUID.class))
            return cls.cast(new UUID(
                ThreadLocalRandom.current().nextLong(),
                ThreadLocalRandom.current().nextLong()
            ));

        if (cls.isAssignableFrom(byte[].class))
            return cls.cast(GridTestUtils.randomString(ThreadLocalRandom.current(), 1, maxStrLen).getBytes());

        throw new IllegalStateException("There is no generator for class=" + cls.getSimpleName());
    }

    /**
     * Verifies range querying.
     *
     * @param data Testing dataset.
     * @param tblName Name of the table from which values should be queried.
     * @param idxFieldName Name of the indexed field.
     * @param idxName Name of the index.
     * @param comp Comparator.
     * @param <T> Java type mapping of the indexed column.
     */
    private <T> void verifyRange(Map<T, Integer> data, String tblName,
        String idxFieldName, String idxName, Comparator<T> comp) {
        T val = getRandom(data.keySet());

        List<List<?>> res = execSql(String.format(SELECT_ORDERED_RANGE_TEMPLATE, tblName, idxName, idxFieldName, idxFieldName), val);

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
     * @param idxFieldName Name of the indexed field.
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
     *
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

    /** Type of the index to create. */
    enum IndexType {
        /** Primary key. */
        PK,

        /** Secondary index with ascending sorting. */
        SECONDARY_ASC,

        /** Secondary index with descending sorting. */
        SECONDARY_DESC
    }
}
