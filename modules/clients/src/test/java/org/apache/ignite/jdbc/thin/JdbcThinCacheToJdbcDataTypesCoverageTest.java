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

package org.apache.ignite.jdbc.thin;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheDataTypesCoverageTest;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Data types coverage for basic cache-put-jdbc-thin-retrieve operations.
 */
public class JdbcThinCacheToJdbcDataTypesCoverageTest extends GridCacheDataTypesCoverageTest {
    /** Signals that tests should start in affinity awareness mode. */
    public static boolean affinityAwareness;

    /**
     * Please pay attention, that it's not a comprehensive java to sql data types mapping,
     * but a holder of specific mappings for given test only.
     *
     * Based on:
     * https://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
     */
    private static final Map<Class, IgniteBiTuple<Integer, Class>> javaClsToSqlTypeMap;

    static {
        Map<Class, IgniteBiTuple<Integer, Class>> innerMap = new HashMap<>();

        innerMap.put(Boolean.class,             new IgniteBiTuple<>(Types.BOOLEAN,      Boolean.class));
        innerMap.put(Integer.class,             new IgniteBiTuple<>(Types.INTEGER,      Integer.class));
        innerMap.put(Byte.class,                new IgniteBiTuple<>(Types.TINYINT,      Byte.class));
        innerMap.put(Short.class,               new IgniteBiTuple<>(Types.SMALLINT,     Short.class));
        innerMap.put(Long.class,                new IgniteBiTuple<>(Types.BIGINT,       Long.class));
        innerMap.put(BigDecimal.class,          new IgniteBiTuple<>(Types.DECIMAL,      BigDecimal.class));
        innerMap.put(Double.class,              new IgniteBiTuple<>(Types.DOUBLE,       Double.class));
        innerMap.put(Float.class,               new IgniteBiTuple<>(Types.FLOAT,        Float.class));
        innerMap.put(java.sql.Time.class,       new IgniteBiTuple<>(Types.TIME,         java.sql.Time.class));
        innerMap.put(java.sql.Date.class,       new IgniteBiTuple<>(Types.DATE,         java.sql.Date.class));
        innerMap.put(java.sql.Timestamp.class,  new IgniteBiTuple<>(Types.TIMESTAMP,    java.sql.Timestamp.class));
        innerMap.put(String.class,              new IgniteBiTuple<>(Types.VARCHAR,      String.class));
        innerMap.put(Character.class,           new IgniteBiTuple<>(Types.OTHER,        Character.class));
        innerMap.put(byte[].class,              new IgniteBiTuple<>(Types.BINARY,       byte[].class));
        innerMap.put(java.util.Date.class,      new IgniteBiTuple<>(Types.TIMESTAMP,    java.sql.Timestamp.class));
        innerMap.put(LocalDate.class,           new IgniteBiTuple<>(Types.DATE,         java.sql.Date.class));
        innerMap.put(LocalDateTime.class,       new IgniteBiTuple<>(Types.TIMESTAMP,    java.sql.Timestamp.class));
        innerMap.put(LocalTime.class,           new IgniteBiTuple<>(Types.TIME,         java.sql.Time.class));

        javaClsToSqlTypeMap = Collections.unmodifiableMap(innerMap);
    }

    /** As stmt parameter. */
    private boolean asPreparedParam;

    /** URL. */
    private String url = affinityAwareness ?
        "jdbc:ignite:thin://127.0.0.1:10800..10802?affinityAwareness=true" :
        "jdbc:ignite:thin://127.0.0.1?affinityAwareness=false";

    /** Connection. */
    private Connection conn;

    /** Expected ex. */
    @Rule
    public ExpectedException expEx = ExpectedException.none();



    /** @inheritDoc */
    @Before
    @Override public void init() throws Exception {
        super.init();

        asPreparedParam = false;
    }

    /**
     * Cleanup.
     *
     * @throws Exception If Failed.
     */
    @After
    public void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();

            assert conn.isClosed();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testFloatDataType() throws Exception {
        checkBasicCacheOperations(
            Float.MIN_VALUE,
            Float.MAX_VALUE,
            new Quoted(Float.NaN),
            new Quoted(Float.NEGATIVE_INFINITY),
            new Quoted(Float.POSITIVE_INFINITY),
            0F,
            0.0F,
            1F,
            1.1F);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testDoubleDataType() throws Exception {
        checkBasicCacheOperations(
            Double.MIN_VALUE,
            Double.MAX_VALUE,
            new Quoted(Double.NaN),
            new Quoted(Double.NEGATIVE_INFINITY),
            new Quoted(Double.POSITIVE_INFINITY),
            0D,
            0.0D,
            1D,
            1.1D);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13231")
    @Test
    @Override public void testCharacterDataType() throws Exception {
        checkBasicCacheOperations(
            new Quoted('a'),
            new Quoted('A'));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testStringDataType() throws Exception {
        checkBasicCacheOperations(
            new Quoted("aAbB"),
            new Quoted(""));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    @Test
    @Override public void testByteArrayDataType() throws Exception {
        checkBasicCacheOperations(
            new ByteArrayed(new byte[] {}),
            new ByteArrayed(new byte[] {1, 2, 3}),
            new ByteArrayed(new byte[] {3, 2, 1}));
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13232")
    @Test
    @Override public void testObjectArrayDataType() throws Exception {
        super.testObjectArrayDataType();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testListDataType() throws Exception {
        asPreparedParam = true;

        checkBasicCacheOperations(new ArrayList<>());
        checkBasicCacheOperations((Serializable)Collections.singletonList("Aaa"));
        checkBasicCacheOperations((Serializable)Arrays.asList("String", Boolean.TRUE, 'A', 1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testSetDataType() throws Exception {
        asPreparedParam = true;

        checkBasicCacheOperations(new HashSet<>());
        checkBasicCacheOperations((Serializable)Collections.singleton("Aaa"));
        checkBasicCacheOperations((Serializable)Arrays.asList("String", Boolean.TRUE, 'A', 1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testObjectBasedOnPrimitivesDataType() throws Exception {
        asPreparedParam = true;

        super.testObjectBasedOnPrimitivesDataType();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testObjectBasedOnPrimitivesAndCollectionsDataType() throws Exception {
        asPreparedParam = true;

        super.testObjectBasedOnPrimitivesAndCollectionsDataType();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testObjectBasedOnPrimitivesAndCollectionsAndNestedObjectsDataType() throws Exception {
        asPreparedParam = true;

        super.testObjectBasedOnPrimitivesAndCollectionsAndNestedObjectsDataType();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13233")
    @Test
    @Override public void testDateDataType() throws Exception {
        checkBasicCacheOperations(
            (Object d) -> new Timestamp(((java.util.Date)d).getTime()),
            new Dated(new java.util.Date()),
            new Dated(new java.util.Date(Long.MIN_VALUE)),
            new Dated(new java.util.Date(Long.MAX_VALUE)));
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12326")
    @Test
    @Override public void testSqlDateDataType() throws Exception {
        super.testSqlDateDataType();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testInstantDataType() throws Exception {
        asPreparedParam = true;

        super.testInstantDataType();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testCalendarDataType() throws Exception {
        asPreparedParam = true;

        super.testCalendarDataType();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12312, https://issues.apache.org/jira/browse/IGNITE-12326")
    @Test
    @Override public void testLocalDateDataType() throws Exception {
        checkBasicCacheOperations(
            (Object ld) -> java.sql.Date.valueOf((LocalDate)ld),
            new Dated(LocalDate.of(2015, 2, 20)),
            new Dated(LocalDate.now().plusDays(1)));
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12312")
    @Test
    @Override public void testLocalDateTimeDataType() throws Exception {
        checkBasicCacheOperations(
            (Object ldt) -> Timestamp.valueOf((LocalDateTime)ldt),
            new Dated(LocalDateTime.of(2015, 2, 20, 9, 4, 30)),
            new Dated(LocalDateTime.now().plusDays(1)));
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12312")
    @Test
    @Override public void testLocalTimeDataType() throws Exception {
        checkBasicCacheOperations(
            (Object lt) -> Time.valueOf((LocalTime)lt),
            new Timed(LocalTime.of(9, 4, 40)),
            new Timed(LocalTime.now()));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSQLTimestampDataType() throws Exception {
        checkBasicCacheOperations(
            new Dated(Timestamp.valueOf(LocalDateTime.now()), "yyyy-MM-dd HH:mm:ss.SSS"),
            new Dated(Timestamp.valueOf(LocalDateTime.now()), "yyyy-MM-dd HH:mm:ss.SSSS"),
            new Dated(Timestamp.valueOf(LocalDateTime.now()), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testBigIntegerDataType() throws Exception {
        asPreparedParam = true;

        super.testBigIntegerDataType();
    }

    /**
     * Verification that jdbc thin SELECT and SELECT with where clause works correctly
     * in context of cache to jdbc data types convertion.
     *
     * @param valsToCheck Array of values to check.
     * @throws Exception If failed.
     */
    @Override protected void checkBasicCacheOperations(Serializable... valsToCheck) throws Exception {
        checkBasicCacheOperations((Object o) -> o, valsToCheck);
    }

    /**
     * Verification that jdbc thin SELECT and SELECT with where clause works correctly
     * in context of cache to jdbc data types convertion.
     *
     * @param converterToSqlExpVal Converter to expected value.
     * @param valsToCheck Array of values to check.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkBasicCacheOperations(Function converterToSqlExpVal, Serializable... valsToCheck) throws Exception {
        assert valsToCheck.length > 0;

        Object originalValItem = valsToCheck[0] instanceof SqlStrConvertedValHolder ?
            ((SqlStrConvertedValHolder)valsToCheck[0]).originalVal() :
            valsToCheck[0];

        // In case of BigDecimal, cache internally changes bitLength of BigDecimal's intValue,
        // so that EqualsBuilder.reflectionEquals returns false.
        // As a result in case of BigDecimal data type Objects.equals is used.
        // Same is about BigInteger.
        BiFunction<Object, Object, Boolean> equalsProcessor =
            originalValItem instanceof BigDecimal || originalValItem instanceof BigInteger ?
                Objects::equals :
                (lhs, rhs) -> EqualsBuilder.reflectionEquals(
                    lhs, rhs, false, lhs.getClass(), true);

        String uuidPostfix = UUID.randomUUID().toString().replaceAll("-", "_");

        String cacheName = "cache" + uuidPostfix;

        String tblName = "table" + uuidPostfix;

        Class<?> dataType = originalValItem.getClass();

        IgniteEx ignite =
            (cacheMode == CacheMode.LOCAL || writeSyncMode == CacheWriteSynchronizationMode.PRIMARY_SYNC) ?
                grid(0) :
                grid(new Random().nextInt(NODES_CNT));

        IgniteCache<Object, Object> cache = ignite.createCache(
            new CacheConfiguration<>()
                .setName(cacheName)
                .setAtomicityMode(atomicityMode)
                .setCacheMode(cacheMode)
                .setExpiryPolicyFactory(ttlFactory)
                .setBackups(backups)
                .setEvictionPolicyFactory(evictionFactory)
                .setOnheapCacheEnabled(evictionFactory != null || onheapCacheEnabled)
                .setWriteSynchronizationMode(writeSyncMode)
                .setAffinity(new RendezvousAffinityFunction(false, PARTITIONS_CNT))
                .setQueryEntities(Collections.singletonList(
                    new QueryEntity(dataType, dataType).setTableName(tblName))));

        Map<Serializable, Serializable> keyValMap = new HashMap<>();

        for (int i = 0; i < valsToCheck.length; i++)
            keyValMap.put(valsToCheck[i], valsToCheck[valsToCheck.length - i - 1]);

        for (Map.Entry<Serializable, Serializable> keyValEntry : keyValMap.entrySet()) {
            Object originalKey;
            Object sqlStrKey;

            if (keyValEntry.getKey() instanceof SqlStrConvertedValHolder) {
                originalKey = ((SqlStrConvertedValHolder) keyValEntry.getKey()).originalVal();
                sqlStrKey = ((SqlStrConvertedValHolder) keyValEntry.getKey()).sqlStrVal();
            }
            else {
                originalKey = keyValEntry.getKey();
                sqlStrKey = keyValEntry.getKey();
            }

            Object originalVal = keyValEntry.getValue() instanceof SqlStrConvertedValHolder ?
                ((SqlStrConvertedValHolder)keyValEntry.getValue()).originalVal() :
                keyValEntry.getValue();

            // Populate cache with data.
            cache.put(originalKey, originalVal);

            // Check SELECT query.
            try (PreparedStatement stmt = prepareStatement(cacheName, "SELECT * FROM " + tblName)) {
                checkQuery(converterToSqlExpVal, equalsProcessor, originalVal.getClass(), originalKey, originalVal, stmt);
            }

            // Check SELECT query with where clause.
            if (asPreparedParam) {
                try (PreparedStatement stmt = prepareStatement(cacheName, "SELECT * FROM " + tblName + " WHERE _key = ?")) {
                    if (originalKey.getClass().isArray())
                        stmt.setArray(1, conn.createArrayOf("OTHER", (Object[])originalKey));
                    else
                        stmt.setObject(1, originalKey);

                    checkQuery(converterToSqlExpVal, equalsProcessor, originalVal.getClass(), originalKey, originalVal, stmt);
                }
            }
            else {
                try (PreparedStatement stmt = prepareStatement(cacheName, "SELECT * FROM " + tblName
                    + " WHERE _key = " + sqlStrKey)) {
                    checkQuery(converterToSqlExpVal, equalsProcessor, originalVal.getClass(), originalKey, originalVal, stmt);
                }
            }

            // Check DELETE.
            checkDelete(tblName, cache, originalKey);

        }
    }

    /**
     * Prepare jdbc thin connection and statement.
     *
     * @param cacheName Cache name.
     * @throws SQLException If Failed.
     */
    private PreparedStatement prepareStatement(String cacheName, String qry) throws SQLException {
        conn = DriverManager.getConnection(url);

        conn.setSchema('"' + cacheName + '"');

        return conn.prepareStatement(qry);
    }

    /**
     * Remove record form cache via cache API, and verify that data was successfully removed with sql jdbc query.
     *
     * @param tblName Table name.
     * @param cache Ignite cache.
     * @param originalKey Original Key.
     * @throws SQLException If Failed.
     */
    private void checkDelete(String tblName, IgniteCache<Object, Object> cache,
        Object originalKey) throws IgniteCheckedException, SQLException {
        // Delete from cache.
        cache.remove(originalKey);

        try (PreparedStatement stmt = prepareStatement(cache.getName(), "SELECT * FROM " + tblName)) {
            try {
                if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                    !waitForCondition(new GridAbsPredicateX() {
                                          @Override public boolean applyx() throws IgniteCheckedException {
                                              try {
                                                  return !stmt.executeQuery().next();
                                              }
                                              catch (SQLException e) {
                                                  throw new IgniteCheckedException(e);
                                              }
                                          }
                                      },
                        TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                    fail("Deleted data are still retrievable via SELECT.");
            }
            catch (GridClosureException e) {
                throw (SQLException)e.getCause().getCause();
            }

            ResultSet rs = stmt.executeQuery();

            assertNotNull(rs);

            assertFalse("Unexpected rows count.", rs.next());
        }
    }

    /**
     *
     * @param converterToSqlExpVal Function that converts expected key/value in conformity with a format of jdbc
     *  return key/value.
     * @param equalsProcessor Equals processor that process equality check of expected and got keys/values.
     * @param dataType Data type.
     * @param originalKey Original key.
     * @param originalVal Original value.
     * @param stmt Prepared statement.
     * @throws IgniteCheckedException If failed.
     * @throws SQLException If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkQuery(Function converterToSqlExpVal, BiFunction<Object, Object, Boolean> equalsProcessor,
        Class<?> dataType, Object originalKey, Object originalVal, PreparedStatement stmt)
        throws IgniteCheckedException, SQLException {
        try {
            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(new GridAbsPredicateX() {
                                      @Override public boolean applyx() throws IgniteCheckedException {
                                          try {
                                              return stmt.executeQuery().next();
                                          }
                                          catch (SQLException e) {
                                              throw new IgniteCheckedException(e);
                                          }
                                      }
                                  },
                    TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve data via SELECT.");
        }
        catch (GridClosureException e) {
            throw (SQLException)e.getCause().getCause();
        }

        ResultSet rs = stmt.executeQuery();

        assertNotNull(rs);

        ResultSetMetaData meta = rs.getMetaData();

        assertNotNull(meta);

        IgniteBiTuple<Integer, Class> typeClsPair = javaClsToSqlTypeMap
            .getOrDefault(dataType, new IgniteBiTuple<>(Types.OTHER, originalVal.getClass()));

        int metaType = typeClsPair.get1();

        Object expJavaDataType = typeClsPair.get2();

        assertEquals("Unexpected metadata data type name for key.", metaType, meta.getColumnType(1));
        assertEquals("Unexpected metadata data type name for value.", metaType, meta.getColumnType(2));

        assertEquals("Unexpected columns count.", 2, meta.getColumnCount());

        int cnt = 0;

        while (rs.next()) {
            Object gotKey = rs.getObject(1);
            Object gotVal = rs.getObject(2);

            assertEquals("Unexpected key data type.", expJavaDataType, gotKey.getClass());
            assertEquals("Unexpected value data type.", expJavaDataType, gotVal.getClass());

            assertTrue(equalsProcessor.apply(converterToSqlExpVal.apply(originalKey), gotKey));
            assertTrue(equalsProcessor.apply(converterToSqlExpVal.apply(originalVal), gotVal));

            cnt++;
        }

        assertEquals("Unexpected rows count.", 1, cnt);
    }
}
