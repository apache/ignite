/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.sqltests;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.AbstractDataTypesCoverageTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Data types coverage for basic sql operations.
 */
public class SqlDataTypesCoverageTests extends AbstractDataTypesCoverageTest {
    /** */
    protected static final int TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE = 10_000;

    /** {@inheritDoc} */
    @Before
    @Override
    public void init() throws Exception {
        super.init();
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-boolean
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBooleanDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.BOOLEAN,
            Boolean.TRUE,
            Boolean.FALSE);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-int
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIntDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.INT,
            0,
            1,
            Integer.MAX_VALUE,
            Integer.MIN_VALUE);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-tinyint
     *
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23065")
    @Test
    public void testTinyIntDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.TINYINT,
            (byte)0,
            (byte)1,
            Byte.MIN_VALUE,
            Byte.MAX_VALUE);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-smallint
     *
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23065")
    @Test
    public void testSmallIntDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.SMALLINT,
            (short)0,
            (short)1,
            Short.MIN_VALUE,
            Short.MAX_VALUE);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-bigint
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBigIntDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.BIGINT,
            0L,
            1L,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-decimal
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.DECIMAL,
            new BigDecimal(123.123),
            BigDecimal.ONE,
            BigDecimal.ZERO,
            BigDecimal.valueOf(123456789, 0),
            BigDecimal.valueOf(123456789, 1),
            BigDecimal.valueOf(123456789, 2),
            BigDecimal.valueOf(123456789, 3));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-double
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDoubleDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.DOUBLE,
            Double.MIN_VALUE,
            Double.MAX_VALUE,
            new Quoted(Double.NaN),
            new Quoted(Double.NEGATIVE_INFINITY),
            new Quoted(Double.POSITIVE_INFINITY),
            0D,
            0.0,
            1D,
            1.0,
            1.1);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-real
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRealDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.REAL,
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
     * https://apacheignite-sql.readme.io/docs/data-types#section-time
     *
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23410")
    @Test
    public void testTimeDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.TIME,
            new Timed(new java.sql.Time(0L)),
            new Timed(new java.sql.Time(123L)));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-date
     *
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-17353")
    @Test
    public void testDateDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.DATE,
            new Dated(new java.sql.Date(0L)),
            new Dated(new java.sql.Date(123L)));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-timestamp
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTimestampDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.TIMESTAMP,
            new Dated(new java.sql.Timestamp(0L)),
            new Dated(new java.sql.Timestamp(123L)));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-varchar
     *
     * @throws Exception If failed.
     */
    @Test
    public void testVarcharDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.VARCHAR,
            new Quoted(""),
            new Quoted("abcABC"),
            new Quoted("!@#$%^&*()"));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-char
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCharDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.CHAR,
            new Quoted("a"),
            new Quoted("A"),
            new Quoted("@"));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-uuid
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUUIDDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.UUID,
            new Quoted(UUID.randomUUID()),
            new Quoted(UUID.randomUUID()));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-binary
     *
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23401")
    @SuppressWarnings("ZeroLengthArrayAllocation")
    @Test
    public void testBinaryDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.BINARY,
            new ByteArrayed(new byte[] {1, 2, 3}),
            new ByteArrayed(new byte[] {3, 2, 1}),
            new ByteArrayed(new byte[] {}));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-geometry
     *
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23066")
    @Test
    public void testGeometryDataType() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();

        checkBasicSqlOperations(SqlDataType.GEOMETRY,
            new Quoted(new Point(
                new CoordinateArraySequence(new Coordinate[] {new Coordinate(1.1, 2.2)}),geometryFactory)),
            new Quoted(new Point(
                new CoordinateArraySequence(new Coordinate[] {new Coordinate(3.3, 4.4)}),geometryFactory)));
    }

    /**
     * Create table based on test-parameters-dependent template with both id (PK) and val of {@code dataType}.
     * Process sql CRUD and verify that all operations works as expected via SELECT:
     * <ul>
     * <li>INSERT</li>
     * <li>SELECT</li>
     * <li>UPDATE</li>
     * <li>SELECT with WHERE CLAUSE</li>
     * <li>DELETE</li>
     * </ul>
     *
     * @param dataType Sql data type to check.
     * @param valsToCheck Array of values to check.
     * @throws Exception If Failed.
     */
    @SuppressWarnings("unchecked")
    protected void checkBasicSqlOperations(SqlDataType dataType, Object... valsToCheck) throws Exception {
        assert valsToCheck.length > 0;

        IgniteEx ignite = grid(new Random().nextInt(NODES_CNT));

        final String tblName = "table" + UUID.randomUUID().toString(). replaceAll("-", "_");

        final String templateName = "template" + UUID.randomUUID().toString(). replaceAll("-", "_");

        CacheConfiguration cfg = new CacheConfiguration<>(templateName)
            .setAtomicityMode(atomicityMode)
            .setCacheMode(cacheMode)
            .setExpiryPolicyFactory(ttlFactory)
            .setBackups(backups)
            .setEvictionPolicyFactory(evictionFactory)
            .setOnheapCacheEnabled(evictionFactory != null || onheapCacheEnabled)
            .setWriteSynchronizationMode(writeSyncMode)
            .setAffinity(new RendezvousAffinityFunction(false, PARTITIONS_CNT));

        ignite.addCacheConfiguration(cfg);

        ignite.context().query().querySqlFields(new SqlFieldsQuery(
            "CREATE TABLE " + tblName +
                "(id " + dataType + " PRIMARY KEY," +
                " val " + dataType + ")" +
                " WITH " + "\"template=" + templateName + "\""), false);

        if (cacheMode != CacheMode.LOCAL) {
            ignite.context().query().querySqlFields(new SqlFieldsQuery(
                "CREATE INDEX IDX" + UUID.randomUUID().toString().replaceAll("-", "_") +
                    " ON " + tblName + "(id, val)"), false);
        }

        checkCRUD(ignite, tblName, dataType, valsToCheck);
    }

    /**
     * Perform and verify sql CRUD operations.
     *
     * @param ignite Ignite instance.
     * @param tblName Table name.
     * @param dataType Sql data type to check.
     * @param valsToCheck Array of values to check.
     * @throws Exception If Failed.
     */
    private void checkCRUD(IgniteEx ignite, String tblName, SqlDataType dataType, Object... valsToCheck)
        throws Exception {
        for (int i = 0; i < valsToCheck.length; i++) {

            Object valToCheck = valsToCheck[i];

            Object valToPut = valToCheck instanceof SqlStrConvertedValHolder ?
                ((SqlStrConvertedValHolder)valToCheck).sqlStrVal() :
                valToCheck;

            Object expVal = valToCheck instanceof SqlStrConvertedValHolder ?
                ((SqlStrConvertedValHolder)valToCheck).originalVal() :
                valToCheck;

            // INSERT
            ignite.context().query().querySqlFields(new SqlFieldsQuery("INSERT INTO " + tblName +
                "(id, val)  VALUES (" + valToPut + ", " + valToPut + ");"), false);

            // Check INSERT/SELECT
            check(ignite, "SELECT id, val FROM " + tblName + ";", dataType, expVal, expVal);

            List<List<?>> res;

            Object revertedVal = valsToCheck[valsToCheck.length - 1 - i];

            Object revertedValToPut = revertedVal instanceof SqlStrConvertedValHolder ?
                ((SqlStrConvertedValHolder)revertedVal).sqlStrVal() :
                revertedVal;

            Object expRevertedVal = revertedVal instanceof SqlStrConvertedValHolder ?
                ((SqlStrConvertedValHolder)revertedVal).originalVal() :
                revertedVal;

            // UPDATE
            ignite.context().query().querySqlFields(
                new SqlFieldsQuery("UPDATE " + tblName + " SET val =  " + revertedValToPut + ";"), false);

            // Check UPDATE/SELECT
            check(ignite, "SELECT id, val FROM " + tblName + ";", dataType, expVal, expRevertedVal);

            // Check UPDATE/SELECT with WHERE clause
            check(ignite, "SELECT id, val FROM " + tblName + " where id =  " + valToPut + ";",
                dataType, expVal, expRevertedVal);

            // DELETE
            ignite.context().query().querySqlFields(
                new SqlFieldsQuery("DELETE FROM " + tblName + " where id =  " + valToPut + ";"), false);

            // Check DELETE/SELECT
            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> ignite.context().query().querySqlFields(
                    new SqlFieldsQuery("SELECT id FROM " + tblName + ";"), false).getAll().isEmpty(),
                    TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Deleted data are still retrievable via SELECT.");

            res = ignite.context().query().querySqlFields(new SqlFieldsQuery("SELECT id FROM " + tblName + ";"),
                false).getAll();

            assertEquals(0, res.size());
        }
    }

    /**
     * Perform Select query and check that both key and value has expected values.
     *
     * @param ignite Ignite instance.
     * @param qryStr Select query string.
     * @param dataType Sql data type.
     * @param expKey expected key.
     * @param expVal expected value.
     * @throws Exception If failed.
     */
    private void check(IgniteEx ignite, String qryStr, SqlDataType dataType, Object expKey, Object expVal)
        throws Exception {
        if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
            !waitForCondition(() -> !ignite.context().query().querySqlFields(
                new SqlFieldsQuery(qryStr), false).getAll().isEmpty(),
                TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
            fail("Unable to retrieve data via SELECT.");

        List<List<?>> res = ignite.context().query().querySqlFields(new SqlFieldsQuery(qryStr), false).
            getAll();

        assertEquals(1, res.size());

        assertEquals(2, res.get(0).size());

        // key
        assertTrue(res.get(0).get(0).getClass().equals(dataType.javaType));

        if (expKey instanceof byte[])
            assertTrue(Arrays.equals((byte[])expKey, (byte[])res.get(0).get(0)));
        else
            assertEquals(expKey, res.get(0).get(0));

        // val
        assertTrue(res.get(0).get(1).getClass().equals(dataType.javaType));

        if (expVal instanceof byte[])
            assertTrue(Arrays.equals((byte[])expVal, (byte[])res.get(0).get(1)));
        else
            assertEquals(expVal, res.get(0).get(1));
    }

    /**
     * Supported sql data types with corresponding java mappings.
     * https://apacheignite-sql.readme.io/docs/data-types
     */
    protected enum SqlDataType {
        /** */
        BOOLEAN(Boolean.class),

        /** */
        INT(Integer.class),

        /** */
        TINYINT(Byte.class),
        /** */
        SMALLINT(Short.class),

        /** */
        BIGINT(Long.class),

        /** */
        DECIMAL(BigDecimal.class),

        /** */
        DOUBLE(Double.class),

        /** */
        REAL(Float.class),

        /** */
        TIME(java.sql.Time.class),

        /** */
        DATE(java.sql.Date.class),

        /** */
        TIMESTAMP(java.sql.Timestamp.class),

        /** */
        VARCHAR(String.class),

        /** */
        CHAR(String.class),

        /** */
        UUID(UUID.class),

        /** */
        BINARY(byte[].class),

        /**
         * Please pay attention that point is just an example of GEOMETRY data types.
         * It might have sense to add few more Geometry data types to check when basic functionality will be fixed.
         */
        GEOMETRY(org.locationtech.jts.geom.Point.class);

        /**
         * Corresponding java type https://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
         */
        private Object javaType;

        /** */
        SqlDataType(Object javaType) {
            this.javaType = javaType;
        }
    }
}
