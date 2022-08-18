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

package org.apache.ignite.internal.processors.query.stat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.GridClientByteUtils;
import org.junit.Test;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToSqlDate;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToSqlTime;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToTimestamp;

/**
 * Gather statistics on test table dtypes and check that statistics manager will return correct statistics for
 * different data types.
 */
public class ManagerStatisticsTypesTest extends StatisticsTypesAbstractTest {
    /**
     * Check collected statistics for each type.
     */
    @Test
    public void testCollectedStatistics() {

        IgniteH2Indexing indexing = (IgniteH2Indexing)grid(0).context().query().getIndexing();
        ObjectStatisticsImpl dtypesStat = (ObjectStatisticsImpl)indexing.statsManager().getLocalStatistics(
            new StatisticsKey(SCHEMA, "DTYPES"));

        assertNotNull(dtypesStat);

        assertEquals(SMALL_SIZE * 1.5 - 1, dtypesStat.rowCount());

        assertEquals(TYPES.length + 3, dtypesStat.columnsStatistics().size());

        for (String type : TYPES) {
            String colName = COL_NAME_PREFIX + type;
            ColumnStatistics colStat = dtypesStat.columnStatistics(colName);

            assertNotNull(colStat);

            if (colName.equals("COL_GEOMETRY"))
                assertEquals("Unexpected nulls count for column " + colName, SMALL_SIZE * 1.5 - 1, colStat.nulls());
            else
                assertEquals("Unexpected nulls count for column " + colName, 50, colStat.nulls());

            assertEquals(dtypesStat.rowCount(), colStat.total());
            assertNotNull(colStat.raw());

            if ("BINARY".equals(type) || "GEOMETRY".equals(type) || "VARCHAR".equals(type) || "CHAR".equals(type)) {
                assertNull("Unexpected min for " + type, colStat.min());
                assertNull("Unexpected max for " + type, colStat.max());
            }
            else {
                assertNotNull("Unexpected min for " + type, colStat.min());
                assertNotNull("Unexpected max for " + type, colStat.max());
            }
        }
    }

    /**
     * Check boolean type statistics collection.
     */
    @Test
    public void testBooleanStatistics() {
        String colName = COL_NAME_PREFIX + "BOOLEAN";
        ColumnStatistics booleanStats = getTypesStats().columnStatistics(colName);

        assertEquals(2, booleanStats.distinct());
        assertTrue(booleanStats.min().compareTo(BigDecimal.ZERO) == 0);
        assertTrue(booleanStats.max().compareTo(BigDecimal.ONE) == 0);
        assertEquals(1, booleanStats.size());
    }

    /**
     * Check boolean type statistics collection.
     */
    @Test
    public void testIntStatistics() {
        String colName = COL_NAME_PREFIX + "INT";
        ColumnStatistics intStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, intStats.distinct());
        assertEquals(1, intStats.min().intValue());
        assertEquals(SMALL_SIZE - 1, intStats.max().intValue());
        assertEquals(4, intStats.size());
    }

    /**
     * Check tinyint type statistics collection.
     */
    @Test
    public void testTinyintStatistics() {
        String colName = COL_NAME_PREFIX + "TINYINT";
        ColumnStatistics tinyintStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, tinyintStats.distinct());
        assertEquals(1, tinyintStats.min().byteValue());
        assertEquals(SMALL_SIZE - 1, tinyintStats.max().byteValue());
        assertEquals(1, tinyintStats.size());
    }

    /**
     * Check smallint type statistics collection.
     */
    @Test
    public void testSmallintStatistics() {
        String colName = COL_NAME_PREFIX + "SMALLINT";
        ColumnStatistics smallintStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, smallintStats.distinct());
        assertEquals(1, smallintStats.min().shortValue());
        assertEquals(SMALL_SIZE - 1, smallintStats.max().shortValue());
        assertEquals(2, smallintStats.size());
    }

    /**
     * Check bigint type statistics collection.
     */
    @Test
    public void testBigintStatistics() {
        String colName = COL_NAME_PREFIX + "BIGINT";
        ColumnStatistics bigintStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, bigintStats.distinct());
        assertEquals(1, bigintStats.min().intValue());
        assertEquals(SMALL_SIZE - 1, bigintStats.max().intValue());
        assertEquals(8, bigintStats.size());
    }

    /**
     * Check decimal type statistics collection.
     */
    @Test
    public void testDecimalStatistics() {
        String colName = COL_NAME_PREFIX + "DECIMAL";
        ColumnStatistics decimalStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, decimalStats.distinct());
        assertEquals(new BigDecimal("0.01"), decimalStats.min());
        assertEquals(new BigDecimal("" + ((double)SMALL_SIZE - 1) / 100), decimalStats.max());

        // Size of unscaled value plus scale.
        assertEquals(new BigDecimal("0.01").unscaledValue().toByteArray().length + 4, decimalStats.size());
    }

    /**
     * Check double type statistics collection.
     */
    @Test
    public void testDoubleStatistics() {
        String colName = COL_NAME_PREFIX + "DOUBLE";
        ColumnStatistics doubleStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, doubleStats.distinct());
        assertEquals(0.01, doubleStats.min().doubleValue());
        assertEquals(((double)SMALL_SIZE - 1) / 100, doubleStats.max().doubleValue());
        assertEquals(8, doubleStats.size());
    }

    /**
     * Check real type statistics collection.
     */
    @Test
    public void testRealStatistics() {
        String colName = COL_NAME_PREFIX + "REAL";
        ColumnStatistics realStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, realStats.distinct());
        assertEquals(0.01f, realStats.min().floatValue());
        assertEquals(((float)SMALL_SIZE - 1) / 100, realStats.max().floatValue());
        assertEquals(4, realStats.size());
    }

    /**
     * Check time type statistics collection.
     */
    @Test
    public void testTimeStatistics() {
        String colName = COL_NAME_PREFIX + "TIME";
        ColumnStatistics timeStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, timeStats.distinct());
        assertEquals(convert(LocalTime.of(12, 0, 1)), timeStats.min());
        assertEquals(convert(LocalTime.of(12, 1, 39)), timeStats.max());
        assertEquals(8, timeStats.size());
    }

    /**
     * Check date type statistics collection.
     */
    @Test
    public void testDateStatistics() {
        String colName = COL_NAME_PREFIX + "DATE";
        ColumnStatistics dateStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, dateStats.distinct());
        assertEquals(convert(LocalDate.of(1970, Month.JANUARY, 2)), dateStats.min());
        assertEquals(convert(LocalDate.of(1970, Month.APRIL, 10)), dateStats.max());
        assertEquals(8, dateStats.size());
    }

    /**
     * Check timestamp type statistics collection.
     */
    @Test
    public void testTimestampStatistics() {
        String colName = COL_NAME_PREFIX + "TIMESTAMP";
        ColumnStatistics timestampStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, timestampStats.distinct());
        assertEquals(convert(LocalDateTime.of(1970, Month.JANUARY, 1, 12, 0, 1)), timestampStats.min());
        assertEquals(convert(LocalDateTime.of(1970, Month.JANUARY, 1, 12, 1, 39)), timestampStats.max());
        assertEquals(12, timestampStats.size());
    }

    /**
     * Check varchar type statistics collection.
     */
    @Test
    public void testVarcharStatistics() {
        String colName = COL_NAME_PREFIX + "VARCHAR";
        ColumnStatistics varcharStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, varcharStats.distinct());
        assertNull(varcharStats.min());
        assertNull(varcharStats.max());
        assertEquals(8, varcharStats.size());
    }

    /**
     * Check char type statistics collection.
     */
    @Test
    public void testCharStatistics() {
        String colName = COL_NAME_PREFIX + "CHAR";
        ColumnStatistics charStats = getTypesStats().columnStatistics(colName);

        assertEquals(26, charStats.distinct());
        assertNull(charStats.min());
        assertNull(charStats.max());
        assertEquals(1, charStats.size());
    }

    /**
     * Check UUID type statistics collection.
     */
    @Test
    public void testUUIDStatistics() {
        String colName = COL_NAME_PREFIX + "UUID";
        ColumnStatistics decimalStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, decimalStats.distinct());
        assertEquals(new BigDecimal(new BigInteger(1, GridClientByteUtils.uuidToBytes(new UUID(0L, 1L)))),
            decimalStats.min());
        assertEquals(new BigDecimal(new BigInteger(1, GridClientByteUtils.uuidToBytes(new UUID(0L, SMALL_SIZE - 1L)))),
            decimalStats.max());
        assertEquals(16, decimalStats.size());
    }

    /**
     * Check binary type statistics collection.
     */
    @Test
    public void testBinaryStatistics() {
        String colName = COL_NAME_PREFIX + "BINARY";
        ColumnStatistics binaryStats = getTypesStats().columnStatistics(colName);

        assertEquals(SMALL_SIZE - 1, binaryStats.distinct());
        assertNull(binaryStats.min());
        assertNull(binaryStats.max());
        assertEquals(4, binaryStats.size());
    }

    /**
     * Get local statistics for dtypes table.
     *
     * @return Local object statistics for dtypes table.
     */
    private ObjectStatisticsImpl getTypesStats() {
        return (ObjectStatisticsImpl)statisticsMgr(0).getLocalStatistics(new StatisticsKey(SCHEMA, "DTYPES"));
    }

    /** */
    private static BigDecimal convert(LocalTime time) {
        return new BigDecimal(convertToSqlTime(time).getTime());
    }

    /** */
    private static BigDecimal convert(LocalDate date) {
        return new BigDecimal(convertToSqlDate(date).getTime());
    }

    /** */
    private static BigDecimal convert(LocalDateTime ts) {
        return new BigDecimal(convertToTimestamp(ts).getTime());
    }
}
