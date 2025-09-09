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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.junit.Test;

/**
 * Test SQL INTERVAL data types.
 */
public class IntervalTest extends AbstractBasicIntegrationTest {
    /**
     * Test returned result for interval data types.
     */
    @Test
    public void testIntervalResult() {
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(-1), eval("INTERVAL -1 SECONDS"));
        assertEquals(Duration.ofSeconds(123), eval("INTERVAL 123 SECONDS"));
        assertEquals(Duration.ofSeconds(123), eval("INTERVAL '123' SECONDS"));
        assertEquals(Duration.ofSeconds(123), eval("INTERVAL '123' SECONDS(3)"));
        assertEquals(Duration.ofMinutes(2), eval("INTERVAL 2 MINUTES"));
        assertEquals(Duration.ofHours(3), eval("INTERVAL 3 HOURS"));
        assertEquals(Duration.ofDays(4), eval("INTERVAL 4 DAYS"));
        assertEquals(Period.ofMonths(5), eval("INTERVAL 5 MONTHS"));
        assertEquals(Period.ofMonths(-5), eval("INTERVAL -5 MONTHS"));
        assertEquals(Period.ofYears(6), eval("INTERVAL 6 YEARS"));
        assertEquals(Period.of(1, 2, 0), eval("INTERVAL '1-2' YEAR TO MONTH"));
        assertEquals(Duration.ofHours(25), eval("INTERVAL '1 1' DAY TO HOUR"));
        assertEquals(Duration.ofMinutes(62), eval("INTERVAL '1:2' HOUR TO MINUTE"));
        assertEquals(Duration.ofSeconds(63), eval("INTERVAL '1:3' MINUTE TO SECOND"));
        assertEquals(Duration.ofSeconds(3723), eval("INTERVAL '1:2:3' HOUR TO SECOND"));
        assertEquals(Duration.ofMillis(3723456), eval("INTERVAL '0 1:2:3.456' DAY TO SECOND"));
    }

    /**
     * Test cast interval types to numeric and numeric to interval.
     */
    @Test
    public void testIntervalNumCast() {
        assertNull(eval("CAST(NULL::INTERVAL SECONDS AS INT)"));
        assertNull(eval("CAST(NULL::INTERVAL MONTHS AS INT)"));
        assertEquals(1, eval("CAST(INTERVAL 1 SECONDS AS INT)"));
        assertEquals(2, eval("CAST(INTERVAL 2 MINUTES AS INT)"));
        assertEquals(3, eval("CAST(INTERVAL 3 HOURS AS INT)"));
        assertEquals(4, eval("CAST(INTERVAL 4 DAYS AS INT)"));
        assertEquals(-4, eval("CAST(INTERVAL -4 DAYS AS INT)"));
        assertEquals(5, eval("CAST(INTERVAL 5 MONTHS AS INT)"));
        assertEquals(6, eval("CAST(INTERVAL 6 YEARS AS INT)"));
        assertEquals(-6, eval("CAST(INTERVAL -6 YEARS AS INT)"));

        assertNull(eval("CAST(NULL::INT AS INTERVAL SECONDS)"));
        assertNull(eval("CAST(NULL::INT AS INTERVAL MONTHS)"));
        assertEquals(Duration.ofSeconds(1), eval("CAST(1 AS INTERVAL SECONDS)"));
        assertEquals(Duration.ofMinutes(2), eval("CAST(2 AS INTERVAL MINUTES)"));
        assertEquals(Duration.ofHours(3), eval("CAST(3 AS INTERVAL HOURS)"));
        assertEquals(Duration.ofDays(4), eval("CAST(4 AS INTERVAL DAYS)"));
        assertEquals(Period.ofMonths(5), eval("CAST(5 AS INTERVAL MONTHS)"));
        assertEquals(Period.ofYears(6), eval("CAST(6 AS INTERVAL YEARS)"));
        assertEquals(Duration.ofDays(1), eval("CAST(1::INT AS INTERVAL DAYS)"));
        assertEquals(Period.ofMonths(1), eval("CAST(1::INT AS INTERVAL MONTHS)"));
        assertEquals(Duration.ofHours(36), eval("CAST(1.5 AS INTERVAL DAYS)"));
        assertEquals(Period.of(1, 6, 0), eval("CAST(1.5 AS INTERVAL YEARS)"));
        assertEquals(Duration.ofHours(36), eval("CAST(f AS INTERVAL DAYS) FROM (VALUES(1.5)) AS t(f)"));
        assertEquals(Period.of(1, 6, 0), eval("CAST(f AS INTERVAL YEARS) FROM (VALUES(1.5)) AS t(f)"));
        assertEquals(Duration.ofHours(36), eval("CAST(1.5::DECIMAL(2,1) AS INTERVAL DAYS)"));
        assertEquals(Period.of(1, 6, 0), eval("CAST(1.5::DECIMAL(2,1) AS INTERVAL YEARS)"));

        // Compound interval types cannot be cast.
        assertThrows("SELECT CAST(INTERVAL '1-2' YEAR TO MONTH AS INT)", IgniteSQLException.class, "cannot convert");
        assertThrows("SELECT CAST(INTERVAL '1 2' DAY TO HOUR AS INT)", IgniteSQLException.class, "cannot convert");

        assertThrows("SELECT CAST(1 AS INTERVAL YEAR TO MONTH)", IgniteSQLException.class, "cannot convert");
        assertThrows("SELECT CAST(1 AS INTERVAL DAY TO HOUR)", IgniteSQLException.class, "cannot convert");
    }

    /**
     * Test cast interval types to string and string to interval.
     */
    @Test
    public void testIntervalStringCast() {
        assertNull(eval("CAST(NULL::INTERVAL SECONDS AS VARCHAR)"));
        assertNull(eval("CAST(NULL::INTERVAL MONTHS AS VARCHAR)"));
        assertEquals("+1.234", eval("CAST(INTERVAL '1.234' SECONDS (1,3) AS VARCHAR)"));
        assertEquals("+1.000000", eval("CAST(INTERVAL 1 SECONDS AS VARCHAR)"));
        assertEquals("+2", eval("CAST(INTERVAL 2 MINUTES AS VARCHAR)"));
        assertEquals("+3", eval("CAST(INTERVAL 3 HOURS AS VARCHAR)"));
        assertEquals("+4", eval("CAST(INTERVAL 4 DAYS AS VARCHAR)"));
        assertEquals("+5", eval("CAST(INTERVAL 5 MONTHS AS VARCHAR)"));
        assertEquals("+6", eval("CAST(INTERVAL 6 YEARS AS VARCHAR)"));
        assertEquals("+1-02", eval("CAST(INTERVAL '1-2' YEAR TO MONTH AS VARCHAR)"));
        assertEquals("+1 02", eval("CAST(INTERVAL '1 2' DAY TO HOUR AS VARCHAR)"));
        assertEquals("-1 02:03:04.000000", eval("CAST(INTERVAL '-1 2:3:4' DAY TO SECOND AS VARCHAR)"));

        assertNull(eval("CAST(NULL::VARCHAR AS INTERVAL SECONDS)"));
        assertNull(eval("CAST(NULL::VARCHAR AS INTERVAL MONTHS)"));
        assertEquals(Duration.ofSeconds(1), eval("CAST('1' AS INTERVAL SECONDS)"));
        assertEquals(Duration.ofMinutes(2), eval("CAST('2' AS INTERVAL MINUTES)"));
        assertEquals(Duration.ofHours(3), eval("CAST('3' AS INTERVAL HOURS)"));
        assertEquals(Duration.ofDays(4), eval("CAST('4' AS INTERVAL DAYS)"));
        assertEquals(Duration.ofHours(26), eval("CAST('1 2' AS INTERVAL DAY TO HOUR)"));
        assertEquals(Duration.ofMinutes(62), eval("CAST('1:2' AS INTERVAL HOUR TO MINUTE)"));
        assertEquals(Duration.ofMillis(3723456), eval("CAST('0 1:2:3.456' AS INTERVAL DAY TO SECOND)"));
        assertEquals(Duration.ofMillis(-3723456), eval("CAST('-0 1:2:3.456' AS INTERVAL DAY TO SECOND)"));
        assertEquals(Period.ofMonths(5), eval("CAST('5' AS INTERVAL MONTHS)"));
        assertEquals(Period.ofYears(6), eval("CAST('6' AS INTERVAL YEARS)"));
        assertEquals(Period.of(1, 2, 0), eval("CAST('1-2' AS INTERVAL YEAR TO MONTH)"));
    }

    /**
     * Test cast between interval types.
     */
    @Test
    public void testIntervalToIntervalCast() {
        assertNull(eval("CAST(NULL::INTERVAL MINUTE AS INTERVAL SECONDS)"));
        assertNull(eval("CAST(NULL::INTERVAL YEAR AS INTERVAL MONTHS)"));
        assertEquals(Duration.ofMinutes(1), eval("CAST(INTERVAL 60 SECONDS AS INTERVAL MINUTE)"));
        assertEquals(Duration.ofHours(1), eval("CAST(INTERVAL 60 MINUTES AS INTERVAL HOUR)"));
        assertEquals(Duration.ofDays(1), eval("CAST(INTERVAL 24 HOURS AS INTERVAL DAY)"));
        assertEquals(Period.ofYears(1), eval("CAST(INTERVAL 1 YEAR AS INTERVAL MONTHS)"));
        assertEquals(Period.ofYears(1), eval("CAST(INTERVAL 12 MONTHS AS INTERVAL YEARS)"));

        // Cannot convert between month-year and day-time interval types.
        assertThrows("SELECT CAST(INTERVAL 1 MONTHS AS INTERVAL DAYS)", IgniteSQLException.class, "cannot convert");
        assertThrows("SELECT CAST(INTERVAL 1 DAYS AS INTERVAL MONTHS)", IgniteSQLException.class, "cannot convert");
    }

    /**
     * Test DML statements with interval data type.
     */
    @Test
    public void testDml() {
        executeSql("CREATE TABLE test(ym INTERVAL YEAR, dt INTERVAL DAYS)");
        executeSql("INSERT INTO test(ym, dt) VALUES (INTERVAL 1 MONTH, INTERVAL 2 DAYS)");
        executeSql("INSERT INTO test(ym, dt) VALUES (INTERVAL 3 YEARS, INTERVAL 4 HOURS)");
        executeSql("INSERT INTO test(ym, dt) VALUES (INTERVAL '4-5' YEARS TO MONTHS, INTERVAL '6:7' HOURS TO MINUTES)");
        executeSql("INSERT INTO test(ym, dt) VALUES (NULL, NULL)");
        assertThrows("INSERT INTO test(ym, dt) VALUES (INTERVAL 1 DAYS, INTERVAL 1 HOURS)",
            IgniteSQLException.class, "Cannot assign");
        assertThrows("INSERT INTO test(ym, dt) VALUES (INTERVAL 1 YEARS, INTERVAL 1 MONTHS)",
            IgniteSQLException.class, "Cannot assign");

        assertQuery("SELECT * FROM test")
            .returns(Period.ofMonths(1), Duration.ofDays(2))
            .returns(Period.ofYears(3), Duration.ofHours(4))
            .returns(Period.of(4, 5, 0), Duration.ofMinutes(367))
            .returns(null, null)
            .check();

        assertThrows("SELECT * FROM test WHERE ym = INTERVAL 6 DAYS", IgniteSQLException.class, "Cannot apply");
        assertThrows("SELECT * FROM test WHERE dt = INTERVAL 6 YEARS", IgniteSQLException.class, "Cannot apply");

        executeSql("UPDATE test SET dt = INTERVAL 3 DAYS WHERE ym = INTERVAL 1 MONTH");
        executeSql("UPDATE test SET ym = INTERVAL 5 YEARS WHERE dt = INTERVAL 4 HOURS");
        executeSql("UPDATE test SET ym = INTERVAL '6-7' YEARS TO MONTHS, dt = INTERVAL '8 9' DAYS TO HOURS " +
            "WHERE ym = INTERVAL '4-5' YEARS TO MONTHS AND dt = INTERVAL '6:7' HOURS TO MINUTES");

        assertThrows("UPDATE test SET dt = INTERVAL 5 YEARS WHERE ym = INTERVAL 1 MONTH", IgniteSQLException.class,
            "Cannot assign");

        assertThrows("UPDATE test SET ym = INTERVAL 8 YEARS WHERE dt = INTERVAL 1 MONTH", IgniteSQLException.class,
            "Cannot apply");

        assertQuery("SELECT * FROM test")
            .returns(Period.ofMonths(1), Duration.ofDays(3))
            .returns(Period.ofYears(5), Duration.ofHours(4))
            .returns(Period.of(6, 7, 0), Duration.ofHours(201))
            .returns(null, null)
            .check();

        assertThrows("DELETE FROM test WHERE ym = INTERVAL 6 DAYS", IgniteSQLException.class, "Cannot apply");
        assertThrows("DELETE FROM test WHERE dt = INTERVAL 6 YEARS", IgniteSQLException.class, "Cannot apply");

        executeSql("DELETE FROM test WHERE ym = INTERVAL 1 MONTH");
        executeSql("DELETE FROM test WHERE dt = INTERVAL 4 HOURS");
        executeSql("DELETE FROM test WHERE ym = INTERVAL '6-7' YEARS TO MONTHS AND dt = INTERVAL '8 9' DAYS TO HOURS");
        executeSql("DELETE FROM test WHERE ym IS NULL AND dt IS NULL");

        assertEquals(0, executeSql("SELECT * FROM test").size());

        executeSql("ALTER TABLE test ADD (ym2 INTERVAL MONTH, dt2 INTERVAL HOURS)");

        executeSql("INSERT INTO test(ym, ym2, dt, dt2) VALUES (INTERVAL 1 YEAR, INTERVAL 2 YEARS, " +
            "INTERVAL 1 SECOND, INTERVAL 2 MINUTES)");

        assertQuery("SELECT ym, ym2, dt, dt2 FROM test")
            .returns(Period.ofYears(1), Period.ofYears(2), Duration.ofSeconds(1), Duration.ofMinutes(2))
            .check();
    }

    /**
     * Test interval arithmetic.
     */
    @Test
    public void testIntervalArithmetic() {
        // Date +/- interval.
        assertEquals(Date.valueOf("2021-01-02"), eval("DATE '2021-01-01' + INTERVAL 1 DAY"));
        assertEquals(Date.valueOf("2020-12-31"), eval("DATE '2021-01-01' - INTERVAL 1 DAY"));
        assertEquals(Date.valueOf("2020-12-31"), eval("DATE '2021-01-01' + INTERVAL -1 DAY"));
        assertEquals(Date.valueOf("2021-02-01"), eval("DATE '2021-01-01' + INTERVAL 1 MONTH"));
        assertEquals(Date.valueOf("2022-01-01"), eval("DATE '2021-01-01' + INTERVAL 1 YEAR"));
        assertEquals(Date.valueOf("2022-02-01"), eval("DATE '2021-01-01' + INTERVAL '1-1' YEAR TO MONTH"));

        // Timestamp +/- interval.
        assertEquals(Timestamp.valueOf("2021-01-01 00:00:01"),
            eval("TIMESTAMP '2021-01-01 00:00:00' + INTERVAL 1 SECOND"));
        assertEquals(Timestamp.valueOf("2021-01-01 00:00:01.123"),
            eval("TIMESTAMP '2021-01-01 00:00:00.123' + INTERVAL 1 SECOND"));
        assertEquals(Timestamp.valueOf("2021-01-01 00:00:01.123"),
            eval("TIMESTAMP '2021-01-01 00:00:00' + INTERVAL '1.123' SECOND"));
        assertEquals(Timestamp.valueOf("2021-01-01 00:00:01.246"),
            eval("TIMESTAMP '2021-01-01 00:00:00.123' + INTERVAL '1.123' SECOND"));
        assertEquals(Timestamp.valueOf("2020-12-31 23:59:59"),
            eval("TIMESTAMP '2021-01-01 00:00:00' - INTERVAL 1 SECOND"));
        assertEquals(Timestamp.valueOf("2020-12-31 23:59:59"),
            eval("TIMESTAMP '2021-01-01 00:00:00' + INTERVAL -1 SECOND"));
        assertEquals(Timestamp.valueOf("2021-01-01 00:01:00"),
            eval("TIMESTAMP '2021-01-01 00:00:00' + INTERVAL 1 MINUTE"));
        assertEquals(Timestamp.valueOf("2021-01-01 01:00:00"),
            eval("TIMESTAMP '2021-01-01 00:00:00' + INTERVAL 1 HOUR"));
        assertEquals(Timestamp.valueOf("2021-01-02 00:00:00"),
            eval("TIMESTAMP '2021-01-01 00:00:00' + INTERVAL 1 DAY"));
        assertEquals(Timestamp.valueOf("2021-02-01 00:00:00"),
            eval("TIMESTAMP '2021-01-01 00:00:00' + INTERVAL 1 MONTH"));
        assertEquals(Timestamp.valueOf("2022-01-01 00:00:00"),
            eval("TIMESTAMP '2021-01-01 00:00:00' + INTERVAL 1 YEAR"));
        assertEquals(Timestamp.valueOf("2021-01-02 01:01:01.123"),
            eval("TIMESTAMP '2021-01-01 00:00:00' + INTERVAL '1 1:1:1.123' DAY TO SECOND"));
        assertEquals(Timestamp.valueOf("2022-02-01 01:01:01.123"),
            eval("TIMESTAMP '2021-01-01 01:01:01.123' + INTERVAL '1-1' YEAR TO MONTH"));

        // Time +/- interval.
        assertEquals(Time.valueOf("00:00:01"), eval("TIME '00:00:00' + INTERVAL 1 SECOND"));
        assertEquals(Time.valueOf("00:01:00"), eval("TIME '00:00:00' + INTERVAL 1 MINUTE"));
        assertEquals(Time.valueOf("01:00:00"), eval("TIME '00:00:00' + INTERVAL 1 HOUR"));

        // Date - date as interval.
        assertEquals(Duration.ofDays(1), eval("(DATE '2021-01-02' - DATE '2021-01-01') DAYS"));
        assertEquals(Duration.ofDays(-1), eval("(DATE '2021-01-01' - DATE '2021-01-02') DAYS"));
        assertEquals(Duration.ofDays(1), eval("(DATE '2021-01-02' - DATE '2021-01-01') HOURS"));
        assertEquals(Period.ofYears(1), eval("(DATE '2022-01-01' - DATE '2021-01-01') YEARS"));
        assertEquals(Period.ofMonths(1), eval("(DATE '2021-02-01' - DATE '2021-01-01') MONTHS"));
        assertEquals(Period.ofMonths(-1), eval("(DATE '2021-01-01' - DATE '2021-02-01') MONTHS"));
        assertEquals(Period.ofMonths(0), eval("(DATE '2021-01-20' - DATE '2021-01-01') MONTHS"));

        // Timestamp - timestamp as interval.
        assertEquals(Duration.ofDays(1),
            eval("(TIMESTAMP '2021-01-02 00:00:00' - TIMESTAMP '2021-01-01 00:00:00') DAYS"));
        assertEquals(Duration.ofDays(-1),
            eval("(TIMESTAMP '2021-01-01 00:00:00' - TIMESTAMP '2021-01-02 00:00:00') DAYS"));
        assertEquals(Duration.ofHours(1),
            eval("(TIMESTAMP '2021-01-01 01:00:00' - TIMESTAMP '2021-01-01 00:00:00') HOURS"));
        assertEquals(Duration.ofMinutes(1),
            eval("(TIMESTAMP '2021-01-01 00:01:00' - TIMESTAMP '2021-01-01 00:00:00') MINUTES"));
        assertEquals(Duration.ofSeconds(1),
            eval("(TIMESTAMP '2021-01-01 00:00:01' - TIMESTAMP '2021-01-01 00:00:00') SECONDS"));
        assertEquals(Duration.ofMillis(123),
            eval("(TIMESTAMP '2021-01-01 00:00:00.123' - TIMESTAMP '2021-01-01 00:00:00') SECONDS"));
        assertEquals(Period.ofYears(1),
            eval("(TIMESTAMP '2022-01-01 00:00:00' - TIMESTAMP '2021-01-01 00:00:00') YEARS"));
        assertEquals(Period.ofMonths(1),
            eval("(TIMESTAMP '2021-02-01 00:00:00' - TIMESTAMP '2021-01-01 00:00:00') MONTHS"));
        assertEquals(Period.ofMonths(-1),
            eval("(TIMESTAMP '2021-01-01 00:00:00' - TIMESTAMP '2021-02-01 00:00:00') MONTHS"));
        assertEquals(Period.ofMonths(0),
            eval("(TIMESTAMP '2021-01-20 00:00:00' - TIMESTAMP '2021-01-01 00:00:00') MONTHS"));

        // Time - time as interval.
        assertEquals(Duration.ofHours(1), eval("(TIME '02:00:00' - TIME '01:00:00') HOURS"));
        assertEquals(Duration.ofMinutes(1), eval("(TIME '00:02:00' - TIME '00:01:00') HOURS"));
        assertEquals(Duration.ofMinutes(1), eval("(TIME '00:02:00' - TIME '00:01:00') MINUTES"));
        assertEquals(Duration.ofSeconds(1), eval("(TIME '00:00:02' - TIME '00:00:01') SECONDS"));
        assertEquals(Duration.ofMillis(123), eval("(TIME '00:00:01.123' - TIME '00:00:01') SECONDS"));

        // Interval +/- interval.
        assertEquals(Duration.ofSeconds(2), eval("INTERVAL 1 SECONDS + INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL 2 SECONDS - INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(61), eval("INTERVAL 1 MINUTE + INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(59), eval("INTERVAL 1 MINUTE - INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofSeconds(59), eval("INTERVAL 1 MINUTE + INTERVAL -1 SECONDS"));
        assertEquals(Duration.ofSeconds(3723), eval("INTERVAL 1 HOUR + INTERVAL '2:3' MINUTE TO SECONDS"));
        assertEquals(Duration.ofSeconds(3477), eval("INTERVAL 1 HOUR - INTERVAL '2:3' MINUTE TO SECONDS"));
        assertEquals(Duration.ofHours(25), eval("INTERVAL 1 DAY + INTERVAL 1 HOUR"));
        assertEquals(Period.ofMonths(2), eval("INTERVAL 1 MONTH + INTERVAL 1 MONTH"));
        assertEquals(Period.ofYears(2), eval("INTERVAL 1 YEAR + INTERVAL 1 YEAR"));
        assertEquals(Period.of(1, 1, 0), eval("INTERVAL 1 YEAR + INTERVAL 1 MONTH"));
        assertEquals(Period.ofMonths(11), eval("INTERVAL 1 YEAR - INTERVAL 1 MONTH"));
        assertEquals(Period.ofMonths(11), eval("INTERVAL 1 YEAR + INTERVAL -1 MONTH"));
        assertThrows("SELECT INTERVAL 1 DAY + INTERVAL 1 MONTH", IgniteSQLException.class, "Cannot apply");

        // Interval * scalar.
        assertEquals(Duration.ofSeconds(2), eval("INTERVAL 1 SECONDS * 2"));
        assertEquals(Duration.ofSeconds(-2), eval("INTERVAL -1 SECONDS * 2"));
        assertEquals(Duration.ofMinutes(4), eval("INTERVAL 2 MINUTES * 2"));
        assertEquals(Duration.ofHours(6), eval("INTERVAL 3 HOURS * 2"));
        assertEquals(Duration.ofDays(8), eval("INTERVAL 4 DAYS * 2"));
        assertEquals(Period.ofMonths(10), eval("INTERVAL 5 MONTHS * 2"));
        assertEquals(Period.ofMonths(-10), eval("INTERVAL -5 MONTHS * 2"));
        assertEquals(Period.ofYears(12), eval("INTERVAL 6 YEARS * 2"));
        assertEquals(Period.of(2, 4, 0), eval("INTERVAL '1-2' YEAR TO MONTH * 2"));
        assertEquals(Duration.ofHours(50), eval("INTERVAL '1 1' DAY TO HOUR * 2"));
        assertEquals(Duration.ofMinutes(124), eval("INTERVAL '1:2' HOUR TO MINUTE * 2"));
        assertEquals(Duration.ofSeconds(126), eval("INTERVAL '1:3' MINUTE TO SECOND * 2"));
        assertEquals(Duration.ofSeconds(7446), eval("INTERVAL '1:2:3' HOUR TO SECOND * 2"));
        assertEquals(Duration.ofMillis(7446912), eval("INTERVAL '0 1:2:3.456' DAY TO SECOND * 2"));

        // Interval / scalar
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL 2 SECONDS / 2"));
        assertEquals(Duration.ofSeconds(-1), eval("INTERVAL -2 SECONDS / 2"));
        assertEquals(Duration.ofSeconds(30), eval("INTERVAL 1 MINUTES / 2"));
        assertEquals(Duration.ofMinutes(90), eval("INTERVAL 3 HOURS / 2"));
        assertEquals(Duration.ofDays(2), eval("INTERVAL 4 DAYS / 2"));
        assertEquals(Period.ofMonths(2), eval("INTERVAL 5 MONTHS / 2"));
        assertEquals(Period.ofMonths(-2), eval("INTERVAL -5 MONTHS / 2"));
        assertEquals(Period.of(3, 6, 0), eval("INTERVAL 7 YEARS / 2"));
        assertEquals(Period.ofMonths(7), eval("INTERVAL '1-2' YEAR TO MONTH / 2"));
        assertEquals(Duration.ofHours(13), eval("INTERVAL '1 2' DAY TO HOUR / 2"));
        assertEquals(Duration.ofMinutes(31), eval("INTERVAL '1:2' HOUR TO MINUTE / 2"));
        assertEquals(Duration.ofSeconds(31), eval("INTERVAL '1:2' MINUTE TO SECOND / 2"));
        assertEquals(Duration.ofSeconds(1862), eval("INTERVAL '1:2:4' HOUR TO SECOND / 2"));
        assertEquals(Duration.ofMillis(1862228), eval("INTERVAL '0 1:2:4.456' DAY TO SECOND / 2"));

        // Interval range overflow
        assertThrows("SELECT INTERVAL 5000000 MONTHS * 1000",
            IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT DATE '2021-01-01' + INTERVAL 999999999999 DAY",
            IgniteSQLException.class, "BIGINT overflow"); // Overflow for interval type (long).
        assertThrows("SELECT DATE '2021-01-01' + INTERVAL 3000000000 DAYS",
            IgniteSQLException.class, "INTEGER overflow"); // Overflow for date type (integer).
        assertThrows("SELECT DATE '2021-01-01' + INTERVAL -999999999 YEAR",
            IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT INTERVAL 1000000000 YEARS + INTERVAL 1 MONTH",
            IgniteSQLException.class, "INTEGER overflow");
        assertThrows("SELECT INTERVAL 100000000000 DAYS + INTERVAL 100000000000 DAYS",
            IgniteSQLException.class, "BIGINT overflow");
    }

    /**
     * Test caching of expressions by digest.
     */
    @Test
    public void testScalarCache() {
        // These expressions differs only in return data type, so digest should include also data type correctly
        // compile scalar for second expression (should not get compiled scalar from the cache).
        assertEquals(Duration.ofDays(1), eval("(DATE '2021-01-02' - DATE '2021-01-01') DAYS"));
        assertEquals(Period.ofMonths(0), eval("(DATE '2021-01-02' - DATE '2021-01-01') MONTHS"));
    }

    /**
     * Test EXTRACT function with interval data types.
     */
    @Test
    public void testExtract() {
        assertEquals(0L, eval("EXTRACT(DAY FROM INTERVAL 1 MONTH)"));
        assertEquals(2L, eval("EXTRACT(MONTH FROM INTERVAL 14 MONTHS)"));
        assertEquals(0L, eval("EXTRACT(MONTH FROM INTERVAL 1 YEAR)"));
        assertEquals(2L, eval("EXTRACT(MONTH FROM INTERVAL '1-2' YEAR TO MONTH)"));
        assertEquals(1L, eval("EXTRACT(YEAR FROM INTERVAL '1-2' YEAR TO MONTH)"));
        assertEquals(-1L, eval("EXTRACT(MONTH FROM INTERVAL -1 MONTHS)"));
        assertEquals(-1L, eval("EXTRACT(YEAR FROM INTERVAL -14 MONTHS)"));
        assertEquals(-2L, eval("EXTRACT(MONTH FROM INTERVAL -14 MONTHS)"));
        assertEquals(-20L, eval("EXTRACT(MINUTE FROM INTERVAL '-10:20' HOURS TO MINUTES)"));
        assertEquals(1L, eval("EXTRACT(DAY FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(2L, eval("EXTRACT(HOUR FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(3L, eval("EXTRACT(MINUTE FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(4L, eval("EXTRACT(SECOND FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(4567L, eval("EXTRACT(MILLISECOND FROM INTERVAL '1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-1L, eval("EXTRACT(DAY FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-2L, eval("EXTRACT(HOUR FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-3L, eval("EXTRACT(MINUTE FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-4L, eval("EXTRACT(SECOND FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));
        assertEquals(-4567L, eval("EXTRACT(MILLISECOND FROM INTERVAL '-1 2:3:4.567' DAY TO SECOND)"));

        assertThrows("SELECT EXTRACT(DOW FROM INTERVAL 1 MONTH)", IgniteSQLException.class, "Cannot apply");
        assertThrows("SELECT EXTRACT(MONTH FROM INTERVAL 1 DAY)", IgniteSQLException.class, "Cannot apply");
    }

    /** */
    public Object eval(String exp, Object... params) {
        return executeSql("SELECT " + exp, params).get(0).get(0);
    }
}
