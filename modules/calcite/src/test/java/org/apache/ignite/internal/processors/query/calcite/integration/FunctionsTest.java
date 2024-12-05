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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Test Ignite SQL functions.
 */
public class FunctionsTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void testTimestampDiffWithFractionsOfSecond() {
        assertQuery("SELECT TIMESTAMPDIFF(MICROSECOND, TIMESTAMP '2022-02-01 10:30:28.000', " +
            "TIMESTAMP '2022-02-01 10:30:28.128')").returns(128000).check();

        assertQuery("SELECT TIMESTAMPDIFF(NANOSECOND, TIMESTAMP '2022-02-01 10:30:28.000', " +
            "TIMESTAMP '2022-02-01 10:30:28.128')").returns(128000000L).check();
    }

    /** */
    @Test
    public void testLength() {
        assertQuery("SELECT LENGTH('TEST')").returns(4).check();
        assertQuery("SELECT LENGTH(NULL)").returns(NULL_RESULT).check();
    }

    /** */
    @Test
    public void testQueryEngine() {
        assertQuery("SELECT QUERY_ENGINE()").returns(CalciteQueryEngineConfiguration.ENGINE_NAME).check();
    }

    /** */
    @Test
    public void testBitwiseOperationsWithTable() {
        try {
            sql("CREATE TABLE TBL(i INT PRIMARY KEY, s SMALLINT, l BIGINT)");

            for (int i = 0; i < 100; ++i)
                sql("INSERT INTO TBL values (?, ?, ?)", i, i, i);

            sql("INSERT INTO TBL values (?, ?, ?)", Short.MAX_VALUE + 1, Short.MAX_VALUE, Short.MAX_VALUE);

            assertQuery("SELECT BITAND(i + 1, i + 1) FROM TBL WHERE i=0").returns(1).check();
            assertQuery("SELECT BITAND(s, s) FROM TBL WHERE i=1").returns((short)1).check();
            assertQuery("SELECT BITAND((SELECT l FROM TBL WHERE i=3), (SELECT l FROM TBL WHERE i=1))").returns(1L).check();
            assertQuery("SELECT BITOR((SELECT s FROM TBL WHERE i=?), (SELECT i FROM TBL WHERE i=?))").withParams(14, 1)
                .returns(15).check();

            assertQuery("SELECT BITAND((SELECT s FROM TBL WHERE s=3), (SELECT l FROM TBL WHERE i=1))").returns(1L).check();

            assertQuery("SELECT BITXOR(1000::BIGINT, i) FROM TBL WHERE i=93").returns(949L).check();
            assertQuery("SELECT BITAND(?, i) FROM TBL WHERE i=73").withParams(NULL_RESULT).returns(NULL_RESULT).check();
            assertQuery("SELECT BITAND(l, ?) FROM TBL WHERE l=45").withParams(NULL_RESULT).returns(NULL_RESULT).check();
            assertQuery("SELECT BITAND(?, s) FROM TBL WHERE s=40").withParams(NULL_RESULT).returns(NULL_RESULT).check();

            assertQuery("SELECT BITAND(i, s) FROM TBL WHERE l=?").withParams(Short.MAX_VALUE).returns(0).check();
            assertQuery("SELECT BITOR(i, s) FROM TBL WHERE l=?").withParams(Short.MAX_VALUE).returns(65535).check();
            assertQuery("SELECT BITXOR(i, s) FROM TBL WHERE l=?").withParams(Short.MAX_VALUE).returns(65535).check();
        }
        finally {
            sql("DROP TABLE if EXISTS TBL");
        }
    }

    /** */
    @Test
    public void testBitwiseOperations() {
        doTestBitwiseOperations(false);

        doTestBitwiseOperations(true);
    }

    /** */
    private void doTestBitwiseOperations(boolean dynamic) {
        for (List<Object> paramSet : bitwiseParams(dynamic)) {
            assert paramSet.size() == 6;

            int idx = 0;

            String op = paramSet.get(idx++).toString();
            Object p1 = paramSet.get(idx++);
            String cast1 = (String)paramSet.get(idx++);
            Object p2 = paramSet.get(idx++);
            String cast2 = (String)paramSet.get(idx++);
            Object res = paramSet.get(idx);

            cast1 = cast1 == null ? "" : "::" + cast1;
            cast2 = cast2 == null ? "" : "::" + cast2;

            log.info("Op: " + op + ", dynamic=" + dynamic + ", p1=" + p1 + ", p2=" + p2 + ", expected=" + res);

            if (dynamic) {
                String sql = "SELECT BIT" + op + "(?" + cast1 + ", ?" + cast2 + ')';

                if (res instanceof Exception)
                    assertThrows(sql, (Class<? extends Exception>)res.getClass(), ((Throwable)res).getMessage(), p1, p2);
                else
                    assertQuery(sql).withParams(p1, p2).returns(res).check();
            }
            else {
                String sql = "SELECT BIT" + op + '(' + p1 + cast1 + ", " + p2 + cast2 + ')';

                if (res instanceof Exception)
                    assertThrows(sql, (Class<? extends Exception>)res.getClass(), ((Throwable)res).getMessage());
                else
                    assertQuery(sql).returns(res).check();
            }
        }
    }

    /** Bitwise operation params: operation, param1, cast1, param2, cast2, result. */
    private Iterable<List<Object>> bitwiseParams(boolean dynamic) {
        List<List<Object>> res = new ArrayList<>(100);

        SqlValidatorException andErr = new SqlValidatorException("Cannot apply 'BITAND' to arguments of type", null);
        SqlValidatorException orErr = new SqlValidatorException("Cannot apply 'BITOR' to arguments of type", null);
        SqlValidatorException xorErr = new SqlValidatorException("Cannot apply 'BITXOR' to arguments of type", null);

        // BITAND
        res.add(F.asList("AND", 1, null, 1.0, null, andErr));
        res.add(F.asList("AND", 1.0, null, 1, null, andErr));
        res.add(F.asList("AND", 1, null, 1.0f, null, andErr));
        res.add(F.asList("AND", 1.0f, null, 1, null, andErr));
        res.add(F.asList("AND", null, null, null, null, null));
        res.add(F.asList("AND", 1, null, 1, null, 1));
        res.add(F.asList("AND", 1, null, 0, null, 0));
        res.add(F.asList("AND", 0, null, 1, null, 0));
        res.add(F.asList("AND", 1, null, 1, "BIGINT", 1L));
        res.add(F.asList("AND", 1, "TINYINT", 1, "INT", 1));
        res.add(F.asList("AND", 1, "TINYINT", 1, "SMALLINT", (short)1));
        res.add(F.asList("AND", 0, "TINYINT", 0, "TINYINT", (byte)0));
        res.add(F.asList("AND", 1, "TINYINT", 1, "SMALLINT", (short)1));
        res.add(F.asList("AND", 15, null, 7, null, 7));
        res.add(F.asList("AND", -1, null, 1, null, 1));
        res.add(F.asList("AND", (short)32767, null, 65535, null, 32767));
        res.add(F.asList("AND", null, null, 1, null, null));
        res.add(F.asList("AND", 1, "SMALLINT", null, null, null));
        // BITOR
        res.add(F.asList("OR", 1, null, 1.0, null, orErr));
        res.add(F.asList("OR", 1.0, null, 1, null, orErr));
        res.add(F.asList("OR", 1, null, 1.0f, null, orErr));
        res.add(F.asList("OR", 1.0f, null, 1, null, orErr));
        res.add(F.asList("OR", 1, null, 1, null, 1));
        res.add(F.asList("OR", 1, null, 0, null, 1));
        res.add(F.asList("OR", 0, null, 1, null, 1));
        res.add(F.asList("OR", 1, null, 1, "BIGINT", 1L));
        res.add(F.asList("OR", 1, "TINYINT", 1, "INT", 1));
        res.add(F.asList("OR", 1, "TINYINT", 1, "SMALLINT", (short)1));
        res.add(F.asList("OR", 0, "TINYINT", 0, "TINYINT", (byte)0));
        res.add(F.asList("OR", 1, "TINYINT", 1, "SMALLINT", (short)1));
        res.add(F.asList("OR", 8, null, 7, null, 15));
        res.add(F.asList("OR", -1, null, 1, null, -1));
        res.add(F.asList("OR", (short)32767, null, 65535, null, 65535));
        res.add(F.asList("OR", (short)32767, null, 65536, null, 98303));
        res.add(F.asList("OR", null, null, 1, null, null));
        res.add(F.asList("OR", 1, null, null, null, null));
        // BITXOR
        res.add(F.asList("XOR", 1, null, 1.0, null, xorErr));
        res.add(F.asList("XOR", 1.0, null, 1, null, xorErr));
        res.add(F.asList("XOR", 1, null, 1.0f, null, xorErr));
        res.add(F.asList("XOR", 1.0f, null, 1, null, xorErr));
        res.add(F.asList("XOR", 1, null, 1, null, 0));
        res.add(F.asList("XOR", 1, null, 0, null, 1));
        res.add(F.asList("XOR", 0, null, 1, null, 1));
        res.add(F.asList("XOR", 1, null, 1, "BIGINT", 0L));
        res.add(F.asList("XOR", 1, "TINYINT", 1, "INT", 0));
        res.add(F.asList("XOR", 1, "TINYINT", 1, "SMALLINT", (short)0));
        res.add(F.asList("XOR", 0, "TINYINT", 0, "TINYINT", (byte)0));
        res.add(F.asList("XOR", 1, "TINYINT", 1, "SMALLINT", (short)0));
        res.add(F.asList("XOR", 8, null, 7, null, 15));
        res.add(F.asList("XOR", -1, null, 1, null, -2));
        res.add(F.asList("XOR", (short)32767, null, 65535, null, 32768));
        res.add(F.asList("XOR", (short)32767, null, 65536, null, 98303));
        res.add(F.asList("XOR", null, null, 1, "TINYINT", null));
        res.add(F.asList("XOR", 1, null, null, null, null));

        return res;
    }

    /** */
    @Test
    public void testCurrentDateTimeTimeStamp() {
        checkDateTimeQuery("SELECT CURRENT_DATE", Date::new);
        checkDateTimeQuery("SELECT CURRENT_TIME", Time::new);
        checkDateTimeQuery("SELECT CURRENT_TIMESTAMP", Timestamp::new);
        checkDateTimeQuery("SELECT LOCALTIME", Time::new);
        checkDateTimeQuery("SELECT LOCALTIMESTAMP", Timestamp::new);
        checkDateTimeQuery("SELECT {fn CURDATE()}", Date::new);
        checkDateTimeQuery("SELECT {fn CURTIME()}", Time::new);
        checkDateTimeQuery("SELECT {fn NOW()}", Timestamp::new);
    }

    /** */
    private <T> void checkDateTimeQuery(String sql, LongFunction<T> func) {
        while (true) {
            long tsBeg = U.currentTimeMillis();

            List<List<?>> res = sql(sql);

            long tsEnd = U.currentTimeMillis();

            assertEquals(1, res.size());
            assertEquals(1, res.get(0).size());

            String strBeg = func.apply(tsBeg).toString();
            String strEnd = func.apply(tsEnd).toString();

            // Date changed, time comparison may return wrong result.
            if (strBeg.compareTo(strEnd) > 0)
                continue;

            String strRes = res.get(0).get(0).toString();

            assertTrue(strBeg.compareTo(strRes) <= 0);
            assertTrue(strEnd.compareTo(strRes) >= 0);

            return;
        }
    }

    /** */
    @Test
    public void testReplace() {
        assertQuery("SELECT REPLACE('12341234', '1', '55')").returns("5523455234").check();
        assertQuery("SELECT REPLACE(NULL, '1', '5')").returns(NULL_RESULT).check();
        assertQuery("SELECT REPLACE('1', NULL, '5')").returns(NULL_RESULT).check();
        assertQuery("SELECT REPLACE('11', '1', NULL)").returns(NULL_RESULT).check();
        assertQuery("SELECT REPLACE('11', '1', '')").returns("").check();
    }

    /** */
    @Test
    public void testRange() {
        assertQuery("SELECT * FROM table(system_range(1, 4))")
            .returns(1L)
            .returns(2L)
            .returns(3L)
            .returns(4L)
            .check();

        assertQuery("SELECT * FROM table(system_range(1, 4, 2))")
            .returns(1L)
            .returns(3L)
            .check();

        assertQuery("SELECT * FROM table(system_range(4, 1, -1))")
            .returns(4L)
            .returns(3L)
            .returns(2L)
            .returns(1L)
            .check();

        assertQuery("SELECT * FROM table(system_range(4, 1, -2))")
            .returns(4L)
            .returns(2L)
            .check();

        assertEquals(0, sql("SELECT * FROM table(system_range(4, 1))").size());

        assertEquals(0, sql("SELECT * FROM table(system_range(null, 1))").size());

        assertThrows("SELECT * FROM table(system_range(1, 1, 0))", IllegalArgumentException.class,
            "Increment can't be 0");
    }

    /**
     * Important! Don`t change query call sequence in this test. This also tests correctness of
     * {@link org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactoryImpl#SCALAR_CACHE} usage.
     */
    @Test
    public void testRangeWithCache() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).getOrCreateCache(
            new CacheConfiguration<Integer, Integer>("test")
                .setBackups(1)
                .setIndexedTypes(Integer.class, Integer.class)
        );

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        awaitPartitionMapExchange();

        // Correlated INNER join.
        assertQuery("SELECT t._val FROM \"test\".Integer t WHERE t._val < 5 AND " +
            "t._key in (SELECT x FROM table(system_range(t._val, t._val))) ")
            .returns(0)
            .returns(1)
            .returns(2)
            .returns(3)
            .returns(4)
            .check();

        // Correlated LEFT joins.
        assertQuery("SELECT t._val FROM \"test\".Integer t WHERE t._val < 5 AND " +
            "EXISTS (SELECT x FROM table(system_range(t._val, t._val)) WHERE mod(x, 2) = 0) ")
            .returns(0)
            .returns(2)
            .returns(4)
            .check();

        assertQuery("SELECT t._val FROM \"test\".Integer t WHERE t._val < 5 AND " +
            "NOT EXISTS (SELECT x FROM table(system_range(t._val, t._val)) WHERE mod(x, 2) = 0) ")
            .returns(1)
            .returns(3)
            .check();

        assertEquals(0, sql("SELECT t._val FROM \"test\".Integer t WHERE " +
                "EXISTS (SELECT x FROM table(system_range(t._val, null))) ").size());

        // Non-correlated join.
        assertQuery("SELECT t._val FROM \"test\".Integer t JOIN table(system_range(1, 50)) as r ON t._key = r.x " +
            "WHERE mod(r.x, 10) = 0")
            .returns(10)
            .returns(20)
            .returns(30)
            .returns(40)
            .returns(50)
            .check();
    }

    /** */
    @Test
    public void testPercentRemainder() {
        assertQuery("SELECT 3 % 2").returns(1).check();
        assertQuery("SELECT 4 % 2").returns(0).check();
        assertQuery("SELECT NULL % 2").returns(NULL_RESULT).check();
        assertQuery("SELECT 3 % NULL::int").returns(NULL_RESULT).check();
        assertQuery("SELECT 3 % NULL").returns(NULL_RESULT).check();
    }

    /** */
    @Test
    public void testNullFunctionArguments() {
        // Don't infer result data type from arguments (result is always INTEGER_NULLABLE).
        assertQuery("SELECT ASCII(NULL)").returns(NULL_RESULT).check();
        // Inferring result data type from first STRING argument.
        assertQuery("SELECT REPLACE(NULL, '1', '2')").returns(NULL_RESULT).check();
        // Inferring result data type from both arguments.
        assertQuery("SELECT MOD(1, null)").returns(NULL_RESULT).check();
        // Inferring result data type from first NUMERIC argument.
        assertQuery("SELECT TRUNCATE(NULL, 0)").returns(NULL_RESULT).check();
        // Inferring arguments data types and then inferring result data type from all arguments.
        assertQuery("SELECT FALSE AND NULL").returns(false).check();
    }

    /** */
    @Test
    public void testMonthnameDayname() {
        assertQuery("SELECT MONTHNAME(DATE '2021-01-01')").returns("January").check();
        assertQuery("SELECT DAYNAME(DATE '2021-01-01')").returns("Friday").check();
    }

    /** */
    @Test
    public void testTypeOf() {
        assertQuery("SELECT TYPEOF(1)").returns("INTEGER").check();
        assertQuery("SELECT TYPEOF(1.1::DOUBLE)").returns("DOUBLE").check();
        assertQuery("SELECT TYPEOF(1.1::DECIMAL(3, 2))").returns("DECIMAL(3, 2)").check();
        assertQuery("SELECT TYPEOF('a')").returns("CHAR(1)").check();
        assertQuery("SELECT TYPEOF('a'::varchar(1))").returns("VARCHAR(1)").check();
        assertQuery("SELECT TYPEOF(NULL)").returns("NULL").check();
        assertQuery("SELECT TYPEOF(NULL::VARCHAR(100))").returns("VARCHAR(100)").check();
        assertThrows("SELECT TYPEOF()", SqlValidatorException.class, "Invalid number of arguments");
        assertThrows("SELECT TYPEOF(1, 2)", SqlValidatorException.class, "Invalid number of arguments");
        assertThrows("SELECT TYPEOF(CAST('NONE' as INTEGER))", IgniteSQLException.class,
            "is neither a decimal digit number, decimal point");
    }

    /** */
    @Test
    public void testRegex() {
        assertQuery("SELECT 'abcd' ~ 'ab[cd]'").returns(true).check();
        assertQuery("SELECT 'abcd' ~ 'ab[cd]$'").returns(false).check();
        assertQuery("SELECT 'abcd' ~ 'ab[CD]'").returns(false).check();
        assertQuery("SELECT 'abcd' ~* 'ab[cd]'").returns(true).check();
        assertQuery("SELECT 'abcd' ~* 'ab[cd]$'").returns(false).check();
        assertQuery("SELECT 'abcd' ~* 'ab[CD]'").returns(true).check();
        assertQuery("SELECT 'abcd' !~ 'ab[cd]'").returns(false).check();
        assertQuery("SELECT 'abcd' !~ 'ab[cd]$'").returns(true).check();
        assertQuery("SELECT 'abcd' !~ 'ab[CD]'").returns(true).check();
        assertQuery("SELECT 'abcd' !~* 'ab[cd]'").returns(false).check();
        assertQuery("SELECT 'abcd' !~* 'ab[cd]$'").returns(true).check();
        assertQuery("SELECT 'abcd' !~* 'ab[CD]'").returns(false).check();
        assertQuery("SELECT null ~ 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' ~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null ~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null ~* 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' ~* null").returns(NULL_RESULT).check();
        assertQuery("SELECT null ~* null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~ 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' !~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~ null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~* 'ab[cd]'").returns(NULL_RESULT).check();
        assertQuery("SELECT 'abcd' !~* null").returns(NULL_RESULT).check();
        assertQuery("SELECT null !~* null").returns(NULL_RESULT).check();
        assertThrows("SELECT 'abcd' ~ '[a-z'", IgniteSQLException.class, null);
    }

    /** */
    @Test
    public void testCastToBoolean() {
        assertQuery("SELECT CAST(CAST(null AS DOUBLE) AS BOOLEAN)").returns(NULL_RESULT).check();
        assertQuery("SELECT CAST(CAST('1' AS DOUBLE) AS BOOLEAN)").returns(true).check();
        assertQuery("SELECT CAST(1.0 AS BOOLEAN)").returns(true).check();
        assertQuery("SELECT CAST(0.1 AS BOOLEAN)").returns(true).check();
        assertQuery("SELECT CAST(1 AS BOOLEAN)").returns(true).check();
        assertQuery("SELECT CAST(CAST('0' AS DOUBLE) AS BOOLEAN)").returns(false).check();
        assertQuery("SELECT CAST(0.0 AS BOOLEAN)").returns(false).check();
        assertQuery("SELECT CAST(0 AS BOOLEAN)").returns(false).check();
        assertQuery("SELECT CAST(CAST(? AS INT) AS BOOLEAN)").withParams(0).returns(false).check();
        assertQuery("SELECT CAST(CAST(? AS INT) AS BOOLEAN)").withParams(1).returns(true).check();
        assertQuery("SELECT CAST(CAST(? AS INT) AS BOOLEAN)").withParams(NULL_RESULT).returns(NULL_RESULT).check();
        assertQuery("SELECT CAST(CAST(? AS DOUBLE) AS BOOLEAN)").withParams(0.0d).returns(false).check();
        assertQuery("SELECT CAST(CAST(? AS DOUBLE) AS BOOLEAN)").withParams(1.0d).returns(true).check();
        assertQuery("SELECT CAST(CAST(? AS DOUBLE) AS BOOLEAN)").withParams(NULL_RESULT).returns(NULL_RESULT).check();
        assertQuery("SELECT CAST(CAST(? AS DECIMAL(2, 1)) AS BOOLEAN)")
            .withParams(BigDecimal.valueOf(0, 1)).returns(false).check();
        assertQuery("SELECT CAST(CAST(? AS DECIMAL(2, 1)) AS BOOLEAN)")
            .withParams(BigDecimal.valueOf(10, 1)).returns(true).check();
        assertQuery("SELECT CAST(CAST(? AS DECIMAL(2, 1)) AS BOOLEAN)")
            .withParams(NULL_RESULT).returns(NULL_RESULT).check();
    }

    /** Tests NVL with different parameters data types. */
    @Test
    public void testNvl() {
        // Result type is the least restrictive type for parameters.
        assertQuery("select nvl('1', 2)").returns("1").check();
        assertQuery("select nvl(1, '2')").returns("1").check();
        assertQuery("select nvl(1, 2.0)").returns(new BigDecimal("1.0")).check();
        assertQuery("select nvl(1, 2::DOUBLE)").returns(1d).check();
        assertQuery("select nvl(1::TINYINT, 2::SMALLINT)").returns((short)1).check();
        assertQuery("select nvl(1.0, '2')").returns("1.0").check();
        assertQuery("select nvl(null, 2)").returns(2).check();
        assertQuery("select nvl(null, '2')").returns("2").check();
        assertQuery("select nvl(null, null)").returns(NULL_RESULT).check();
        assertQuery("select nvl(?, ?)").withParams("1", 2).returns("1").check();
        assertQuery("select nvl(?, ?)").withParams(1, "2").returns("1").check();
        assertQuery("select nvl(?, ?)").withParams(1, 2d).returns(1d).check();
        assertQuery("select nvl(?, ?)").withParams(null, 2).returns(2).check();
        assertQuery("select nvl(?, ?)").withParams(null, "2").returns("2").check();
        assertQuery("select nvl(?, ?)").withParams(null, null).returns(NULL_RESULT).check();
    }

    /** Tests DECODE with different parameters data types. */
    @Test
    public void testDecode() {
        // Result type is the least restrictive type for then-else parameters.
        // Type cast rules for comparison are identical to rules for WHERE clause.
        assertQuery("select decode('1', 1, '1', '2')").returns("1").check();
        assertQuery("select decode('2', 1, '1', 2, 2)").returns("2").check();
        assertQuery("select decode(1, '1', 1, 2)").returns(1).check();
        assertQuery("select decode(2, '1', 1, '2', 2)").returns(2).check();
        assertQuery("select decode(2, '1', 1, '2', 2, '3')").returns("2").check();
        assertQuery("select decode(2, '1', 1, '2', 2::DOUBLE)").returns(2.0).check();
        assertQuery("select decode(1.0, 1, 1, 2)").returns(1).check();
        assertQuery("select decode(1.1, 1, 1, 2)").returns(2).check();
        assertQuery("select decode(1, 1.0, 1, 2)").returns(1).check();
        assertQuery("select decode(1, 1.1, 1, 2)").returns(2).check();
        assertQuery("select decode(1, 1::DOUBLE, 1, 2)").returns(1).check();
        assertQuery("select decode('1', 1::DOUBLE, 1, 2)").returns(1).check();
        assertQuery("select decode(1.0, '1', 1, 2)").returns(1).check();
        assertQuery("select decode('1', 1.0, 1, 2)").returns(1).check();
        assertQuery("select decode(null, null, 1, 2)").returns(1).check();
        assertQuery("select decode(null, 1, 1, 2)").returns(2).check();
        assertQuery("select decode(1, null, 1, 2)").returns(2).check();
    }
}
