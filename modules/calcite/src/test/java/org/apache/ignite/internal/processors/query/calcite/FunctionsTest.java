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

package org.apache.ignite.internal.processors.query.calcite;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.function.LongFunction;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test Ignite SQL functions.
 */
public class FunctionsTest extends GridCommonAbstractTest {
    /** */
    private static QueryEngine qryEngine;

    /** */
    private static final Object[] NULL_RESULT = new Object[] { null };

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGridsMultiThreaded(3);

        qryEngine = Commons.lookupComponent(((IgniteEx)grid).context(), QueryEngine.class);
    }

    /** */
    @Test
    public void testTimestampDiffWithFractionsOfSecond() {
        checkQuery("SELECT TIMESTAMPDIFF(MICROSECOND, TIMESTAMP '2022-02-01 10:30:28.000', " +
            "TIMESTAMP '2022-02-01 10:30:28.128')").returns(128000).check();

        checkQuery("SELECT TIMESTAMPDIFF(NANOSECOND, TIMESTAMP '2022-02-01 10:30:28.000', " +
            "TIMESTAMP '2022-02-01 10:30:28.128')").returns(128000000L).check();
    }

    /** */
    @Test
    public void testLength() {
        checkQuery("SELECT LENGTH('TEST')").returns(4).check();
        checkQuery("SELECT LENGTH(NULL)").returns(NULL_RESULT).check();
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

            List<List<?>> res = qryEngine.query(null, "PUBLIC", sql).get(0).getAll();

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
        checkQuery("SELECT REPLACE('12341234', '1', '55')").returns("5523455234").check();
        checkQuery("SELECT REPLACE(NULL, '1', '5')").returns(NULL_RESULT).check();
        checkQuery("SELECT REPLACE('1', NULL, '5')").returns(NULL_RESULT).check();
        checkQuery("SELECT REPLACE('11', '1', NULL)").returns(NULL_RESULT).check();
        checkQuery("SELECT REPLACE('11', '1', '')").returns("").check();
    }

    /** */
    @Test
    public void testRange() {
        checkQuery("SELECT * FROM table(system_range(1, 4))")
            .returns(1L)
            .returns(2L)
            .returns(3L)
            .returns(4L)
            .check();

        checkQuery("SELECT * FROM table(system_range(1, 4, 2))")
            .returns(1L)
            .returns(3L)
            .check();

        checkQuery("SELECT * FROM table(system_range(4, 1, -1))")
            .returns(4L)
            .returns(3L)
            .returns(2L)
            .returns(1L)
            .check();

        checkQuery("SELECT * FROM table(system_range(4, 1, -2))")
            .returns(4L)
            .returns(2L)
            .check();

        assertEquals(0, qryEngine.query(null, "PUBLIC",
            "SELECT * FROM table(system_range(4, 1))").get(0).getAll().size());

        assertEquals(0, qryEngine.query(null, "PUBLIC",
            "SELECT * FROM table(system_range(null, 1))").get(0).getAll().size());

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
        checkQuery("SELECT t._val FROM \"test\".Integer t WHERE t._val < 5 AND " +
            "t._key in (SELECT x FROM table(system_range(t._val, t._val))) ")
            .returns(0)
            .returns(1)
            .returns(2)
            .returns(3)
            .returns(4)
            .check();

        // Correlated LEFT joins.
        checkQuery("SELECT t._val FROM \"test\".Integer t WHERE t._val < 5 AND " +
            "EXISTS (SELECT x FROM table(system_range(t._val, t._val)) WHERE mod(x, 2) = 0) ")
            .returns(0)
            .returns(2)
            .returns(4)
            .check();

        checkQuery("SELECT t._val FROM \"test\".Integer t WHERE t._val < 5 AND " +
            "NOT EXISTS (SELECT x FROM table(system_range(t._val, t._val)) WHERE mod(x, 2) = 0) ")
            .returns(1)
            .returns(3)
            .check();

        assertEquals(0, qryEngine.query(null, "PUBLIC",
            "SELECT t._val FROM \"test\".Integer t WHERE " +
                "EXISTS (SELECT x FROM table(system_range(t._val, null))) ").get(0).getAll().size());

        // Non-correlated join.
        checkQuery("SELECT t._val FROM \"test\".Integer t JOIN table(system_range(1, 50)) as r ON t._key = r.x " +
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
        checkQuery("SELECT 3 % 2").returns(1).check();
        checkQuery("SELECT 4 % 2").returns(0).check();
        checkQuery("SELECT NULL % 2").returns(NULL_RESULT).check();
        checkQuery("SELECT 3 % NULL::int").returns(NULL_RESULT).check();
        checkQuery("SELECT 3 % NULL").returns(NULL_RESULT).check();
    }

    /** */
    @Test
    public void testNullFunctionArguments() {
        // Don't infer result data type from arguments (result is always INTEGER_NULLABLE).
        checkQuery("SELECT ASCII(NULL)").returns(NULL_RESULT).check();
        // Inferring result data type from first STRING argument.
        checkQuery("SELECT REPLACE(NULL, '1', '2')").returns(NULL_RESULT).check();
        // Inferring result data type from both arguments.
        checkQuery("SELECT MOD(1, null)").returns(NULL_RESULT).check();
        // Inferring result data type from first NUMERIC argument.
        checkQuery("SELECT TRUNCATE(NULL, 0)").returns(NULL_RESULT).check();
        // Inferring arguments data types and then inferring result data type from all arguments.
        checkQuery("SELECT FALSE AND NULL").returns(false).check();
    }

    /** */
    @Test
    public void testMonthnameDayname() {
        checkQuery("SELECT MONTHNAME(DATE '2021-01-01')").returns("January").check();
        checkQuery("SELECT DAYNAME(DATE '2021-01-01')").returns("Friday").check();
    }

    /** */
    @Test
    public void testTypeOf() {
        checkQuery("SELECT TYPEOF(1)").returns("INTEGER").check();
        checkQuery("SELECT TYPEOF(1.1::DOUBLE)").returns("DOUBLE").check();
        checkQuery("SELECT TYPEOF(1.1::DECIMAL(3, 2))").returns("DECIMAL(3, 2)").check();
        checkQuery("SELECT TYPEOF('a')").returns("CHAR(1)").check();
        checkQuery("SELECT TYPEOF('a'::varchar(1))").returns("VARCHAR(1)").check();
        checkQuery("SELECT TYPEOF(NULL)").returns("NULL").check();
        checkQuery("SELECT TYPEOF(NULL::VARCHAR(100))").returns("VARCHAR(100)").check();
        assertThrows("SELECT TYPEOF()", SqlValidatorException.class, "Invalid number of arguments");
        assertThrows("SELECT TYPEOF(1, 2)", SqlValidatorException.class, "Invalid number of arguments");
    }

    /** */
    @Test
    public void testRegex() {
        checkQuery("SELECT 'abcd' ~ 'ab[cd]'").returns(true).check();
        checkQuery("SELECT 'abcd' ~ 'ab[cd]$'").returns(false).check();
        checkQuery("SELECT 'abcd' ~ 'ab[CD]'").returns(false).check();
        checkQuery("SELECT 'abcd' ~* 'ab[cd]'").returns(true).check();
        checkQuery("SELECT 'abcd' ~* 'ab[cd]$'").returns(false).check();
        checkQuery("SELECT 'abcd' ~* 'ab[CD]'").returns(true).check();
        checkQuery("SELECT 'abcd' !~ 'ab[cd]'").returns(false).check();
        checkQuery("SELECT 'abcd' !~ 'ab[cd]$'").returns(true).check();
        checkQuery("SELECT 'abcd' !~ 'ab[CD]'").returns(true).check();
        checkQuery("SELECT 'abcd' !~* 'ab[cd]'").returns(false).check();
        checkQuery("SELECT 'abcd' !~* 'ab[cd]$'").returns(true).check();
        checkQuery("SELECT 'abcd' !~* 'ab[CD]'").returns(false).check();
        checkQuery("SELECT null ~ 'ab[cd]'").returns(NULL_RESULT).check();
        checkQuery("SELECT 'abcd' ~ null").returns(NULL_RESULT).check();
        checkQuery("SELECT null ~ null").returns(NULL_RESULT).check();
        checkQuery("SELECT null ~* 'ab[cd]'").returns(NULL_RESULT).check();
        checkQuery("SELECT 'abcd' ~* null").returns(NULL_RESULT).check();
        checkQuery("SELECT null ~* null").returns(NULL_RESULT).check();
        checkQuery("SELECT null !~ 'ab[cd]'").returns(NULL_RESULT).check();
        checkQuery("SELECT 'abcd' !~ null").returns(NULL_RESULT).check();
        checkQuery("SELECT null !~ null").returns(NULL_RESULT).check();
        checkQuery("SELECT null !~* 'ab[cd]'").returns(NULL_RESULT).check();
        checkQuery("SELECT 'abcd' !~* null").returns(NULL_RESULT).check();
        checkQuery("SELECT null !~* null").returns(NULL_RESULT).check();
        assertThrows("SELECT 'abcd' ~ '[a-z'", IgniteSQLException.class, null);
    }

    /** */
    private void assertThrows(String qry, Class<? extends Throwable> cls, String msg) {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> qryEngine.query(null, "PUBLIC", qry).get(0).getAll(),
            cls,
            msg
        );
    }

    /** */
    private QueryChecker checkQuery(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return qryEngine;
            }
        };
    }
}
