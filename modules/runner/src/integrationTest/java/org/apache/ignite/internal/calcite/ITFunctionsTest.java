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

package org.apache.ignite.internal.calcite;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.function.LongFunction;

import org.apache.ignite.lang.IgniteInternalException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Ignite SQL functions.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ITFunctionsTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void testLength() {
        assertQuery("SELECT LENGTH('TEST')").returns(4).check();
        assertQuery("SELECT LENGTH(NULL)").returns(new Object[] { null }).check();
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
            long tsBeg = System.currentTimeMillis();

            List<List<?>> res = sql(sql);

            long tsEnd = System.currentTimeMillis();

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

        IgniteInternalException ex = assertThrows(IgniteInternalException.class,
            () -> sql("SELECT * FROM table(system_range(1, 1, 0))"));

        assertEquals("Increment can't be 0", ex.getMessage());
    }

//    /**
//     * Important! Don`t change query call sequence in this test. This also tests correctness of
//     * {@link org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactoryImpl#SCALAR_CACHE} usage.
//     */
//    @Test
//    public void testRangeWithCache() throws Exception {
//        IgniteCache<Integer, Integer> cache = grid(0).getOrCreateCache(
//            new CacheConfiguration<Integer, Integer>("test")
//                .setBackups(1)
//                .setIndexedTypes(Integer.class, Integer.class)
//        );
//
//        for (int i = 0; i < 100; i++)
//            cache.put(i, i);
//
//        awaitPartitionMapExchange();
//
//        // Correlated INNER join.
//        assertQuery("SELECT t._val FROM \"test\".Integer t WHERE t._val < 5 AND " +
//            "t._key in (SELECT x FROM table(system_range(t._val, t._val))) ")
//            .returns(0)
//            .returns(1)
//            .returns(2)
//            .returns(3)
//            .returns(4)
//            .check();
//
//        // Correlated LEFT joins.
//        assertQuery("SELECT t._val FROM \"test\".Integer t WHERE t._val < 5 AND " +
//            "EXISTS (SELECT x FROM table(system_range(t._val, t._val)) WHERE mod(x, 2) = 0) ")
//            .returns(0)
//            .returns(2)
//            .returns(4)
//            .check();
//
//        assertQuery("SELECT t._val FROM \"test\".Integer t WHERE t._val < 5 AND " +
//            "NOT EXISTS (SELECT x FROM table(system_range(t._val, t._val)) WHERE mod(x, 2) = 0) ")
//            .returns(1)
//            .returns(3)
//            .check();
//
//        assertEquals(0, qryEngine.query(null, "PUBLIC",
//            "SELECT t._val FROM \"test\".Integer t WHERE " +
//                "EXISTS (SELECT x FROM table(system_range(t._val, null))) ").get(0).getAll().size());
//
//        // Non-correlated join.
//        assertQuery("SELECT t._val FROM \"test\".Integer t JOIN table(system_range(1, 50)) as r ON t._key = r.x " +
//            "WHERE mod(r.x, 10) = 0")
//            .returns(10)
//            .returns(20)
//            .returns(30)
//            .returns(40)
//            .returns(50)
//            .check();
//    }

    /** */
    @Test
    public void testPercentRemainder() {
        assertQuery("SELECT 3 % 2").returns(1).check();
        assertQuery("SELECT 4 % 2").returns(0).check();
        assertQuery("SELECT NULL % 2").returns(new Object[] { null }).check();
        assertQuery("SELECT 3 % NULL::int").returns(new Object[] { null }).check();
    }
}
