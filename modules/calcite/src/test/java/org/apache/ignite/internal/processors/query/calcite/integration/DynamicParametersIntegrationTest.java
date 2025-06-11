/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 *  Dynamic parameters types inference test.
 */
public class DynamicParametersIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        sql("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val1 INTEGER NOT NULL, val2 INTEGER)");
        sql("CREATE TABLE t2 (id2 INTEGER PRIMARY KEY, val3 INTEGER NOT NULL, val4 INTEGER)");
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        sql("DROP TABLE IF EXISTS t1");
        sql("DROP TABLE IF EXISTS t2");

        super.afterTest();
    }

    /** */
    @Test
    public void testMetadataTypesForDynamicParameters() {
        List<Object> values = F.asList(
            "test",
            BigDecimal.valueOf(1),
            1,
            1L,
            1f,
            1d,
            UUID.randomUUID(),
            Duration.ofSeconds(1),
            Date.valueOf("2022-01-01"),
            Timestamp.valueOf("2022-01-01 01:01:01"),
            Time.valueOf("02:01:01"),
            Period.ofMonths(1)
        );

        List<String> types = F.asList("VARCHAR", "DECIMAL(32767, 0)", "INTEGER", "BIGINT", "REAL", "DOUBLE",
            "UUID", "INTERVAL DAY TO SECOND", "DATE", "TIMESTAMP(3)", "TIME(0)", "INTERVAL YEAR TO MONTH");

        for (int i = 0; i < values.size(); i++) {
            assertQuery("SELECT typeof(?)").withParams(values.get(i)).returns(types.get(i)).check();
            assertQuery("SELECT ?").withParams(values.get(i)).returns(values.get(i)).check();
        }
    }

    /** */
    @Test
    public void testMissedValue() {
        assertUnexpectedNumberOfParameters("SELECT ?");

        assertUnexpectedNumberOfParameters("SELECT ?, ?", "arg0");
    }

    /** */
    @Test
    public void testCasts() {
        assertQuery("SELECT CAST(? as INTEGER)").withParams('1').returns(1).check();
        assertQuery("SELECT ?::INTEGER").withParams('1').returns(1).check();
        assertQuery("SELECT ?::VARCHAR").withParams(1).returns("1").check();
        assertQuery("SELECT CAST(? as VARCHAR)").withParams(1).returns("1").check();

        createAndPopulateTable();

        IgniteCache<Integer, Employer> cache = client.getOrCreateCache(TABLE_NAME);

        cache.put(cache.size(), new Employer("15", 15d));

        assertQuery("SELECT name FROM Person WHERE id=?::INTEGER").withParams("2").returns("Ilya").check();
        assertQuery("SELECT name FROM Person WHERE id=CAST(? as INTEGER)").withParams("2").returns("Ilya").check();

        assertQuery("SELECT id FROM Person WHERE name=CAST(? as VARCHAR)").withParams(15).returns(5).check();
        assertQuery("SELECT id FROM Person WHERE name IN (?::VARCHAR)").withParams(15).returns(5).check();
        assertQuery("SELECT name FROM Person WHERE id IN (?::INTEGER)").withParams("2").returns("Ilya").check();
        assertQuery("SELECT name FROM Person WHERE id IN (?::INTEGER, ?::INTEGER)").withParams("2", "3")
            .returns("Ilya").returns("Roma").check();

        assertQuery("SELECT count(*) FROM Person WHERE ? IS NOT NULL").withParams(1).returns(6L).check();
        assertQuery("SELECT count(*) FROM Person WHERE ? IS NOT NULL").withParams("abc").returns(6L).check();
        assertQuery("SELECT count(*) FROM Person WHERE ? IS NOT NULL").withParams(new Object[] { null }).returns(0L).check();

        assertQuery("SELECT count(*) FROM Person WHERE ? IS NULL").withParams(1).returns(0L).check();
        assertQuery("SELECT count(*) FROM Person WHERE ? IS NULL").withParams("abc").returns(0L).check();
        assertQuery("SELECT count(*) FROM Person WHERE ? IS NULL").withParams(new Object[] {null}).returns(6L).check();
    }

    /** */
    @Test
    public void testDynamicParameters() {
        assertQuery("select 1 + ?").withParams(1).returns(2).check();
        assertQuery("select ? + 1").withParams(1).returns(2).check();
        assertQuery("select 1 + CAST(? AS INTEGER)").withParams(2L).returns(3).check();
        assertQuery("select CAST(? AS INTEGER) + 1").withParams(2L).returns(3).check();
        assertQuery("select 1 + ?").withParams(1L).returns(2L).check();
        assertQuery("select ? + 1").withParams(1L).returns(2L).check();
        assertQuery("select 1 + ?").withParams(new BigDecimal("2")).returns(new BigDecimal(3)).check();
        assertQuery("select ? + 1").withParams(new BigDecimal("2")).returns(new BigDecimal(3)).check();

        assertQuery("SELECT COALESCE(?, ?)").withParams("a", 10).returns("a").check();
        assertQuery("SELECT COALESCE(null, ?)").withParams(13).returns(13).check();
        assertQuery("SELECT LOWER(?)").withParams("ASD").returns("asd").check();
        assertQuery("SELECT ?").withParams("asd").returns("asd").check();
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2, 2, "TeSt").returns(4, "test").check();
        assertQuery("SELECT LOWER(?), ? + ? ").withParams("TeSt", 2, 2).returns("test", 4).check();
        assertQuery("SELECT POWER(?, ?)").withParams(2, 3).returns(8d).check();
        assertQuery("SELECT SQRT(?)").withParams(4d).returns(2d).check();
        assertQuery("SELECT ? % ?").withParams(11, 10).returns(1).check();

        assertQuery("SELECT LAST_DAY(?)").withParams(Date.valueOf("2022-01-01"))
            .returns(Date.valueOf("2022-01-31")).check();
        assertQuery("SELECT LAST_DAY(?)").withParams(LocalDate.parse("2022-01-01"))
            .returns(Date.valueOf("2022-01-31")).check();

        createAndPopulateTable();

        assertQuery("SELECT name LIKE '%' || ? || '%' FROM person where name is not null").withParams("go")
            .returns(true).returns(false).returns(false).returns(false).check();

        assertQuery("SELECT id FROM person WHERE name LIKE ? ORDER BY id LIMIT ?").withParams("I%", 1)
            .returns(0).check();

        assertQuery("SELECT id FROM person WHERE name LIKE ? ORDER BY id LIMIT ? OFFSET ?").withParams("I%", 1, 1)
            .returns(2).check();

        assertQuery("SELECT id FROM person WHERE salary<? and id>?").withParams(15, 1)
            .returns(3).returns(4).check();
    }

    /** Tests the same query with different type of parameters to cover case with check right plans cache work. **/
    @Test
    public void testWithDifferentParametersTypes() {
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2, 2, "TeSt").returns(4, "test").check();
        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2.2, 2.2, "TeSt").returns(4.4, "test").check();

        assertQuery("SELECT COALESCE(?, ?)").withParams(null, null).returns(NULL_RESULT).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(null, 13).returns(13).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams("a", 10).returns("a").check();
        assertQuery("SELECT COALESCE(?, ?)").withParams("a", "b").returns("a").check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(22, 33).returns(22).check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(12.2, "b").returns("12.2").check();
        assertQuery("SELECT COALESCE(?, ?)").withParams(12, "b").returns("12").check();
        assertQuery("SELECT UPPER(TYPEOF(?))").withParams(1).returns("INTEGER").check();
        assertQuery("SELECT UPPER(TYPEOF(?))").withParams(1d).returns("DOUBLE").check();
    }

    /** */
    @Test
    public void testWrongParametersNumberInSelectList() {
        assertUnexpectedNumberOfParameters("SELECT 1", 1);
        assertUnexpectedNumberOfParameters("SELECT ?", 1, 2);
        assertUnexpectedNumberOfParameters("SELECT COALESCE(?)");
        assertUnexpectedNumberOfParameters("SELECT COALESCE(?)", 1, 2);
        assertUnexpectedNumberOfParameters("SELECT * FROM (VALUES(1, 2, ?)) t1");
        assertUnexpectedNumberOfParameters("SELECT * FROM (VALUES(1, 2, ?)) t1", 1, 2);
    }

    /** */
    @Test
    public void testDynamicParametersInExplain() {
        sql("EXPLAIN PLAN FOR SELECT * FROM t1 WHERE id > ?", 1);
    }

    /** */
    @Test
    public void testWrongParametersNumberInDelete() {
        assertUnexpectedNumberOfParameters("DELETE FROM t1 WHERE id = 1 AND val1=1", 1);
        assertUnexpectedNumberOfParameters("DELETE FROM t1 WHERE id = ? AND val1=1", 1, 2);
        assertUnexpectedNumberOfParameters("DELETE FROM t1 WHERE id = ? AND val1=1");
    }

    /** */
    @Test
    public void testWrongParametersNumberInInsert() {
        assertUnexpectedNumberOfParameters("INSERT INTO t1 VALUES(1, 2, 3)", 1);
        assertUnexpectedNumberOfParameters("INSERT INTO t1 VALUES(1, 2, ?)", 1, 2);
        assertUnexpectedNumberOfParameters("INSERT INTO t1 VALUES(1, 2, ?)");
    }

    /** */
    @Test
    public void testWrongParametersNumberInExplain() {
        assertUnexpectedNumberOfParameters("EXPLAIN PLAN FOR SELECT * FROM t1 WHERE id > ?");
        assertUnexpectedNumberOfParameters("EXPLAIN PLAN FOR SELECT * FROM t1 WHERE id > ?", 1, 2);
    }

    /** */
    @Test
    public void testWrongParametersNumberInSubqueries() {
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val2 from t1 where val2>? and val2!=val1)", 1, 2);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val2 from t1 where val2>? and val2!=val1)");
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val2 from t1 where val2>? and val2!=val1 " +
            "LIMIT ? OFFSET 1)", 1, 2, 3);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val2 from t1 where val2>? and val2!=val1 " +
            "LIMIT ? OFFSET ?)", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val2 from t1 where val2>? and val2!=val1 " +
                "LIMIT ? OFFSET ?) LIMIT ?",
            1, 2, 3);

        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val3 from t2 where val3>? and val4!=val1)", 1, 2);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val3 from t2 where val3>? and val4!=val1)");
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val3 from t2 where val3>? and val4!=val1 " +
            "LIMIT ? OFFSET 1)", 1, 2, 3);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val3 from t2 where val3>? and val4!=val1 " +
            "LIMIT ? OFFSET ?)", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 where val1 in (SELECT val3 from t2 where val3>? and val4!=val1 " +
            "LIMIT ? OFFSET ?) LIMIT ?", 1, 2, 3);
    }

    /** */
    @Test
    public void testWrongParametersNumberInJoins() {
        assertUnexpectedNumberOfParameters("SELECT val1, val3 FROM t1 JOIN t2 on val2=val4", 1);
        assertUnexpectedNumberOfParameters("SELECT val1 * ?, val3 FROM t1 JOIN t2 on val2=val4*?");
        assertUnexpectedNumberOfParameters("SELECT val1 * ?, val3 FROM t1 JOIN t2 on val2=val4*?", 1);
        assertUnexpectedNumberOfParameters("SELECT val1 * ?, val3 FROM t1 JOIN t2 on val2=val4*?", 1, 2, 3);

        assertUnexpectedNumberOfParameters("SELECT val1 * ?, val3 FROM t1 JOIN t2 on val2=val4*? WHERE val3>? ORDER BY " +
            "val1 LIMIT ? OFFSET ?", 1, 2, 3, 4);
        assertUnexpectedNumberOfParameters("SELECT val1 * ?, val3 FROM t1 JOIN t2 on val2=val4*? WHERE val3>? ORDER BY " +
            "val1 LIMIT ? OFFSET ?", 1, 2, 3, 4, 5, 6);
    }

    /** */
    @Test
    public void testWrongParametersNumberInUnion() {
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? UNION ALL SELECT val3 FROM t2 where val4=?", 1);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? UNION ALL SELECT val3 FROM t2 where val4=?", 1, 2, 3);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? UNION ALL SELECT val3 FROM t2 where val4=?");

        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? UNION ALL SELECT val3 FROM t2 where val4=? " +
            "LIMIT 5 OFFSET 3", 1);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? UNION ALL SELECT val3 FROM t2 where val4=? " +
            "LIMIT 5 OFFSET 3", 1, 2, 3);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? UNION ALL SELECT val3 FROM t2 where val4=? " +
            "LIMIT 5 OFFSET 3");

        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? UNION ALL SELECT val3 FROM t2 where val4=? " +
            "LIMIT ? OFFSET ?", 1, 2, 3);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? UNION ALL SELECT val3 FROM t2 where val4=? " +
            "LIMIT ? OFFSET ?", 1, 2, 3, 4, 5);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? UNION ALL SELECT val3 FROM t2 where val4=? " +
            "LIMIT ? OFFSET ?");

        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=(SELECT val3 from T2 where val3 > val1 + ? LIMIT ?) " +
            "UNION ALL SELECT val3 FROM t2 where val4=?");
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=(SELECT val3 from T2 where val3 > val1 + ? LIMIT ?) " +
            "UNION ALL SELECT val3 FROM t2 where val4=?", 1, 2);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=(SELECT val3 from T2 where val3 > val1 + ? LIMIT ?) " +
            "UNION ALL SELECT val3 FROM t2 where val4=?", 1, 2, 3, 4);

        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=?+1 and val1>? UNION ALL SELECT val3 FROM t2 " +
            "where val4=? LIMIT 1 OFFSET 2");
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=?+1 and val1>? UNION ALL SELECT val3 FROM t2 " +
            "where val4=? LIMIT 1 OFFSET 2", 1);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=?+1 and val1>? UNION ALL SELECT val3 FROM t2 " +
            "where val4=? LIMIT 1 OFFSET 2", 1, 2, 3, 4);
    }

    /** */
    @Test
    public void testWrongParametersNumberInSeveralQueries() {
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=?; SELECT val3 FROM t2 where val4=?");
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=?; SELECT val3 FROM t2 where val4=?", 1);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=?; SELECT val3 FROM t2 where val4=?", 1);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=?; SELECT val3 FROM t2 where val4=?", 1, 2, 3);

        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? LIMIT ? OFFSET ?; SELECT val3 FROM t2 " +
            "where val4=? LIMIT ?", 1, 2, 3, 4);
        assertUnexpectedNumberOfParameters("SELECT val1 FROM t1 where val2=? LIMIT ? OFFSET ?; SELECT val3 FROM t2 " +
            "where val4=? LIMIT ?", 1, 2, 3, 4, 5, 6);
    }

    /** */
    @Test
    public void testWrongParametersNumberInLimitOffset() {
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT ?", 1, 2);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT ?");

        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1 OFFSET 1", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT ? OFFSET ?", 1, 2, 3);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT ? OFFSET ?", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1 OFFSET ?", 1, 2);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 LIMIT 1 OFFSET ?");

        assertUnexpectedNumberOfParameters("SELECT * FROM t1 OFFSET 1", 1);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 OFFSET ?", 1, 2);
        assertUnexpectedNumberOfParameters("SELECT * FROM t1 OFFSET ?");
    }

    /** */
    @Test
    public void testWrongParametersNumberInUpdate() {
        assertUnexpectedNumberOfParameters("UPDATE t1 SET val1=? WHERE id = 1");
        assertUnexpectedNumberOfParameters("UPDATE t1 SET val1=? WHERE id = 1", 1, 2);
        assertUnexpectedNumberOfParameters("UPDATE t1 SET val1=10 WHERE id = ?");
        assertUnexpectedNumberOfParameters("UPDATE t1 SET val1=10 WHERE id = ?", 1, 2);
    }

    /** */
    private void assertUnexpectedNumberOfParameters(String qry, Object... params) {
        assertThrows(qry, IgniteSQLException.class, "Wrong number of query parameters", params);
    }
}
