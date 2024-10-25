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
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 *  Dynamic parameters types inference test.
 */
public class DynamicParametersIntegrationTest extends AbstractBasicIntegrationTest {
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
        assertThrows("SELECT ?", SqlValidatorException.class, "No value passed for dynamic parameter 1 or its type is unknown.");

        assertThrows("SELECT ?, ?", SqlValidatorException.class, "No value passed for dynamic parameter 2 or its type is unknown.", "arg0");
    }

    /** */
    @Test
    public void testCasts() {
        assertQuery("SELECT CAST(? as INTEGER)").withParams('1').returns(1).check();
        assertQuery("SELECT ?::INTEGER").withParams('1').returns(1).check();
        assertQuery("SELECT ?::VARCHAR").withParams(1).returns("1").check();
        assertQuery("SELECT CAST(? as VARCHAR)").withParams(1).returns("1").check();

        createAndPopulateTable();

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
//        assertQuery("SELECT COALESCE(?, ?)").withParams("a", 10).returns("a").check();
//        assertQuery("SELECT COALESCE(null, ?)").withParams(13).returns(13).check();
//        assertQuery("SELECT LOWER(?)").withParams("ASD").returns("asd").check();
//        assertQuery("SELECT ?").withParams("asd").returns("asd").check();
//        assertQuery("SELECT ? + ?, LOWER(?) ").withParams(2, 2, "TeSt").returns(4, "test").check();
//        assertQuery("SELECT LOWER(?), ? + ? ").withParams("TeSt", 2, 2).returns("test", 4).check();
//        assertQuery("SELECT POWER(?, ?)").withParams(2, 3).returns(8d).check();
//        assertQuery("SELECT SQRT(?)").withParams(4d).returns(2d).check();
//        assertQuery("SELECT ? % ?").withParams(11, 10).returns(1).check();
//
//        assertQuery("SELECT LAST_DAY(?)").withParams(Date.valueOf("2022-01-01"))
//            .returns(Date.valueOf("2022-01-31")).check();
//        assertQuery("SELECT LAST_DAY(?)").withParams(LocalDate.parse("2022-01-01"))
//            .returns(Date.valueOf("2022-01-31")).check();

        createAndPopulateTable();

//        assertQuery("SELECT name LIKE '%' || ? || '%' FROM person where name is not null").withParams("go")
//            .returns(true).returns(false).returns(false).returns(false).returns(false).check();
//
//        assertQuery("SELECT id FROM person WHERE name LIKE ? ORDER BY id LIMIT ?").withParams("I%", 1)
//            .returns(0).check();

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
}
