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

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assume.assumeTrue;

/** */
public class DateTimeTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /** {@inheritDoc} */
    @Override protected void init() throws Exception {
        super.init();

        QueryEntity qryEnt = new QueryEntity();
        qryEnt.setKeyFieldName("ID");
        qryEnt.setKeyType(Integer.class.getName());
        qryEnt.setValueType(DateTimeEntry.class.getName());

        qryEnt.addQueryField("ID", Integer.class.getName(), null);
        qryEnt.addQueryField("JAVADATE", Date.class.getName(), null);
        qryEnt.addQueryField("SQLDATE", java.sql.Date.class.getName(), null);
        qryEnt.addQueryField("SQLTIME", Time.class.getName(), null);
        qryEnt.addQueryField("SQLTIMESTAMP", Timestamp.class.getName(), null);
        qryEnt.setTableName("datetimetable");

        final CacheConfiguration<Integer, DateTimeEntry> cfg = this.<Integer, DateTimeEntry>cacheConfiguration()
            .setName(qryEnt.getTableName())
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setQueryEntities(singletonList(qryEnt))
            .setSqlSchema("PUBLIC");

        IgniteCache<Integer, DateTimeEntry> dateTimeCache = client.createCache(cfg);

        put(client, dateTimeCache, 1, new DateTimeEntry(1, javaDate("2020-10-01 12:00:00.000"),
            sqlDate("2020-10-01"), sqlTime("12:00:00"), sqlTimestamp("2020-10-01 12:00:00.000")));
        put(client, dateTimeCache, 2, new DateTimeEntry(2, javaDate("2020-12-01 00:10:20.000"),
            sqlDate("2020-12-01"), sqlTime("00:10:20"), sqlTimestamp("2020-12-01 00:10:20.000")));
        put(client, dateTimeCache, 3, new DateTimeEntry(3, javaDate("2020-10-20 13:15:00.000"),
            sqlDate("2020-10-20"), sqlTime("13:15:00"), sqlTimestamp("2020-10-20 13:15:00.000")));
        put(client, dateTimeCache, 4, new DateTimeEntry(4, javaDate("2020-01-01 22:40:00.000"),
            sqlDate("2020-01-01"), sqlTime("22:40:00"), sqlTimestamp("2020-01-01 22:40:00.000")));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Don't clean up caches after test.
    }

    /** */
    @Test
    public void testQuery1() throws Exception {
        assertQuery("SELECT SQLDATE FROM datetimetable where SQLTIME = '12:00:00'")
            .returns(sqlDate("2020-10-01"))
            .check();
    }

    /** */
    @Test
    public void testQuery2() throws Exception {
        assertQuery("SELECT SQLDATE FROM datetimetable where JAVADATE = ?")
            .withParams(javaDate("2020-12-01 00:10:20.000"))
            .returns(sqlDate("2020-12-01"))
            .check();
    }

    /** */
    @Test
    public void testQuery3() throws Exception {
        assertQuery("SELECT SQLDATE FROM datetimetable where JAVADATE = ?")
            .withParams(sqlTimestamp("2020-12-01 00:10:20.000"))
            .returns(sqlDate("2020-12-01"))
            .check();
    }

    /** */
    @Test
    public void testQuery4() throws Exception {
        assertQuery("SELECT MAX(SQLDATE) FROM datetimetable")
            .returns(sqlDate("2020-12-01"))
            .check();
    }

    /** */
    @Test
    public void testQuery5() throws Exception {
        assertQuery("SELECT MIN(SQLDATE) FROM datetimetable")
            .returns(sqlDate("2020-01-01"))
            .check();
    }

    /** */
    @Test
    public void testQuery6() throws Exception {
        assertQuery("SELECT JAVADATE FROM datetimetable WHERE SQLTIME = '13:15:00'")
            .returns(javaDate("2020-10-20 13:15:00.000"))
            .check();
    }

    /** */
    @Test
    public void testQuery7() throws Exception {
        assertQuery("SELECT t1.JAVADATE, t2.JAVADATE FROM datetimetable t1 " +
            "INNER JOIN " +
            "(SELECT JAVADATE, CAST(SQLTIMESTAMP AS TIME) AS CASTED_TIME FROM datetimetable) t2 " +
            "ON t1.SQLTIME = t2.CASTED_TIME " +
            "WHERE t2.JAVADATE = '2020-10-20 13:15:00.000'")
            .returns(javaDate("2020-10-20 13:15:00.000"), javaDate("2020-10-20 13:15:00.000"))
            .check();
    }

    /**
     * Test right date/time interpretation taking into account DST clock shift.
     */
    @Test
    public void testDstShift() throws Exception {
        TimeZone oldTz = TimeZone.getDefault();

        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));

            // Time zone change (EET->MSK) 1992-01-19 02:00:00 -> 1992-01-19 03:00:00
            assertQuery("select date '1992-01-19'").returns(sqlDate("1992-01-19")).check();
            assertQuery("select date '1992-01-18' + interval (1) days").returns(sqlDate("1992-01-19")).check();
            assertQuery("select date '1992-01-18' + interval (24) hours").returns(sqlDate("1992-01-19")).check();
            assertQuery("SELECT timestamp '1992-01-18 02:30:00' + interval (25) hours")
                .returns(sqlTimestamp("1992-01-19 03:30:00.000")).check();
            assertQuery("SELECT timestamp '1992-01-18 02:30:00' + interval (23) hours")
                .returns(sqlTimestamp("1992-01-19 01:30:00.000")).check();
            assertQuery("SELECT timestamp '1992-01-18 02:30:00' + interval (24) hours")
                .returns(sqlTimestamp("1992-01-19 02:30:00.000")).check();

            // DST started 1992-03-29 02:00:00 -> 1992-03-29 03:00:00
            assertQuery("select date '1992-03-29'").returns(sqlDate("1992-03-29")).check();
            assertQuery("select date '1992-03-28' + interval (1) days").returns(sqlDate("1992-03-29")).check();
            assertQuery("select date '1992-03-28' + interval (24) hours").returns(sqlDate("1992-03-29")).check();
            assertQuery("SELECT timestamp '1992-03-28 02:30:00' + interval (25) hours")
                .returns(sqlTimestamp("1992-03-29 03:30:00.000")).check();
            assertQuery("SELECT timestamp '1992-03-28 02:30:00' + interval (23) hours")
                .returns(sqlTimestamp("1992-03-29 01:30:00.000")).check();
            assertQuery("SELECT timestamp '1992-03-28 02:30:00' + interval (24) hours")
                .returns(sqlTimestamp("1992-03-29 02:30:00.000")).check();

            // DST ended 1992-09-27 03:00:00 -> 1992-09-27 02:00:00
            assertQuery("select date '1992-09-27'").returns(sqlDate("1992-09-27")).check();
            assertQuery("select date '1992-09-26' + interval (1) days").returns(sqlDate("1992-09-27")).check();
            assertQuery("select date '1992-09-26' + interval (24) hours").returns(sqlDate("1992-09-27")).check();
            assertQuery("SELECT timestamp '1992-09-26 02:30:00' + interval (25) hours")
                .returns(sqlTimestamp("1992-09-27 03:30:00.000")).check();
            assertQuery("SELECT timestamp '1992-09-26 02:30:00' + interval (23) hours")
                .returns(sqlTimestamp("1992-09-27 01:30:00.000")).check();
            assertQuery("SELECT timestamp '1992-09-26 02:30:00' + interval (24) hours")
                .returns(sqlTimestamp("1992-09-27 02:30:00.000")).check();

            TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

            // DST ended 2021-11-07 02:00:00 -> 2021-11-07 01:00:00
            assertQuery("select date '2021-11-07'").returns(sqlDate("2021-11-07")).check();
            assertQuery("select date '2021-11-06' + interval (1) days").returns(sqlDate("2021-11-07")).check();
            assertQuery("select date '2021-11-06' + interval (24) hours").returns(sqlDate("2021-11-07")).check();
            assertQuery("SELECT timestamp '2021-11-06 01:30:00' + interval (25) hours")
                .returns(sqlTimestamp("2021-11-07 02:30:00.000")).check();
            // Check string representation here, since after timestamp calculation we have '2021-11-07T01:30:00.000-0800'
            // but Timestamp.valueOf method converts '2021-11-07 01:30:00' in 'America/Los_Angeles' time zone to
            // '2021-11-07T01:30:00.000-0700' (we pass through '2021-11-07 01:30:00' twice after DST ended).
            assertQuery("SELECT (timestamp '2021-11-06 02:30:00' + interval (23) hours)::varchar")
                .returns("2021-11-07 01:30:00").check();
            assertQuery("SELECT (timestamp '2021-11-06 01:30:00' + interval (24) hours)::varchar")
                .returns("2021-11-07 01:30:00").check();
        }
        finally {
            TimeZone.setDefault(oldTz);
        }
    }

    /** */
    @Test
    public void testOldDateLiterals() throws Exception {
        assertQuery("SELECT DATE '1582-10-20'").returns(sqlDate("1582-10-20")).check();
        assertQuery("SELECT DATE '1582-10-15'").returns(sqlDate("1582-10-15")).check();
        assertQuery("SELECT DATE '1582-10-01'").returns(sqlDate("1582-10-01")).check();
        assertQuery("SELECT DATE '1582-09-30'").returns(sqlDate("1582-09-30")).check();
        assertQuery("SELECT DATE '1000-01-01'").returns(sqlDate("1000-01-01")).check();
        assertQuery("SELECT DATE '0550-05-05'").returns(sqlDate("0550-05-05")).check();

        assertQuery("SELECT ?").returns(sqlDate("1582-10-20")).withParams(sqlDate("1582-10-20")).check();
        assertQuery("SELECT ?").returns(sqlDate("1582-10-01")).withParams(sqlDate("1582-10-01")).check();
        assertQuery("SELECT ?").returns(sqlDate("1000-01-01")).withParams(sqlDate("1000-01-01")).check();

        assertQuery("SELECT TIMESTAMP '1582-10-20 17:12:47.111'").returns(sqlTimestamp("1582-10-20 17:12:47.111"))
            .check();
        assertQuery("SELECT TIMESTAMP '1582-10-15 00:00:00.001'").returns(sqlTimestamp("1582-10-15 00:00:00.001"))
            .check();
        assertQuery("SELECT TIMESTAMP '1582-10-01 01:01:15.555'").returns(sqlTimestamp("1582-10-01 01:01:15.555"))
            .check();
        assertQuery("SELECT TIMESTAMP '1582-09-30 23:23:59.999'").returns(sqlTimestamp("1582-09-30 23:23:59.999"))
            .check();
        assertQuery("SELECT TIMESTAMP '1000-01-01 23:23:59.999'").returns(sqlTimestamp("1000-01-01 23:23:59.999"))
            .check();
        assertQuery("SELECT TIMESTAMP '0550-05-05 04:04:31.015'").returns(sqlTimestamp("0550-05-05 04:04:31.015"))
            .check();

        assertQuery("SELECT ?").withParams(sqlTimestamp("1582-10-20 17:12:47.111"))
            .returns(sqlTimestamp("1582-10-20 17:12:47.111")).check();
        assertQuery("SELECT ?").withParams(sqlTimestamp("1582-10-15 00:00:00.001"))
            .returns(sqlTimestamp("1582-10-15 00:00:00.001")).check();
        assertQuery("SELECT ?").withParams(sqlTimestamp("0550-05-05 04:04:31.015"))
            .returns(sqlTimestamp("0550-05-05 04:04:31.015")).check();
    }

    /** */
    @Test
    public void testOldDateLiteralsWithTable() throws Exception {
        try {
            sql(client, "INSERT INTO datetimetable (ID, SQLDATE, SQLTIMESTAMP) VALUES(?, ?, ? )", 5, sqlDate("1582-10-04"),
                sqlTimestamp("1582-10-04 15:31:47.381"));
            sql(client, "INSERT INTO datetimetable (ID, SQLDATE, SQLTIMESTAMP) VALUES(6, DATE '1582-10-04'," +
                " TIMESTAMP '1582-10-04 15:31:47.381')");

            assertQuery("SELECT SQLDATE from datetimetable WHERE ID=5").returns(sqlDate("1582-10-04")).check();
            assertQuery("SELECT SQLDATE from datetimetable WHERE ID=6").returns(sqlDate("1582-10-04")).check();
            assertQuery("SELECT SQLDATE + INTERVAL 1 DAYS from datetimetable WHERE ID=5").returns(sqlDate("1582-10-15"))
                .check();
            assertQuery("SELECT SQLDATE + INTERVAL 1 DAYS from datetimetable WHERE ID=6").returns(sqlDate("1582-10-15"))
                .check();

            assertQuery("SELECT SQLTIMESTAMP from datetimetable WHERE ID=5").returns(sqlTimestamp("1582-10-04 15:31:47.381")).check();
            assertQuery("SELECT SQLTIMESTAMP from datetimetable WHERE ID=6").returns(sqlTimestamp("1582-10-04 15:31:47.381")).check();
            assertQuery("SELECT SQLTIMESTAMP + INTERVAL 1 DAYS from datetimetable WHERE ID=5")
                .returns(sqlTimestamp("1582-10-15 15:31:47.381"))
                .check();
            assertQuery("SELECT SQLTIMESTAMP + INTERVAL 1 DAYS from datetimetable WHERE ID=6")
                .returns(sqlTimestamp("1582-10-15 15:31:47.381"))
                .check();
        }
        finally {
            sql(client, "DELETE FROM datetimetable WHERE ID IN(5, 6)");
        }
    }

    /** */
    @Test
    public void testDefaultDDLOldTemporalValues() throws Exception {
        assumeTrue(sqlTxMode == SqlTransactionMode.NONE);

        try {
            sql(client, "CREATE TABLE TBL(ID INTEGER, DT DATE DEFAULT DATE '1582-10-15', " +
                "TS TIMESTAMP DEFAULT TIMESTAMP '1582-10-15 01:02:03.456')");

            sql("INSERT INTO TBL(ID) VALUES(1)");

            assertQuery("SELECT DT, TS FROM TBL").returns(sqlDate("1582-10-15"), sqlTimestamp("1582-10-15 01:02:03.456")).check();

            sql("DROP TABLE TBL");

            sql(client, "CREATE TABLE TBL(ID INTEGER, DT DATE DEFAULT DATE '1582-10-04', " +
                "TS TIMESTAMP DEFAULT TIMESTAMP '1582-10-04 01:02:03.456')");

            sql("INSERT INTO TBL(ID) VALUES(1)");

            assertQuery("SELECT DT, TS FROM TBL").returns(sqlDate("1582-10-04"), sqlTimestamp("1582-10-04 01:02:03.456")).check();
        }
        finally {
            sql(client, "DROP TABLE IF EXISTS TBL");
        }
    }

    /** */
    @Test
    public void testDateTimeCast() throws Exception {
        assertQuery("SELECT CAST('2021-01-01 01:02:03.456' AS TIMESTAMP)")
            .returns(sqlTimestamp("2021-01-01 01:02:03.456")).check();

        assertQuery("SELECT CAST('2021-01-01 01:02:03.0' AS TIMESTAMP)")
            .returns(sqlTimestamp("2021-01-01 01:02:03")).check();

        assertQuery("SELECT CAST('2021-01-01 01:02:03' AS TIMESTAMP)")
            .returns(sqlTimestamp("2021-01-01 01:02:03")).check();

        assertQuery("SELECT CAST('2021-01-01 01:02:03.456' AS TIMESTAMP(0))")
            .returns(sqlTimestamp("2021-01-01 01:02:03")).check();

        assertQuery("SELECT CAST('2021-01-01 01:02:03.456' AS TIMESTAMP(2))")
            .returns(sqlTimestamp("2021-01-01 01:02:03.45")).check();

        assertQuery("SELECT CAST('2021-01-01' AS DATE)")
            .returns(sqlDate("2021-01-01")).check();

        assertQuery("SELECT CAST('01:02:03' AS TIME)")
            .returns(sqlTime("01:02:03")).check();

        assertThrows("SELECT CAST('2021-01-02' AS DATE FORMAT 'DD-MM-YY')", IgniteSQLException.class,
            "Operator 'CAST' supports only the parameters: value and target type.");
    }

    /** */
    public static class DateTimeEntry {
        /** */
        long id;

        /** */
        Date javaDate;

        /** */
        java.sql.Date sqlDate;

        /** */
        Time sqlTime;

        /** */
        Timestamp sqlTimestamp;

        /** */
        public DateTimeEntry(long id, Date javaDate, java.sql.Date sqlDate, Time sqlTime, Timestamp sqlTimestamp) {
            this.id = id;
            this.javaDate = javaDate;
            this.sqlDate = sqlDate;
            this.sqlTime = sqlTime;
            this.sqlTimestamp = sqlTimestamp;
        }
    }

    /** */
    private Date javaDate(String str) throws Exception {
        return DATE_FORMAT.parse(str);
    }

    /** */
    private java.sql.Date sqlDate(String str) throws Exception {
        return java.sql.Date.valueOf(str);
    }

    /** */
    private Time sqlTime(String str) throws Exception {
        return Time.valueOf(str);
    }

    /** */
    private Timestamp sqlTimestamp(String str) throws Exception {
        return Timestamp.valueOf(str);
    }
}
