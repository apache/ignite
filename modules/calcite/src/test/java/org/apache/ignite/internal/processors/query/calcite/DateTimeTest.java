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

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static java.util.Collections.singletonList;

@WithSystemProperty(key = "calcite.debug", value = "true")
public class DateTimeTest extends GridCommonCalciteAbstractTest {
    /** */
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /** */
    private static QueryEngine queryEngine;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGridsMultiThreaded(1);

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

        final CacheConfiguration<Integer, DateTimeEntry> cfg = new CacheConfiguration<>(qryEnt.getTableName());

        cfg.setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setQueryEntities(singletonList(qryEnt))
            .setSqlSchema("PUBLIC");

        IgniteCache<Integer, DateTimeEntry> dateTimeCache = grid.createCache(cfg);

        dateTimeCache.put(1, new DateTimeEntry(1, javaDate("2020-10-01 12:00:00.000"), sqlDate("2020-10-01"), sqlTime("12:00:00"), sqlTimestamp("2020-10-01 12:00:00.000")));
        dateTimeCache.put(2, new DateTimeEntry(2, javaDate("2020-12-01 00:10:20.000"), sqlDate("2020-12-01"), sqlTime("00:10:20"), sqlTimestamp("2020-12-01 00:10:20.000")));
        dateTimeCache.put(3, new DateTimeEntry(3, javaDate("2020-10-20 13:15:00.000"), sqlDate("2020-10-20"), sqlTime("13:15:00"), sqlTimestamp("2020-10-20 13:15:00.000")));
        dateTimeCache.put(4, new DateTimeEntry(4, javaDate("2020-01-01 22:40:00.000"), sqlDate("2020-01-01"), sqlTime("22:40:00"), sqlTimestamp("2020-01-01 22:40:00.000")));

        queryEngine = Commons.lookupComponent(((IgniteEx)grid).context(), QueryEngine.class);

        awaitPartitionMapExchange();
    }

    @Test
    public void testQuery1() throws Exception {
        checkQuery("SELECT SQLDATE FROM datetimetable where SQLTIME = '12:00:00'")
            .returns(sqlDate("2020-10-01"))
            .check();
    }

    @Test
    public void testQuery2() throws Exception {
        checkQuery("SELECT SQLDATE FROM datetimetable where JAVADATE = ?")
            .withParams(javaDate("2020-12-01 00:10:20.000"))
            .returns(sqlDate("2020-12-01"))
            .check();
    }

    @Test
    public void testQuery3() throws Exception {
        checkQuery("SELECT SQLDATE FROM datetimetable where JAVADATE = ?")
            .withParams(sqlTimestamp("2020-12-01 00:10:20.000"))
            .returns(sqlDate("2020-12-01"))
            .check();
    }

    @Test
    public void testQuery4() throws Exception {
        checkQuery("SELECT MAX(SQLDATE) FROM datetimetable")
            .returns(sqlDate("2020-12-01"))
            .check();
    }

    @Test
    public void testQuery5() throws Exception {
        checkQuery("SELECT MIN(SQLDATE) FROM datetimetable")
            .returns(sqlDate("2020-01-01"))
            .check();
    }

    @Test
    public void testQuery6() throws Exception {
        checkQuery("SELECT JAVADATE FROM datetimetable WHERE SQLTIME = '13:15:00'")
            .returns(javaDate("2020-10-20 13:15:00.000"))
            .check();
    }

    @Test
    public void testQuery7() throws Exception {
        checkQuery("SELECT t1.JAVADATE, t2.JAVADATE FROM datetimetable t1 " +
            "INNER JOIN " +
            "(SELECT JAVADATE, CAST(SQLTIMESTAMP AS TIME) AS CASTED_TIME FROM datetimetable) t2 " +
            "ON t1.SQLTIME = t2.CASTED_TIME " +
            "WHERE t2.JAVADATE = '2020-10-20 13:15:00.000'")
            .returns(javaDate("2020-10-20 13:15:00.000"), javaDate("2020-10-20 13:15:00.000"))
            .check();
    }

    public static class DateTimeEntry {
        long id;

        Date javaDate;

        java.sql.Date sqlDate;

        Time sqlTime;

        Timestamp sqlTimestamp;

        public DateTimeEntry(long id, Date javaDate, java.sql.Date sqlDate, Time sqlTime, Timestamp sqlTimestamp) {
            this.id = id;
            this.javaDate = javaDate;
            this.sqlDate = sqlDate;
            this.sqlTime = sqlTime;
            this.sqlTimestamp = sqlTimestamp;
        }
    }

    /** */
    private QueryChecker checkQuery(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return queryEngine;
            }
        };
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
