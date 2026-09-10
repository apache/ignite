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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** */
public class TypeUtilsTest {
    /** */
    @Test
    public void testLocalDateConversion() {
        for (String date : new String[] {
            "0001-01-01", "1500-01-02", "1582-10-10", "1969-12-31", "1970-01-01", "2011-12-30", "9999-12-31"
        }) {
            checkConversion(LocalDate.parse(date), new DateString(date).getDaysSinceEpoch(), Date.class);
        }
    }

    /** */
    @Test
    public void testLocalTimeConversion() {
        for (String time : new String[] {"00:00:00", "02:30:00", "12:34:56.123", "23:59:59.999"})
            checkConversion(LocalTime.parse(time), new TimeString(time).getMillisOfDay(), Time.class);
    }

    /** */
    @Test
    public void testLocalDateTimeConversion() {
        for (String ts : new String[] {
            "0001-01-01 00:00:00", "1500-01-02 03:04:05", "1582-10-10 12:34:56", "1969-12-31 23:59:59.999",
            "1970-01-01 00:00:00", "2011-12-30 12:34:56", "2021-03-14 02:30:00.123", "2021-11-07 01:30:00.123"
        }) {
            checkConversion(LocalDateTime.parse(ts.replace(' ', 'T')), new TimestampString(ts).getMillisSinceEpoch(),
                Timestamp.class);
        }
    }

    /** */
    @Test
    public void testSqlDateConversion() {
        for (String date : new String[] {
            "0001-01-01", "1500-01-02", "1582-10-04", "1582-10-15", "1969-12-31", "1970-01-01", "9999-12-31"
        }) {
            checkConversion(DataContexts.EMPTY, Date.valueOf(date), new DateString(date).getDaysSinceEpoch(), Date.class);
        }
    }

    /** */
    @Test
    public void testSqlTimestampConversion() {
        for (String ts : new String[] {
            "0001-01-01 00:00:00", "1500-01-02 03:04:05.123", "1582-10-04 23:59:59.999",
            "1582-10-15 00:00:00", "1969-12-31 23:59:59.999", "1970-01-01 00:00:00", "2021-03-14 12:30:00.123"
        }) {
            Timestamp val = Timestamp.valueOf(ts);
            long internal = new TimestampString(ts).getMillisSinceEpoch();

            checkConversion(DataContexts.EMPTY, val, internal, Timestamp.class);
            checkConversion(DataContexts.EMPTY, new java.util.Date(val.getTime()), internal, java.util.Date.class);
        }
    }

    /** */
    @Test
    public void testHistoricalJdbcConversionWithTimeZone() {
        for (String zone : new String[] {"UTC", "Europe/Moscow", "America/New_York", "Pacific/Apia"}) {
            TimeZone tz = TimeZone.getTimeZone(zone);
            DataContext ctx = DataContexts.of(Collections.singletonMap(DataContext.Variable.TIME_ZONE.camelName, tz));
            Calendar cal = new GregorianCalendar(tz, Locale.ROOT);

            cal.clear();
            cal.set(1500, Calendar.JANUARY, 2);

            checkConversion(ctx, new Date(cal.getTimeInMillis()), new DateString("1500-01-02").getDaysSinceEpoch(),
                Date.class);

            cal.set(1500, Calendar.JANUARY, 2, 3, 4, 5);
            cal.set(Calendar.MILLISECOND, 123);

            long internal = new TimestampString("1500-01-02 03:04:05.123").getMillisSinceEpoch();

            checkConversion(ctx, new Timestamp(cal.getTimeInMillis()), internal, Timestamp.class);
            checkConversion(ctx, new java.util.Date(cal.getTimeInMillis()), internal, java.util.Date.class);
        }
    }

    /** */
    private void checkConversion(Object val, Object internal, Type sqlJavaType) {
        // Constant reduction has no time zone in its data context.
        checkConversion(DataContexts.EMPTY, val, internal, sqlJavaType);

        for (String zone : new String[] {"UTC", "Europe/Moscow", "America/New_York", "Pacific/Apia"}) {
            DataContext ctx = DataContexts.of(Collections.singletonMap(
                DataContext.Variable.TIME_ZONE.camelName, TimeZone.getTimeZone(zone)));

            checkConversion(ctx, val, internal, sqlJavaType);
        }
    }

    /** */
    private void checkConversion(DataContext ctx, Object val, Object internal, Type sqlJavaType) {
        assertEquals(internal, TypeUtils.toInternal(ctx, val));
        // Table functions and dynamic parameters may use the corresponding JDBC class as the storage type.
        assertEquals(internal, TypeUtils.toInternal(ctx, val, sqlJavaType));
        assertEquals(val, TypeUtils.fromInternal(ctx, internal, val.getClass()));
        assertSame(val, TypeUtils.toInternal(ctx, val, Object.class));
    }
}
