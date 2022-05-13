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

package org.apache.ignite.internal.processors.cache.index;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cache.query.index.sorted.keys.DateIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.TimeIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.TimestampIndexKey;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.junit.Test;

/**
 * Tests that core IndexKey values calculation compatible with H2 date/time values calculation.
 */
public class DateIndexKeyTypeTest extends GridCommonAbstractTest {
    /** */
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /** */
    private final DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss.SSS");

    /** */
    private final DateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /** */
    private void checkDate(long millis) {
        Date date = new Date(millis);
        DateIndexKey key = new DateIndexKey(date);
        ValueDate v = ValueDate.get(date);

        assertEquals(v.getDateValue(), key.dateValue());

        assertEquals(dateFormat.format(date), dateFormat.format((java.util.Date)key.key()));

        // Check construction from LocalDate.
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
        LocalDate locDate = LocalDateTime.ofEpochSecond(seconds, 0, ZoneOffset.UTC).toLocalDate();

        ValueDate locV = (ValueDate)LocalDateTimeUtils.localDateToDateValue(locDate);
        DateIndexKey locKey = new DateIndexKey(locDate);

        assertEquals(locV.getDateValue(), locKey.dateValue());
    }

    /** */
    private void checkTimestamp(long millis) {
        Timestamp ts = new Timestamp(millis);
        TimestampIndexKey key = new TimestampIndexKey(ts);
        ValueTimestamp v = ValueTimestamp.get(ts);

        assertEquals(v.getDateValue(), key.dateValue());
        assertEquals(v.getTimeNanos(), key.nanos());

        assertEquals(tsFormat.format(ts), tsFormat.format((java.util.Date)key.key()));

        // Check construction from LocalDateTime.
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
        LocalDateTime locDateTime = LocalDateTime.ofEpochSecond(seconds, 0, ZoneOffset.UTC);

        ValueTimestamp locV = (ValueTimestamp)LocalDateTimeUtils.localDateTimeToValue(locDateTime);
        TimestampIndexKey locKey = new TimestampIndexKey(locDateTime);

        assertEquals(locV.getDateValue(), locKey.dateValue());
        assertEquals(locV.getTimeNanos(), locKey.nanos());
    }

    /** */
    private void checkTime(long millis) {
        Time t = new Time(millis);
        TimeIndexKey key = new TimeIndexKey(t);
        ValueTime v = ValueTime.get(t);

        assertEquals(v.getNanos(), key.nanos());

        assertEquals(timeFormat.format(t), timeFormat.format((java.util.Date)key.key()));

        // Check construction from LocalTime.
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
        LocalTime locTime = LocalDateTime.ofEpochSecond(seconds, 0, ZoneOffset.UTC).toLocalTime();

        ValueTime locV = (ValueTime)LocalDateTimeUtils.localTimeToTimeValue(locTime);
        TimeIndexKey locKey = new TimeIndexKey(locTime);

        assertEquals(locV.getNanos(), locKey.nanos());
    }

    /** */
    private void checkAllTypes(long tsStart, long tsEnd, long increment) {
        for (long millis = tsStart; millis <= tsEnd; millis += increment) {
            checkDate(millis);
            checkTimestamp(millis);
            checkTime(millis);
        }
    }

    /** */
    @Test
    public void testIndexKeyTypes() throws Exception {
        // Check every half a day since -1000-01-01 to 3000-01-01.
        checkAllTypes(
            dateFormat.parse("-1000-01-01").getTime(),
            dateFormat.parse("3000-01-01").getTime(),
            TimeUnit.HOURS.toMillis(12)
        );

        // Check every half an hour since 1970-01-01 to 2023-01-01.
        checkAllTypes(
            0,
            dateFormat.parse("2023-01-01").getTime(),
            TimeUnit.MINUTES.toMillis(30)
        );
    }
}
