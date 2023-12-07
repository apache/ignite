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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils;

/** */
public class TimestampIndexKey extends DateTimeIndexKey {
    /** */
    private final long dateVal;

    /** */
    private final long nanos;

    /** */
    public TimestampIndexKey(Object obj) {
        if (obj instanceof Date) {
            long millis = DateValueUtils.utcMillisFromDefaultTz(((Date)obj).getTime());
            dateVal = DateValueUtils.dateValueFromMillis(millis);

            millis %= DateValueUtils.MILLIS_PER_DAY;

            if (millis < 0)
                millis += DateValueUtils.MILLIS_PER_DAY;

            if (obj instanceof Timestamp)
                nanos = TimeUnit.MILLISECONDS.toNanos(millis) + ((Timestamp)obj).getNanos() % 1_000_000L;
            else
                nanos = TimeUnit.MILLISECONDS.toNanos(millis);
        }
        else if (obj instanceof LocalDateTime) {
            LocalDateTime locDateTime = ((LocalDateTime)obj);
            LocalDate locDate = locDateTime.toLocalDate();
            LocalTime locTime = locDateTime.toLocalTime();
            dateVal = DateValueUtils.dateValue(locDate.getYear(), locDate.getMonthValue(), locDate.getDayOfMonth());
            nanos = locTime.toNanoOfDay();
        }
        else {
            throw new IgniteException("Failed to convert object to timestamp value, unexpected class " +
                obj.getClass().getName());
        }

    }

    /** */
    public TimestampIndexKey(long dateVal, long nanos) {
        this.dateVal = dateVal;
        this.nanos = nanos;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        long millis = DateValueUtils.millisFromDateValue(dateVal) + TimeUnit.NANOSECONDS.toMillis(nanos);
        millis = DateValueUtils.defaultTzMillisFromUtc(millis);
        return new Timestamp(millis);
    }

    /** @return a date value {@link DateValueUtils}. */
    public long dateValue() {
        return dateVal;
    }

    /** @return nanoseconds since midnight. */
    public long nanos() {
        return nanos;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.TIMESTAMP;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(long dateVal, long nanos) {
        return this.dateVal != dateVal ? Long.compare(this.dateVal, dateVal) : Long.compare(this.nanos, nanos);
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        return -((DateTimeIndexKey)o).compareTo(dateVal, nanos);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return dateVal + " " + nanos;
    }
}
