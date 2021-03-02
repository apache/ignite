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
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.util.typedef.T2;

/** */
public class TimestampIndexKey implements IndexKey {
    /** */
    private final long dateVal;

    /** */
    private final long nanos;

    /**
     * Create pair of date value and nanos for the given date.
     *
     * @param date Date.
     */
    public TimestampIndexKey(java.util.Date date) {
        long ms = date.getTime();

        dateVal = DateTimeUtils.dateValueFromDate(ms);
        nanos = DateTimeUtils.nanosFromDate(ms);
    }

    /** */
    public TimestampIndexKey(LocalDateTime date) {
        LocalDate locDate = date.toLocalDate();

        dateVal = DateTimeUtils.dateValue(locDate.getYear(), locDate.getMonthValue(),
            locDate.getDayOfMonth());

        nanos = date.toLocalTime().toNanoOfDay();
    }

    /** */
    public TimestampIndexKey(Timestamp timestamp) {
        long ms = timestamp.getTime();
        long nanos = timestamp.getNanos() % 1_000_000;

        dateVal = DateTimeUtils.dateValueFromDate(ms);
        this.nanos = nanos + DateTimeUtils.nanosFromDate(ms);
    }

    /** */
    public TimestampIndexKey(long dateVal, long nanos) {
        this.dateVal = dateVal;
        this.nanos = nanos;
    }

    /** {@inheritDoc} */
    @Override public Object getKey() {
        return new T2<>(dateVal, nanos);
    }

    /** {@inheritDoc} */
    @Override public int getType() {
        return IndexKeyTypes.TIMESTAMP;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o, IndexKeyTypeSettings keySettings) {
        T2<Long, Long> okey = (T2<Long, Long>)o.getKey();

        int cmp = Long.compare(dateVal, okey.get1());
        return cmp != 0 ? cmp : Long.compare(nanos, okey.get2());
    }
}
