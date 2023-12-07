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

import java.sql.Date;
import java.time.LocalDate;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils;

/** */
public class DateIndexKey extends DateTimeIndexKey {
    /** */
    private final long dateVal;

    /** */
    public DateIndexKey(Object obj) {
        if (obj instanceof Date) {
            long millis = DateValueUtils.utcMillisFromDefaultTz(((Date)obj).getTime());
            dateVal = DateValueUtils.dateValueFromMillis(millis);
        }
        else if (obj instanceof LocalDate) {
            LocalDate locDate = (LocalDate)obj;
            dateVal = DateValueUtils.dateValue(locDate.getYear(), locDate.getMonthValue(), locDate.getDayOfMonth());
        }
        else {
            throw new IgniteException("Failed to convert object to date value, unexpected class " +
                obj.getClass().getName());
        }
    }

    /** */
    public DateIndexKey(long dateVal) {
        this.dateVal = dateVal;
    }

    /** @return a date value {@link DateValueUtils}. */
    public long dateValue() {
        return dateVal;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.DATE;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        long millis = DateValueUtils.millisFromDateValue(dateVal);
        millis = DateValueUtils.defaultTzMillisFromUtc(millis);

        return new Date(millis);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(long dateVal, long nanos) {
        return this.dateVal != dateVal ? Long.compare(this.dateVal, dateVal) : Long.compare(0, nanos);
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        return -((DateTimeIndexKey)o).compareTo(dateVal, 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.valueOf(dateVal);
    }
}
