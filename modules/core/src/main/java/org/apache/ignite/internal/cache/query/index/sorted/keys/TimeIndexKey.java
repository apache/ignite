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

import java.sql.Time;
import java.time.LocalTime;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils;

/** */
public class TimeIndexKey implements IndexKey {
    /** */
    private final long nanos;

    /** */
    public TimeIndexKey(Object obj) {
        if (obj instanceof Date) {
            long millis = DateValueUtils.utcMillisFromDefaultTz(((Date)obj).getTime());
            millis %= DateValueUtils.MILLIS_PER_DAY;

            if (millis < 0)
                millis += DateValueUtils.MILLIS_PER_DAY;

            nanos = TimeUnit.MILLISECONDS.toNanos(millis);
        }
        else if (obj instanceof LocalTime) {
            LocalTime locTime = (LocalTime)obj;
            nanos = locTime.toNanoOfDay();
        }
        else {
            throw new IgniteException("Failed to convert object to time value, unexpected class " +
                obj.getClass().getName());
        }
    }

    /** */
    public TimeIndexKey(long nanos) {
        this.nanos = nanos;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        millis = DateValueUtils.defaultTzMillisFromUtc(millis);

        return new Time(millis);
    }

    /** @return nanoseconds since midnight. */
    public long nanos() {
        return nanos;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.TIME;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        return Long.compare(nanos, ((TimeIndexKey)o).nanos);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.valueOf(nanos);
    }
}
