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
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;

/** */
public class TimeIndexKey implements IndexKey {
    /** */
    private final long nanos;

    /** */
    public TimeIndexKey(Time time) {
        nanos = DateTimeUtils.nanosFromDate(time.getTime());
    }

    /** */
    public TimeIndexKey(LocalTime time) {
        nanos = DateTimeUtils.nanosFromDate(time.toNanoOfDay());
    }

    /** */
    public TimeIndexKey(long nanos) {
        this.nanos = nanos;
    }

    /** {@inheritDoc} */
    @Override public Object getKey() {
        return nanos;
    }

    /** {@inheritDoc} */
    @Override public int getType() {
        return IndexKeyTypes.TIME;
    }


    /** {@inheritDoc} */
    @Override public int compare(IndexKey o, IndexKeyTypeSettings keySettings) {
        // TODO assert type?
        long okey = (long) o.getKey();

        return Long.compare(nanos, okey);
    }
}
