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

package org.apache.ignite.internal.processors.query.h2.index.keys;

import java.io.IOException;
import java.io.ObjectInput;
import org.apache.ignite.internal.cache.query.index.sorted.keys.AbstractTimestampIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.h2.value.ValueTimestamp;

/** */
public class TimestampIndexKey extends AbstractTimestampIndexKey implements H2ValueWrapperMixin {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private ValueTimestamp timestamp;

    /** */
    public TimestampIndexKey(Object obj) {
        timestamp = (ValueTimestamp) wrapToValue(obj, type());
    }

    /** */
    public TimestampIndexKey() {
        // No-op.
    }

    /** */
    public static TimestampIndexKey fromDateValueAndNanos(long dateVal, long nanos) {
        return new TimestampIndexKey(ValueTimestamp.fromDateValueAndNanos(dateVal, nanos));
    }

    /** */
    private TimestampIndexKey(ValueTimestamp timestamp) {
        this.timestamp = timestamp;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return timestamp.getTimestamp();
    }

    /** {@inheritDoc} */
    @Override public long dateValue() {
        return timestamp.getDateValue();
    }

    /** {@inheritDoc} */
    @Override public long nanos() {
        return timestamp.getTimeNanos();
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        return timestamp.compareTo(((TimestampIndexKey)o).timestamp, null);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        timestamp = ValueTimestamp.fromDateValueAndNanos(in.readLong(), in.readLong());
    }
}
