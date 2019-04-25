/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Base class for records with timeStamp.
 * All records which support timeStamp should be inherited from {@code TimeStampRecord}.
 */
public abstract class TimeStampRecord extends WALRecord {
    /** Timestamp. */
    @GridToStringInclude
    protected long timestamp;

    /**
     *
     */
    protected TimeStampRecord() {
        timestamp = U.currentTimeMillis();
    }

    /**
     * @param timestamp TimeStamp.
     */
    protected TimeStampRecord(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @param timestamp TimeStamp.
     */
    public void timestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return TimeStamp.
     */
    public long timestamp() {
        return timestamp;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TimeStampRecord.class, this);
    }
}
