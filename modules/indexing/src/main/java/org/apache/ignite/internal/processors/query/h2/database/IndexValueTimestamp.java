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

package org.apache.ignite.internal.processors.query.h2.database;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Timestamp value.
 */
public class IndexValueTimestamp {
    /** Date. */
    private final long date;

    /** Time in nanoseconds. */
    private final long timeNanos;

    /**
     * Constructor.
     *
     * @param date Date.
     * @param timeNanos Time in nanoseconds.
     */
    public IndexValueTimestamp(long date, long timeNanos) {
        this.date = date;
        this.timeNanos = timeNanos;
    }

    /**
     * @return Date.
     */
    public long date() {
        return date;
    }

    /**
     * @return Time in nanoseconds.
     */
    public long timeNanos() {
        return timeNanos;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexValueTimestamp.class, this);
    }
}
