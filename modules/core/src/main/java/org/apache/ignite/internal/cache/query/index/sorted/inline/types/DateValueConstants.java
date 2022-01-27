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

package org.apache.ignite.internal.cache.query.index.sorted.inline.types;

/**
 * DateValue is a representation of a date in bit form:
 *
 * dv = (year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day.
 */
public class DateValueConstants {
    /** Forbid instantiation of this class. Just hold constants there. */
    private DateValueConstants() {}

    /** */
    private static final int SHIFT_YEAR = 9;

    /** */
    private static final int SHIFT_MONTH = 5;

    /** Min date value. */
    public static final long MIN_DATE_VALUE = (-999_999_999L << SHIFT_YEAR) + (1 << SHIFT_MONTH) + 1;

    /** Max date value. */
    public static final long MAX_DATE_VALUE = (999_999_999L << SHIFT_YEAR) + (12 << SHIFT_MONTH) + 31;

    /** The number of milliseconds per day. */
    public static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    /** The number of nanoseconds per day. */
    public static final long NANOS_PER_DAY = MILLIS_PER_DAY * 1_000_000;
}
