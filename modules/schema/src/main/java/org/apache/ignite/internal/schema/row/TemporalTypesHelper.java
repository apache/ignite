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

package org.apache.ignite.internal.schema.row;

import java.time.LocalDate;
import java.time.LocalTime;
import org.apache.ignite.internal.schema.TemporalNativeType;

/**
 * Helper class for temporal type conversions.
 *
 * <p>Provides methods to encode/decode temporal types in a compact way for further writing to row. Conversion preserves natural type
 * order.
 *
 * <p>DATE is a fixed-length type which compacted representation keeps ordering, value is signed and fit into a 3-bytes. Thus, DATE value
 * canbe compared by bytes where first byte is signed and others - unsigned. Thus temporal functions, like YEAR(), can easily extracts
 * fields with a mask,
 *
 * <p>Date compact structure: ┌──────────────┬─────────┬────────┐ │ Year(signed) │ Month   │ Day    │ ├──────────────┼─────────┼────────┤ │
 * 15 bits      │ 4 bits  │ 5 bits │ └──────────────┴─────────┴────────┘
 *
 * <p>TIME is a fixed-length type supporting accuracy from 1 second up to 1 nanosecond. Compacted time representation keeps ordering,
 * values fits to 4-6 bytes value. The first 18 bits is used for hours, minutes and seconds, and the last bits for fractional seconds: 14
 * for millisecond precision and 30 for nanosecond. Values of a type of any intermediate precisions is normalized to the type, then stored
 * as shortest possible structure without a precision lost.
 *
 * <p>Time compact structure: ┌─────────┬─────────┬──────────┬─────────────┐ │ Hours   │ Minutes │ Seconds  │ Sub-seconds │
 * ├─────────┼─────────┼──────────┼─────────────┤ │ 6 bit   │ 6 bits  │ 6 bit    │ 14 bits     │ - 32-bits in total. │ 6 bit   │ 6 bits  │
 * 6 bit    │ 30 bits     │ - 48-bits in total. └─────────┴─────────┴──────────┴─────────────┘
 *
 * <p>DATETIME is just a concatenation of DATE and TIME values.
 *
 * <p>TIMESTAMP has similar structure to {@link java.time.Instant} and supports precision from 1 second up to 1 nanosecond. Fractional
 * seconds part is stored in a separate bit sequence which is omitted for {@code 0} accuracy.
 *
 * <p>Total value size is 8/12 bytes depending on the type precision.
 *
 * <p>Timestamp compact structure: ┌──────────────────────────┬─────────────┐ │ Seconds since the epoch  │ Sub-seconds │
 * ├──────────────────────────┼─────────────┤ │    64 bits               │ 0/32 bits   │ └──────────────────────────┴─────────────┘
 *
 * @see org.apache.ignite.internal.schema.row.Row
 * @see org.apache.ignite.internal.schema.row.RowAssembler
 */
public class TemporalTypesHelper {
    /** Month field length. */
    public static final int MONTH_FIELD_LENGTH = 4;

    /** Day field length. */
    public static final int DAY_FIELD_LENGTH = 5;

    /** Hours field length. */
    public static final int HOUR_FIELD_LENGTH = 5;

    /** Minutes field length. */
    public static final int MINUTES_FIELD_LENGTH = 6;

    /** Seconds field length. */
    public static final int SECONDS_FIELD_LENGTH = 6;

    /** Max year boundary. */
    public static final int MAX_YEAR = (1 << 14) - 1;

    /** Min year boundary. */
    public static final int MIN_YEAR = -(1 << 14);

    /** Fractional part length for millis precision. */
    public static final int MILLISECOND_PART_LEN = 14;

    /** Fractional part mask for millis precision. */
    public static final long MILLISECOND_PART_MASK = (1L << MILLISECOND_PART_LEN) - 1;

    /** Fractional part length for nanos precision. */
    public static final int NANOSECOND_PART_LEN = 30;

    /** Fractional part mask for nanos precision. */
    public static final long NANOSECOND_PART_MASK = (1L << NANOSECOND_PART_LEN) - 1;

    /**
     * @param len Mask length in bits.
     * @return Mask.
     */
    private static int mask(int len) {
        return (1 << len) - 1;
    }

    /**
     * Compact LocalDate.
     *
     * @param date Date.
     * @return Encoded date.
     */
    public static int encodeDate(LocalDate date) {
        int val = date.getYear() << MONTH_FIELD_LENGTH;
        val = (val | date.getMonthValue()) << DAY_FIELD_LENGTH;
        val |= date.getDayOfMonth();

        return val & (0x00FF_FFFF);
    }

    /**
     * Expands to LocalDate.
     *
     * @param date Encoded date.
     * @return LocalDate instance.
     */
    public static LocalDate decodeDate(int date) {
        date = (date << 8) >> 8; // Restore sign.

        int day = (date) & mask(DAY_FIELD_LENGTH);
        int mon = (date >>= DAY_FIELD_LENGTH) & mask(MONTH_FIELD_LENGTH);
        int year = (date >> MONTH_FIELD_LENGTH); // Sign matters.

        return LocalDate.of(year, mon, day);
    }

    /**
     * Encode LocalTime to long as concatenation of 2 int values: encoded time with precision of seconds and fractional seconds.
     *
     * @param type      Native temporal type.
     * @param localTime Time.
     * @return Encoded local time.
     * @see #NANOSECOND_PART_LEN
     * @see #MILLISECOND_PART_LEN
     */
    public static long encodeTime(TemporalNativeType type, LocalTime localTime) {
        int time = localTime.getHour() << (MINUTES_FIELD_LENGTH + SECONDS_FIELD_LENGTH);
        time |= localTime.getMinute() << SECONDS_FIELD_LENGTH;
        time |= localTime.getSecond();

        int fractional = truncateTo(type.precision(), localTime.getNano());

        return ((long) time << 32) | fractional;
    }

    /**
     * Decode to LocalTime.
     *
     * @param type Type.
     * @param time Encoded time.
     * @return LocalTime instance.
     */
    public static LocalTime decodeTime(TemporalNativeType type, long time) {
        int fractional = (int) time;
        int time0 = (int) (time >>> 32);

        int sec = time0 & mask(SECONDS_FIELD_LENGTH);
        int min = (time0 >>>= SECONDS_FIELD_LENGTH) & mask(MINUTES_FIELD_LENGTH);
        int hour = (time0 >>> MINUTES_FIELD_LENGTH) & mask(HOUR_FIELD_LENGTH);

        // Convert to nanoseconds.
        switch (type.precision()) {
            case 0:
                break;
            case 1:
            case 2:
            case 3: {
                fractional *= 1_000_000;
                break;
            }
            case 4:
            case 5:
            case 6: {
                fractional *= 1_000;
                break;
            }
            default:
                break;
        }

        return LocalTime.of(hour, min, sec, fractional);
    }

    /**
     * Normalize nanoseconds regarding the precision.
     *
     * @param nanos     Nanoseconds.
     * @param precision Meaningful digits.
     * @return Normalized nanoseconds.
     */
    public static int normalizeNanos(int nanos, int precision) {
        switch (precision) {
            case 0:
                nanos = 0;
                break;
            case 1:
                nanos = (nanos / 100_000_000) * 100_000_000; // 100ms precision.
                break;
            case 2:
                nanos = (nanos / 10_000_000) * 10_000_000; // 10ms precision.
                break;
            case 3: {
                nanos = (nanos / 1_000_000) * 1_000_000; // 1ms precision.
                break;
            }
            case 4: {
                nanos = (nanos / 100_000) * 100_000; // 100mcs precision.
                break;
            }
            case 5: {
                nanos = (nanos / 10_000) * 10_000; // 10mcs precision.
                break;
            }
            case 6: {
                nanos = (nanos / 1_000) * 1_000; // 1mcs precision.
                break;
            }
            case 7: {
                nanos = (nanos / 100) * 100; // 100ns precision.
                break;
            }
            case 8: {
                nanos = (nanos / 10) * 10; // 10ns precision.
                break;
            }
            case 9: {
                // 1ns precision
                break;
            }
            default: // Should never get here.
                throw new IllegalArgumentException("Unsupported fractional seconds precision: " + precision);
        }

        return nanos;
    }

    /**
     * Normalize to given precision and truncate to meaningful time unit.
     *
     * @param precision Precision.
     * @param nanos     Seconds' fractional part.
     * @return Truncated fractional seconds (millis, micros or nanos).
     */
    private static int truncateTo(int precision, int nanos) {
        switch (precision) {
            case 0:
                return 0;
            case 1:
                return (nanos / 100_000_000) * 100; // 100ms precision.
            case 2:
                return (nanos / 10_000_000) * 10; // 10ms precision.
            case 3: {
                return nanos / 1_000_000; // 1ms precision.
            }
            case 4: {
                return (nanos / 100_000) * 100; // 100mcs precision.
            }
            case 5: {
                return (nanos / 10_000) * 10; // 10mcs precision.
            }
            case 6: {
                return nanos / 1_000; // 1mcs precision.
            }
            case 7: {
                return (nanos / 100) * 100; // 100ns precision.
            }
            case 8: {
                return (nanos / 10) * 10; // 10ns precision.
            }
            case 9: {
                return nanos; // 1ns precision
            }
            default: // Should never get here.
                throw new IllegalArgumentException("Unsupported fractional seconds precision: " + precision);
        }
    }
}
