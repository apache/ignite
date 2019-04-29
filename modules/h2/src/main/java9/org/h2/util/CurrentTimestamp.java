/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.time.Instant;

import org.h2.value.ValueTimestampTimeZone;

public final class CurrentTimestamp {

    /**
     * Returns current timestamp.
     *
     * @return current timestamp
     */
    public static ValueTimestampTimeZone get() {
        Instant now = Instant.now();
        long second = now.getEpochSecond();
        int nano = now.getNano();
        /*
         * This code intentionally does not support properly dates before UNIX
         * epoch and time zone offsets with seconds because such support is not
         * required for current dates.
         */
        int offsetSec = DateTimeUtils.getTimeZoneOffset(second * 1_000 + nano / 1_000_000) / 1000;
        second += offsetSec;
        return ValueTimestampTimeZone.fromDateValueAndNanos(
                DateTimeUtils.dateValueFromAbsoluteDay(second / DateTimeUtils.SECONDS_PER_DAY),
                second % DateTimeUtils.SECONDS_PER_DAY * 1_000_000_000 + nano, (short) (offsetSec / 60));
    }

    private CurrentTimestamp() {
    }

}
