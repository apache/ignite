/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.value.ValueTimestampTimeZone;

public final class CurrentTimestamp {

    /*
     * Signatures of methods should match with
     * h2/src/java9/src/org/h2/util/CurrentTimestamp.java and precompiled
     * h2/src/java9/precompiled/org/h2/util/CurrentTimestamp.class.
     */

    /**
     * Returns current timestamp.
     *
     * @return current timestamp
     */
    public static ValueTimestampTimeZone get() {
        return DateTimeUtils.timestampTimeZoneFromMillis(System.currentTimeMillis());
    }

    private CurrentTimestamp() {
    }

}
