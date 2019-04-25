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

/*
 * Copyright (C) 2011 The Guava Authors
 */

package org.apache.ignite.internal.util;

/**
 * A time source; returns a time value representing the number of nanoseconds elapsed since some
 * fixed but arbitrary point in time. Note that most users should use {@link IgniteStopwatch} instead of
 * interacting with this class directly.
 *
 * <p><b>Warning:</b> this interface can only be used to measure elapsed time, not wall time.
 */
public abstract class IgniteTicker {
    /** Constructor for use by subclasses. */
    protected IgniteTicker() {}

    /** Returns the number of nanoseconds elapsed since this ticker's fixed point of reference. */
    public abstract long read();

    /**
     * A ticker that reads the current time using {@link System#nanoTime}.
     */
    public static IgniteTicker systemTicker() {
        return SYSTEM_TICKER;
    }

    /** System ticker. */
    private static final IgniteTicker SYSTEM_TICKER =
        new IgniteTicker() {
            @Override public long read() {
                return System.nanoTime();
            }
        };
}
