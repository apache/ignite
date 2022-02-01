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

package org.apache.ignite.internal.processors.platform.cache.expiry;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

/**
 * Platform expiry policy.
 */
public class PlatformExpiryPolicy implements ExpiryPolicy {
    /** Duration: unchanged. */
    private static final long DUR_UNCHANGED = -2;

    /** Duration: eternal. */
    private static final long DUR_ETERNAL = -1;

    /** Duration: zero. */
    private static final long DUR_ZERO = 0;

    /** Expiry for create. */
    private final Duration create;

    /** Expiry for update. */
    private final Duration update;

    /** Expiry for access. */
    private final Duration access;

    /**
     * Constructor.
     *
     * @param create Expiry for create.
     * @param update Expiry for update.
     * @param access Expiry for access.
     */
    public PlatformExpiryPolicy(long create, long update, long access) {
        this.create = convert(create);
        this.update = convert(update);
        this.access = convert(access);
    }

    /** {@inheritDoc} */
    @Override public Duration getExpiryForCreation() {
        return create;
    }

    /** {@inheritDoc} */
    @Override public Duration getExpiryForUpdate() {
        return update;
    }

    /** {@inheritDoc} */
    @Override public Duration getExpiryForAccess() {
        return access;
    }

    /**
     * Convert encoded duration to actual duration.
     *
     * @param dur Encoded duration.
     * @return Actual duration.
     */
    private static Duration convert(long dur) {
        if (dur == DUR_UNCHANGED)
            return null;
        else if (dur == DUR_ETERNAL)
            return Duration.ETERNAL;
        else if (dur == DUR_ZERO)
            return Duration.ZERO;
        else {
            assert dur > 0;

            return new Duration(TimeUnit.MILLISECONDS, dur);
        }
    }

    /**
     * Convert actual duration to encoded duration for serialization.
     *
     * @param dur Actual duration.
     * @return Encoded duration.
     */
    public static long convertDuration(Duration dur) {
        if (dur == null)
            return DUR_UNCHANGED;
        else if (dur.isEternal())
            return DUR_ETERNAL;
        else if (dur.isZero())
            return DUR_ZERO;
        else
            return dur.getTimeUnit().toMillis(dur.getDurationAmount());
    }
}
