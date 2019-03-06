/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.platform.cache.expiry;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.concurrent.TimeUnit;

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
}
