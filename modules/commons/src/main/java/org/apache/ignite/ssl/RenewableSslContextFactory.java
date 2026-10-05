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

package org.apache.ignite.ssl;

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;

/**
 * SSL context factory whose certificate the node renews by itself before it expires.
 * <p>
 * Every {@link #create()} must return a context with a newly obtained certificate, typically one requested from a
 * certificate authority service. The node calls it again once the certificate in use enters the renewal window, the
 * last part of its lifetime. The new context goes through the same checks as one the {@code --ssl reload} command
 * puts in use. It is also turned down if its certificate expires no later than the one in use, since such a renewal
 * gains nothing. A node does not call {@link #create()} again before the previous call has returned.
 * <p>
 * The window is a share of the lifetime of the certificate in use, from its start to the earliest expiry in its chain.
 * A service that issues shorter certificates than asked for therefore cannot make the node come back for a new one
 * over and over. An absolute window can be set as well, and the node then takes the smaller of the two.
 * <p>
 * A failed renewal is retried until it succeeds. The pause doubles after each failure, from
 * {@link #getRenewalRetryMinInterval()} up to {@link #getRenewalRetryMaxInterval()}, but never grows longer than a
 * quarter of the window, and a random delay is added to it. Each failure is logged as a warning, and as an error once
 * less than half of the window remains. A successful renewal, or a successful {@code --ssl reload}, stops the retries,
 * and the next renewal is planned by the new certificate.
 */
public interface RenewableSslContextFactory extends Factory<SSLContext> {
    /** Default share of the certificate lifetime left when the node renews it. */
    public static final double DFLT_RENEW_BEFORE_FRACTION = 0.15;

    /** Default pause after the first failed renewal, in milliseconds. */
    public static final long DFLT_RENEWAL_RETRY_MIN_INTERVAL = 60_000L;

    /** Default longest pause between failed renewals, in milliseconds. */
    public static final long DFLT_RENEWAL_RETRY_MAX_INTERVAL = 3_600_000L;

    /**
     * @return Share of the certificate lifetime left when the node renews it, greater than {@code 0} and less than
     *      {@code 1}. Default is {@link #DFLT_RENEW_BEFORE_FRACTION}.
     */
    public default double getRenewBeforeFraction() {
        return DFLT_RENEW_BEFORE_FRACTION;
    }

    /**
     * @return Time before expiry when the node renews the certificate at the latest, in milliseconds. The node takes
     *      the smaller of this window and the one {@link #getRenewBeforeFraction()} gives. {@code 0}, the default,
     *      leaves the window to the share of the lifetime alone.
     */
    public default long getRenewBefore() {
        return 0;
    }

    /**
     * @return Share of the window, from {@code 0} to {@code 1}, by which the node brings the renewal forward at random.
     *      Certificates issued at once expire at once, and the shift keeps their nodes from all coming for new ones
     *      at the same moment. Default is {@code 0}, no shift.
     */
    public default double getRenewalJitter() {
        return 0;
    }

    /**
     * @return Pause after the first failed renewal, in milliseconds. The node also never renews more often than
     *      this. Default is {@link #DFLT_RENEWAL_RETRY_MIN_INTERVAL}.
     */
    public default long getRenewalRetryMinInterval() {
        return DFLT_RENEWAL_RETRY_MIN_INTERVAL;
    }

    /**
     * @return Longest pause between failed renewals, in milliseconds, not less than
     *      {@link #getRenewalRetryMinInterval()}. Default is {@link #DFLT_RENEWAL_RETRY_MAX_INTERVAL}.
     */
    public default long getRenewalRetryMaxInterval() {
        return DFLT_RENEWAL_RETRY_MAX_INTERVAL;
    }
}
