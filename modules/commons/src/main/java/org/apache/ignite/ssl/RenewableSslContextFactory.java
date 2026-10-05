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
 * Every {@link #create()} must return a context with a newly issued certificate. The node calls it when a transport starts, before the
 * node joins the cluster, on every {@code --ssl reload}, and when the certificate in use enters the renewal window. A renewed context is
 * rejected unless the node can tell its certificate, every certificate of its chain is valid now, the chain expires later than the one
 * in use, and, if the factory serves discovery or communication, a handshake with the node's own trust store accepts it. That handshake
 * uses the trusted authorities of the returned context, so take them from a source local to the node, not from the issuing service.
 * <p>
 * All renewals of a node run in one thread: bound every wait for the issuing service well below {@link #getRenewalRetryMinInterval()}
 * and stop waiting when the thread is interrupted, which is how a stopping node ends an attempt. A node calls {@link #create()} of an
 * instance once at a time, but nodes that share an instance in one JVM may call it concurrently. The node reads the settings below once
 * and does not start if they are out of range. It does not release what the factory holds.
 */
public interface RenewableSslContextFactory extends Factory<SSLContext> {
    /** Default share of the certificate lifetime left when the node renews it. */
    public static final double DFLT_RENEW_BEFORE_FRACTION = 0.15;

    /** Default pause after the first failed renewal, in milliseconds. */
    public static final long DFLT_RENEWAL_RETRY_MIN_INTERVAL = 60_000L;

    /** Default longest pause between failed renewals, in milliseconds. */
    public static final long DFLT_RENEWAL_RETRY_MAX_INTERVAL = 3_600_000L;

    /**
     * @return Share of the certificate lifetime, from its start to the earliest expiry in its chain, left when the node renews it;
     *      greater than {@code 0} and less than {@code 1}.
     */
    public default double getRenewBeforeFraction() {
        return DFLT_RENEW_BEFORE_FRACTION;
    }

    /**
     * @return Maximum renewal window, in milliseconds, not negative; the node takes the smaller of it and the window from
     *      {@link #getRenewBeforeFraction()}. {@code 0} means no maximum.
     */
    public default long getRenewBefore() {
        return 0;
    }

    /**
     * @return Share of the window, from {@code 0} to {@code 1}, by which the node moves a renewal earlier at random, so that nodes whose
     *      certificates expire together do not renew them at the same moment; {@code renewBeforeFraction * (1 + renewalJitter)} must be
     *      less than {@code 1}.
     */
    public default double getRenewalJitter() {
        return 0;
    }

    /** @return Pause after the first failed renewal, in milliseconds, positive; the node never renews more often than this. */
    public default long getRenewalRetryMinInterval() {
        return DFLT_RENEWAL_RETRY_MIN_INTERVAL;
    }

    /** @return Longest pause between failed renewals, in milliseconds, not less than {@link #getRenewalRetryMinInterval()}. */
    public default long getRenewalRetryMaxInterval() {
        return DFLT_RENEWAL_RETRY_MAX_INTERVAL;
    }
}
