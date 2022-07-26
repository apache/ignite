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

package org.apache.ignite.internal.util;

import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.A;

import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The simplified version of Google Guava smooth rate limiter.<br><br>
 *
 * The primary feature of a rate limiter is its "stable rate", the maximum rate that is should
 * allow at normal conditions. This is enforced by "throttling" incoming requests as needed, i.e.
 * compute, for an incoming request, the appropriate throttle time, and make the calling thread
 * wait as much.<br><br>
 *
 * The simplest way to maintain a rate of QPS is to keep the timestamp of the last granted
 * request, and ensure that (1/QPS) seconds have elapsed since then. For example, for a rate of
 * QPS=5 (5 tokens per second), if we ensure that a request isn't granted earlier than 200ms after
 * the last one, then we achieve the intended rate. If a request comes and the last request was
 * granted only 100ms ago, then we wait for another 100ms. At this rate, serving 15 fresh permits
 * (i.e. for an acquire(15) request) naturally takes 3 seconds.<br><br>
 *
 * It is important to realize that such a limiter has a very superficial memory of the past:
 * it only remembers the last request. if the limiter was unused for a long period of
 * time, then a request arrived and was immediately granted? This limiter would immediately
 * forget about that past underutilization.
 */
public class BasicRateLimiter {
    /** Start timestamp. */
    private final long startTime = System.nanoTime();

    /** Synchronization mutex. */
    private final Object mux = new Object();

    /**
     * The interval between two unit requests, at our stable rate. E.g., a stable rate of 5 permits
     * per second has a stable interval of 200ms.
     */
    private double stableIntervalNanos;

    /**
     * The time when the next request (no matter its size) will be granted. After granting a request,
     * this is pushed further in the future. Large requests push this further than small requests.
     */
    private long nextFreeTicketNanos;

    /** The currently stored permits. */
    private double storedPermits;

    /** The stable rate as {@code permits per seconds} ({@code 0} means that the rate is unlimited). */
    private volatile double rate;

    /**
     * @param permitsPerSecond Estimated number of permits per second.
     */
    public BasicRateLimiter(double permitsPerSecond) {
        setRate(permitsPerSecond);
    }

    /**
     * Reset internal state.
     */
    public void reset() {
        synchronized (mux) {
            nextFreeTicketNanos = System.nanoTime() - startTime;
            storedPermits = 0;
        }
    }

    /**
     * Updates the stable rate.
     *
     * @param permitsPerSecond The new stable rate of this {@code RateLimiter}, set {@code 0} for unlimited rate.
     * @throws IllegalArgumentException If {@code permitsPerSecond} is negative or zero.
     */
    public void setRate(double permitsPerSecond) {
        A.ensure(permitsPerSecond >= 0, "Requested permits (" + permitsPerSecond + ") must be non-negative.");

        if ((rate = permitsPerSecond) == 0)
            return;

        synchronized (mux) {
            stableIntervalNanos = SECONDS.toNanos(1L) / permitsPerSecond;

            reset();
        }
    }

    /**
     * @return The stable rate as {@code permits per seconds} ({@code 0} means that the rate is unlimited).
     */
    public double getRate() {
        return rate;
    }

    /**
     * @return {@code True} if the rate is not limited.
     */
    public boolean isUnlimited() {
        return rate == 0;
    }

    /**
     * Acquires the given number of permits from this {@code RateLimiter}, blocking until the request
     * can be granted. Tells the amount of time slept, if any.
     *
     * @param permits The number of permits to acquire.
     * @throws IllegalArgumentException If the requested number of permits is negative or zero.
     */
    public void acquire(long permits) throws IgniteInterruptedCheckedException {
        if (isUnlimited())
            return;

        long nanosToWait = reserve(permits);

        try {
            NANOSECONDS.sleep(nanosToWait);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Reserves the given number of permits for future use.
     *
     * @param permits The number of permits.
     * @return Time in nanoseconds to wait until the resource can be acquired.
     */
    private long reserve(long permits) {
        A.ensure(permits > 0, "Requested permits (" + permits + ") must be positive");

        synchronized (mux) {
            long nowNanos = resync();

            long momentAvailable = nextFreeTicketNanos;
            double storedPermitsToSpend = min(permits, storedPermits);
            double freshPermits = permits - storedPermitsToSpend;

            nextFreeTicketNanos = momentAvailable + (long)(freshPermits * stableIntervalNanos);
            storedPermits -= storedPermitsToSpend;

            return momentAvailable - nowNanos;
        }
    }

    /**
     * Updates {@code nextFreeTicketNanos} based on the current time.
     *
     * @return Time passed (since start) in nanoseconds.
     */
    private long resync() {
        long passed = System.nanoTime() - startTime;

        // if nextFreeTicket is in the past, resync to now.
        if (passed > nextFreeTicketNanos) {
            // This is the number of permits we can give for free because we've been inactive longer than expected.
            storedPermits = min(getRate(), storedPermits + ((passed - nextFreeTicketNanos) / stableIntervalNanos));

            nextFreeTicketNanos = passed;
        }

        return passed;
    }
}
