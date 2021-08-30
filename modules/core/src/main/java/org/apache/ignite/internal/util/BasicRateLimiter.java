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

import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
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
    private double stableIntervalMicros;

    /**
     * The time when the next request (no matter its size) will be granted. After granting a request,
     * this is pushed further in the future. Large requests push this further than small requests.
     */
    private long nextFreeTicketMicros;

    /**
     * The flag indicates that the rate is not limited.
     */
    private volatile boolean unlimited;

    /**
     * @param permitsPerSecond Estimated number of permits per second.
     */
    public BasicRateLimiter(double permitsPerSecond) {
        setRate(permitsPerSecond);
    }

    /**
     * Updates the stable rate.
     *
     * @param permitsPerSecond The new stable rate of this {@code RateLimiter}, set {@code 0} for unlimited rate.
     * @throws IllegalArgumentException If {@code permitsPerSecond} is negative or zero.
     */
    public void setRate(double permitsPerSecond) {
        A.ensure(permitsPerSecond >= 0, "Requested permits (" + permitsPerSecond + ") must be non-negative.");

        if (unlimited = (permitsPerSecond == 0))
            return;

        synchronized (mux) {
            resync();

            stableIntervalMicros = SECONDS.toMicros(1L) / permitsPerSecond;
        }
    }

    /**
     * @return The stable rate as {@code permits per seconds} ({@code 0} means that the rate is unlimited).
     */
    public double getRate() {
        if (unlimited)
            return 0;

        synchronized (mux) {
            return SECONDS.toMicros(1L) / stableIntervalMicros;
        }
    }

    /**
     * Acquires the given number of permits from this {@code RateLimiter}, blocking until the request
     * can be granted. Tells the amount of time slept, if any.
     *
     * @param permits The number of permits to acquire.
     * @throws IllegalArgumentException If the requested number of permits is negative or zero.
     */
    public void acquire(int permits) throws IgniteInterruptedCheckedException {
        if (unlimited)
            return;

        long microsToWait = reserve(permits);

        try {
            MICROSECONDS.sleep(microsToWait);
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
     * @return Time in microseconds to wait until the resource can be acquired, never negative.
     */
    private long reserve(int permits) {
        A.ensure(permits > 0, "Requested permits (" + permits + ") must be positive");

        synchronized (mux) {
            long nowMicros = resync();

            long momentAvailable = nextFreeTicketMicros;

            nextFreeTicketMicros = momentAvailable + (long)(permits * stableIntervalMicros);

            return max(momentAvailable - nowMicros, 0);
        }
    }

    /**
     * Updates {@code nextFreeTicketMicros} based on the current time.
     *
     * @return Time passed (since start) in microseconds.
     */
    private long resync() {
        long passed = MICROSECONDS.convert(System.nanoTime() - startTime, NANOSECONDS);

        // if nextFreeTicket is in the past, resync to now
        if (passed > nextFreeTicketMicros)
            nextFreeTicketMicros = passed;

        return passed;
    }
}
