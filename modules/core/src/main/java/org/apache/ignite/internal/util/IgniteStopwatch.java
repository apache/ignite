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

/*
 * Copyright (C) 2008 The Guava Authors
 */

package org.apache.ignite.internal.util;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;
import org.jetbrains.annotations.NotNull;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * An object that measures elapsed time in nanoseconds. It is useful to measure elapsed time using
 * this class instead of direct calls to {@link System#nanoTime} for a few reasons:
 *
 * <ul>
 *   <li>An alternate time source can be substituted, for testing or performance reasons.
 *   <li>As documented by {@code nanoTime}, the value returned has no absolute meaning, and can only
 *       be interpreted as relative to another timestamp returned by {@code nanoTime} at a different
 *       time. {@code Stopwatch} is a more effective abstraction because it exposes only these
 *       relative values, not the absolute ones.
 * </ul>
 *
 * <p>Basic usage:
 *
 * <pre>{@code
 * Stopwatch stopwatch = Stopwatch.createStarted();
 * doSomething();
 * stopwatch.stop(); // optional
 *
 * Duration duration = stopwatch.elapsed();
 *
 * log.info("time: " + stopwatch); // formatted string like "12.3 ms"
 * }</pre>
 *
 * <p>Stopwatch methods are not idempotent; it is an error to start or stop a stopwatch that is
 * already in the desired state.
 *
 * <p>When testing code that uses this class, use {@link #createUnstarted(IgniteTicker)} or {@link
 * #createStarted(IgniteTicker)} to supply a fake or mock ticker. This allows you to simulate any valid
 * behavior of the stopwatch.
 *
 * <p><b>Note:</b> This class is not thread-safe.
 *
 * <p><b>Warning for Android users:</b> a stopwatch with default behavior may not continue to keep
 * time while the device is asleep. Instead, create one like this:
 *
 * <pre>{@code
 * Stopwatch.createStarted(
 *      new Ticker() {
 *        public long read() {
 *          return android.os.SystemClock.elapsedRealtimeNanos();
 *        }
 *      });
 * }</pre>
 */
@SuppressWarnings("GoodTime") // lots of violations
public final class IgniteStopwatch {
    /** Ticker. */
    private final IgniteTicker ticker;

    /** Is running. */
    private boolean isRunning;

    /** Elapsed nanos. */
    private long elapsedNanos;

    /** Start tick. */
    private long startTick;

    /**
     * Creates (but does not start) a new stopwatch using {@link System#nanoTime} as its time source.
     */
    public static IgniteStopwatch createUnstarted() {
        return new IgniteStopwatch();
    }

    /**
     * Creates (but does not start) a new stopwatch, using the specified time source.
     */
    public static IgniteStopwatch createUnstarted(IgniteTicker ticker) {
        return new IgniteStopwatch(ticker);
    }

    /**
     * Creates (and starts) a new stopwatch using {@link System#nanoTime} as its time source.
     */
    public static IgniteStopwatch createStarted() {
        return new IgniteStopwatch().start();
    }

    /**
     * Creates (and starts) a new stopwatch, using the specified time source.
     */
    public static IgniteStopwatch createStarted(IgniteTicker ticker) {
        return new IgniteStopwatch(ticker).start();
    }

    /**
     * Execution given operation and calculation it time.
     *
     * @param log Logger fol logging.
     * @param operationName Operation name for logging.
     * @param operation Operation for execution.
     * @throws IgniteCheckedException If failed.
     */
    public static void logTime(
        IgniteLogger log,
        String operationName,
        IgniteThrowableRunner operation
    ) throws IgniteCheckedException {
        long start = System.currentTimeMillis();

        if (log.isInfoEnabled())
            log.info("Operation was started: " + operationName);

        try {
            operation.run();
        }
        catch (Throwable ex) {
            if (log.isInfoEnabled())
                log.info("Operation failed [operation=" + operationName
                    + ", elapsedTime=" + (System.currentTimeMillis() - start) + "ms]");

            throw ex;
        }

        if (log.isInfoEnabled())
            log.info("Operation succeeded [operation=" + operationName
                + ", elapsedTime=" + (System.currentTimeMillis() - start) + "ms]");
    }

    /**
     * Default constructor.
     */
    IgniteStopwatch() {
        this.ticker = IgniteTicker.systemTicker();
    }

    /**
     * @param ticker Ticker.
     */
    IgniteStopwatch(@NotNull IgniteTicker ticker) {
        this.ticker = ticker;
    }

    /**
     * Returns {@code true} if {@link #start()} has been called on this stopwatch, and {@link #stop()}
     * has not been called since the last call to {@code start()}.
     */
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * Starts the stopwatch.
     *
     * @return this {@code Stopwatch} instance
     * @throws IllegalStateException if the stopwatch is already running.
     */
    public IgniteStopwatch start() {
        assert !isRunning : "This stopwatch is already running.";

        isRunning = true;

        startTick = ticker.read();

        return this;
    }

    /**
     * Stops the stopwatch. Future reads will return the fixed duration that had elapsed up to this
     * point.
     *
     * @return this {@code Stopwatch} instance
     * @throws IllegalStateException if the stopwatch is already stopped.
     */
    public IgniteStopwatch stop() {
        long tick = ticker.read();

        assert !isRunning : "This stopwatch is already running.";

        isRunning = false;
        elapsedNanos += tick - startTick;
        return this;
    }

    /**
     * Sets the elapsed time for this stopwatch to zero, and places it in a stopped state.
     *
     * @return this {@code Stopwatch} instance
     */
    public IgniteStopwatch reset() {
        elapsedNanos = 0;

        isRunning = false;

        return this;
    }

    /**
     *
     */
    private long elapsedNanos() {
        return isRunning ? ticker.read() - startTick + elapsedNanos : elapsedNanos;
    }

    /**
     * Returns the current elapsed time shown on this stopwatch, expressed in the desired time unit,
     * with any fraction rounded down.
     *
     * <p><b>Note:</b> the overhead of measurement can be more than a microsecond, so it is generally
     * not useful to specify {@link TimeUnit#NANOSECONDS} precision here.
     *
     * <p>It is generally not a good idea to use an ambiguous, unitless {@code long} to represent
     * elapsed time. Therefore, we recommend using {@link #elapsed()} instead, which returns a
     * strongly-typed {@link Duration} instance.
     */
    public long elapsed(TimeUnit desiredUnit) {
        return desiredUnit.convert(elapsedNanos(), NANOSECONDS);
    }

    /**
     * Returns the current elapsed time shown on this stopwatch as a {@link Duration}. Unlike {@link
     * #elapsed(TimeUnit)}, this method does not lose any precision due to rounding.
     */
    public Duration elapsed() {
        return Duration.ofNanos(elapsedNanos());
    }

    /**
     * @param nanos Nanos.
     */
    private static TimeUnit chooseUnit(long nanos) {
        if (DAYS.convert(nanos, NANOSECONDS) > 0) return DAYS;
        if (HOURS.convert(nanos, NANOSECONDS) > 0) return HOURS;
        if (MINUTES.convert(nanos, NANOSECONDS) > 0) return MINUTES;
        if (SECONDS.convert(nanos, NANOSECONDS) > 0) return SECONDS;
        if (MILLISECONDS.convert(nanos, NANOSECONDS) > 0) return MILLISECONDS;
        if (MICROSECONDS.convert(nanos, NANOSECONDS) > 0) return MICROSECONDS;
        return NANOSECONDS;
    }
}
