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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements exponential backoff logic. Contains a counter and increments it on each {@link #nextDuration()}.
 * May be reset using {@link #reset()}.
 */
class ExponentialBackoff {
    /**
     * Starting backoff duration.
     */
    private final long startingBackoffNanos;

    /**
     * Backoff ratio. Each next duration will be this times longer.
     */
    private final double backoffRatio;

    /**
     * Exponential backoff counter.
     */
    private final AtomicInteger exponentialBackoffCounter = new AtomicInteger(0);

    /**
     * Constructs a new instance with the given parameters.
     *
     * @param startingBackoffNanos duration of first backoff in nanoseconds
     * @param backoffRatio         each next duration will be this times longer
     */
    public ExponentialBackoff(long startingBackoffNanos, double backoffRatio) {
        this.startingBackoffNanos = startingBackoffNanos;
        this.backoffRatio = backoffRatio;
    }

    /**
     * Returns next backoff duration (in nanoseconds). As a side effect, increments the backoff counter so that
     * next call will return a longer duration.
     *
     * @return next backoff duration in nanoseconds
     */
    public long nextDuration() {
        int exponent = exponentialBackoffCounter.getAndIncrement();
        return (long)(startingBackoffNanos * Math.pow(backoffRatio, exponent));
    }

    /**
     * Resets the exponential backoff counter so that next call to {@link #nextDuration()}
     * will return {@link #startingBackoffNanos}.
     *
     * @return {@code true} iff this backoff was not already in a reset state
     */
    public boolean reset() {
        int oldVal = exponentialBackoffCounter.getAndSet(0);
        return oldVal != 0;
    }
}
