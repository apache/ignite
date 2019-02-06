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

package org.apache.ignite.spi;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Strategy which incorporates retriable network operation, handling of totalTimeout logic.
 * It increases startTimeout based on exponential backoff algorithm.
 *
 * If failure detection is enabled it relies on totalTimeout
 * otherwise implements exponential backoff totalTimeout logic based on startTimeout, maxTimeout and retryCnt.
 */
public class ExponentialBackoffTimeoutStrategy implements TimeoutStrategy {
    /** Default backoff coefficient to calculate next timeout based on backoff strategy. */
    private static final double DLFT_BACKOFF_COEFF = 2.0;

    /** Initial startTimeout, ms. */
    private final long startTimeout;

    /** Max startTimeout of the next try, ms. */
    private final long maxTimeout;

    /** Max reconnections count. */
    private final int reconCnt;

    /** Total startTimeout, ms. */
    private final long totalTimeout;

    /** Start of operation to check totalTimeout. */
    private final long start;

    /** Current startTimeout, ms. */
    private long currTimeout;

    /**
     * Compute expected backoffConnTimeout delay based on startTimeout, maxTimeout and reconCnt and backoff coeffient.
     *
     * @param initTimeout Initial timeout.
     * @param maxTimeout Max Timeout per retry.
     * @param reconCnt Reconnection count.
     * @return Calculated total backoff timeout.
     */
    public static long totalBackoffTimeout(
            long initTimeout,
            long maxTimeout,
            long reconCnt
    ) {
        long maxBackoffTimeout = initTimeout;

        for (int i = 1; i < reconCnt && maxBackoffTimeout < maxTimeout; i++)
            maxBackoffTimeout += nextTimeout(maxBackoffTimeout, maxTimeout);

        return maxBackoffTimeout;
    }

    /**
     *
     * @param timeout Timeout.
     * @param maxTimeout Maximum startTimeout for backoff function.
     * @return Next exponetial backoff totalTimeout.
     */
    public static long nextTimeout(long timeout, long maxTimeout) {
        return (long) Math.min(timeout * DLFT_BACKOFF_COEFF, maxTimeout);
    }

    /**
     *
     * @param totalTimeout Total startTimeout.
     * @param startTimeout Initial connection timeout.
     * @param maxTimeout Max connection Timeout.
     * @param reconCnt Max number of reconnects.
     *
     */
    public ExponentialBackoffTimeoutStrategy(
        long totalTimeout,
        long startTimeout,
        long maxTimeout,
        int reconCnt
    ) {
        this.totalTimeout = totalTimeout;

        this.startTimeout = startTimeout;
        this.maxTimeout = maxTimeout;

        currTimeout = startTimeout;

        this.reconCnt = reconCnt;

        start = U.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public long getAndCalculateNextTimeout() throws IgniteSpiOperationTimeoutException {
        long remainingTime = remainingTime(U.currentTimeMillis());

        if (remainingTime <= 0)
            throw new IgniteSpiOperationTimeoutException("Operation timed out [startTimeout = " +this +"]");

        long currTimeout0 = currTimeout;

        currTimeout = nextTimeout(currTimeout, maxTimeout);

        return Math.min(currTimeout0, remainingTime);
    }

    /**
     * Returns remaining time for current totalTimeout chunk.
     *
     * @param curTs Current time stamp.
     * @return Time to wait in millis.
     */
    public long remainingTime(long curTs) {
        return totalTimeout - (curTs - start);
    }

    /**
     *
     * @param timeInFut Some time in millis in future.
     * @return {@code True} if startTimeout enabled.
     */
    @Override public boolean checkTimeout(long timeInFut) {
        return remainingTime(U.currentTimeMillis() + timeInFut) <= 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExponentialBackoffTimeoutStrategy.class, this);
    }
}
