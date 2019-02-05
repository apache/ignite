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

import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.IOException;

/**
 * Strategy which incorporates retriable network operation, handling of totalTimeout logic.
 *
 * If failure detection is enabled it relies on failureDetectionTimeout
 * otherwise implements exponential backoff totalTimeout logic based on connTimeout, maxConnTimeout and retryCnt.
 *
 * It contains 4 totalTimeout types:
 *  - connection totalTimeout to wait for socket.connect()
 *  - handshake totalTimeout to handle handshake after connect.
 *  - recoonectDelay, sleep between retries on temporary network failures.
 *  - outOfTopologyDelay, sleep between retries, when initiator or target doesn't observe each other in topology.
 */
public class ExponentialBackoffSpiTimeout {
    /** Default backoff coefficient. */
    private static final int DLFT_BACKOFF_COEFF = 2;

    /** Default initial delay for out of topology and reconnects. */
    private static final int DLFT_INITIAL_DELAY = 200;

    /** Initial connection totalTimeout. */
    private final long connTimeout;

    /** Max connection totalTimeout. */
    private final long maxConnTimeout;

    /** Max reconnections count. */
    private final int reconCnt;

    /** Backoff coefficient */
    private final int backoffCoeff = DLFT_BACKOFF_COEFF;

    /** Calculated max totalTimeout. */
    private final long totalTimeout;

    /** Start of operation to check totalTimeout. */
    private final long start;

    /** Failure detection timeout. */
    private final long failureDetectionTimeout;

    /** Failure detection timeout enabled. */
    private final boolean failureDetectionTimeoutEnabled;

    /** Current connection totalTimeout, ms. */
    private long currConnTimeout;

    /** Current handshake totalTimeout, ms. */
    private long currHandshakeTimeout;

    /** Current out of topology delay, ms. */
    private long currOutOfTopDelay = DLFT_INITIAL_DELAY;

    /** Delay between reconnects in case of recoverable network errors (like unavailable network routes, DNS), ms. */
    private long currReconnectDelay = DLFT_INITIAL_DELAY;

    /**
     *
     * @param failureDetectionTimeoutEnabled Whether failureDetectionTimeout enabled or not.
     * @param failureDetectionTimeout FailureDetectionTimeout.
     * @param connTimeout Initial connection totalTimeout, will be taken for initial connection and handshake timeouts.
     * @param maxConnTimeout Max connection Timeout.
     * @param reconCnt Max number of reconnects.
     */
    public ExponentialBackoffSpiTimeout(
        boolean failureDetectionTimeoutEnabled,
        long failureDetectionTimeout,
        long connTimeout,
        long maxConnTimeout,
        int reconCnt
    ) {
        this.failureDetectionTimeout = failureDetectionTimeout;
        this.failureDetectionTimeoutEnabled = failureDetectionTimeoutEnabled;

        this.connTimeout = connTimeout;
        this.maxConnTimeout = maxConnTimeout;

        currHandshakeTimeout = connTimeout;
        currConnTimeout = connTimeout;

        this.reconCnt = reconCnt;

        totalTimeout = failureDetectionTimeoutEnabled ? failureDetectionTimeout : maxBackoffTimeout();

        start = U.currentTimeMillis();
    }

    /**
     * Get next connection timeout as min(calculated exponential timeout, remainingTime).
     * @return Next Timeout value;
     */
    public int connTimeout() throws IOException {
        long remainingTime = remainingTime(U.currentTimeMillis());

        if (remainingTime <= 0)
            throw new IOException("Connect operation timed out [timeout = " +this +"]");

        return (int) Math.min((int)currConnTimeout, remainingTime);
    }

    /**
     * Get next connection timeout as min(calculated exponential timeout, remainingTime).
     *
     * @return Next Timeout value;
     * @throws IgniteSpiOperationTimeoutException in case of timeout reached at the moment.
     */
    public long handshakeTimeout() throws IgniteSpiOperationTimeoutException {
        long remainingTime = remainingTime(U.currentTimeMillis());

        if (remainingTime <= 0)
            throw new IgniteSpiOperationTimeoutException("Network operation timed out [timeout = " +this +"]");

        return (int) Math.min((int) currHandshakeTimeout, remainingTime);
    }

    /**
     *
     * @return Gets current value of outOfTopologyDelay and calculates next backoff value;
     */
    public long getAndBackOffOutOfTopDelay() {
        long currOutOfTopDelay0 = currOutOfTopDelay;

        currOutOfTopDelay = nextTimeout(currOutOfTopDelay);

        return currOutOfTopDelay0;
    }

    /**
     *
     * @return Current reconnect delay.
     */
    public long getAndBackoffReconnectDelay() {
        long currReconnectDelay0 = currReconnectDelay;

        currReconnectDelay = nextTimeout(currReconnectDelay);

        return currReconnectDelay0;
    }

    /**
     * Calculate next backoffConnTimeout totalTimeout value.
     */
    public void backoffConnTimeout() {
        currConnTimeout = nextTimeout(currConnTimeout);
    }

    /**
     * Calculate next backoffConnTimeout totalTimeout value.
     */
    public void backoffHandshakeTimeout() {
        currHandshakeTimeout = nextTimeout(currHandshakeTimeout);
    }

    /**
     *
     * @param timeout Timeout.
     * @return Next exponetial backoff totalTimeout.
     */
    private long nextTimeout(long timeout) {
        return Math.min(timeout * backoffCoeff, maxConnTimeout);
    }

    /** Compute expected backoffConnTimeout delay based on connTimeout, maxConnTimeout and reconCnt. */
    private long maxBackoffTimeout() {
        long maxBackoffTimeout = connTimeout;

        for (int i = 1; i < reconCnt && maxBackoffTimeout < maxConnTimeout; i++)
            maxBackoffTimeout += nextTimeout(maxBackoffTimeout);

        return maxBackoffTimeout;
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
     * @return True if timeout enabled.
     */
    public boolean checkTimeout(long timeInFut) {
        return remainingTime(U.currentTimeMillis() + timeInFut) <= 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ExponentialBackoffSpiTimeout{" +
                "connTimeout=" + connTimeout +
                ", maxConnTimeout=" + maxConnTimeout +
                ", reconCnt=" + reconCnt +
                ", backoffCoeff=" + backoffCoeff +
                ", totalTimeout=" + totalTimeout +
                ", start=" + start +
                ", failureDetectionTimeout=" + failureDetectionTimeout +
                ", failureDetectionTimeoutEnabled=" + failureDetectionTimeoutEnabled +
                ", currConnTimeout=" + currConnTimeout +
                ", currHandshakeTimeout=" + currHandshakeTimeout +
                ", currOutOfTopDelay=" + currOutOfTopDelay +
                ", currReconnectDelay=" + currReconnectDelay +
                '}';
    }
}
