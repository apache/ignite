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

/**
 * Strategy which incorporates retriable network operation, handling of totalTimeout logic.
 *
 * If failure detection is enabled it relies on failureDetectionTimeout
 * otherwise implements exponential backoff totalTimeout logic based on connTimeout, maxConnTimeout and retryCnt.
 *
 * It contains 4 totalTimeout types:
 *  - connection totalTimeout to wait for socket.connect()
 *  - handshake totalTimeout to handle handshake after execution.
 *  - recoonectDelay, sleep between retries on temporary network failures.
 *  - outOfTopologyDelay, sleep between retries, when initiator or target doesn't observe each other in topology.
 */
public class ExponentialBackoffSpiTimeout {
    /** Default backoff coefficient. */
    private static final int DLFT_BACKOFF_COEFF = 2;

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

    /** Current connection totalTimeout, ms. */
    private long currConnTimeout;

    /** Current handshake totalTimeout, ms. */
    private long currHandshakeTimeout;

    /** Current out of topology delay, ms. */
    private long currOutOfTopDelay = 200;

    /** Delay between reconnects in case of recoverable network errors (like unavailable network routes, DNS), ms. */
    private long currReconnectDelay = 200;

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
        this.connTimeout = connTimeout;
        this.maxConnTimeout = maxConnTimeout;

        currHandshakeTimeout = connTimeout;
        currConnTimeout = connTimeout;

        this.reconCnt = reconCnt;

        totalTimeout = failureDetectionTimeoutEnabled ? failureDetectionTimeout : maxBackoffTimeout();

        start = U.currentTimeMillis();
    }

    /**
     * Get next timeout calculated on exponential backoffConnTimeout function.
     * @return Timeout value;
     */
    public int connTimeout() {
        return (int)currConnTimeout;
    }

    /**
     * @return Current hanshakeTimeout.
     */
    public long handshakeTimeout() {
        return currHandshakeTimeout;
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
            maxBackoffTimeout += nextTimeout(connTimeout);

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
     * Check whether totalTimeout of operation reached.
     *
     * @return True if totalTimeout reached.
     */
    public boolean checkTimeout() {
        return remainingTime(U.currentTimeMillis()) <= 0;
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
            ", currConnTimeout=" + currConnTimeout +
            ", currHandshakeTimeout=" + currHandshakeTimeout +
            ", currOutOfTopDelay=" + currOutOfTopDelay +
            ", currReconnectDelay=" + currReconnectDelay +
            '}';
    }
}
