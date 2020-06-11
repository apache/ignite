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

import java.net.SocketException;
import java.net.SocketTimeoutException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Object that incorporates logic that determines a timeout value for the next network related operation and checks
 * whether a failure detection timeout is reached or not.
 *
 * A new instance of the class should be created for every complex network based operations that usually consists of
 * request and response parts.
 *
 */
public class IgniteSpiOperationTimeoutHelper {
    // https://issues.apache.org/jira/browse/IGNITE-11221
    // We need to reuse new logic ExponentialBackoffTimeout logic in TcpDiscovery instead of this class.

    /** */
    private long lastOperStartNanos;

    /** Time left for the operation in nanos. */
    private long timeLeft;

    /** Flag whether to use timeout. */
    private final boolean timeoutEnabled;

    /** Timeout on operation. */
    private final long timeout;

    /** Absolute time threshold which must not be reached. Ignired if negative. */
    private long absolteThreshold = -1L;

    /**
     * Constructor.
     *
     * @param adapter SPI adapter.
     * @param srvOp {@code True} if communicates with server node.
     */
    public IgniteSpiOperationTimeoutHelper(IgniteSpiAdapter adapter, boolean srvOp) {
        timeoutEnabled = adapter.failureDetectionTimeoutEnabled();

        timeout = U.millisToNanos(srvOp ? adapter.failureDetectionTimeout() :
            adapter.clientFailureDetectionTimeout());
    }

    /**
     * Creates timeout helper based on time of last related operation.
     *
     * @param adapter SPI adapter.
     * @param srvOp {@code True} if communicates with server node.
     * @param lastOperationNanos Time of last related operation in nanos.
     */
    public IgniteSpiOperationTimeoutHelper(IgniteSpiAdapter adapter, boolean srvOp, long lastOperationNanos) {
        this(adapter, srvOp);

        lastOperStartNanos = lastOperationNanos;

        if (lastOperationNanos > 0)
            timeLeft = timeout;
    }

    /**
     * Returns a timeout value to use for the next network operation.
     *
     * If failure detection timeout is enabled then the returned value is a portion of time left since the last time
     * this method is called. If the timeout is disabled then {@code dfltTimeout} is returned.
     *
     * @param dfltTimeout Timeout to use if failure detection timeout is disabled.
     * @return Timeout in milliseconds.
     * @throws IgniteSpiOperationTimeoutException If failure detection timeout is reached for an operation that uses
     * this {@code IgniteSpiOperationTimeoutController}.
     */
    public long nextTimeoutChunk(long dfltTimeout) throws IgniteSpiOperationTimeoutException {
        if (!timeoutEnabled)
            return checkedNextChunk(U.millisToNanos(dfltTimeout));

        if (lastOperStartNanos <= 0) {
            lastOperStartNanos = System.nanoTime();

            timeLeft = timeout;
        }
        else {
            long curNanos = System.nanoTime();

            timeLeft -= curNanos - lastOperStartNanos;

            lastOperStartNanos = curNanos;
        }

        return checkedNextChunk(timeLeft);
    }

    /** If the absolute time threshold is set, checks it is not reached. */
    private long checkedNextChunk(long nextTimeoutChunk) throws IgniteSpiOperationTimeoutException {
        if (nextTimeoutChunk > 0 && absolteThreshold > 0 && lastOperStartNanos + nextTimeoutChunk > absolteThreshold)
            nextTimeoutChunk = absolteThreshold - lastOperStartNanos;

        if (nextTimeoutChunk <= 0) {
            throw new IgniteSpiOperationTimeoutException("Network operation timed out. Timeout: " +
                U.nanosToMillis(nextTimeoutChunk));
        }

        return U.nanosToMillis(nextTimeoutChunk);
    }

    /**
     * Sets absolute time threshold which must never be reached. Ignired if negative.
     *
     * @param absolteTimeThreshold Maximum time threshold in nonos.
     */
    public void absolteTimeThreshold(long absolteTimeThreshold) {
        absolteThreshold = absolteTimeThreshold;
    }

    /**
     * Checks whether the given {@link Exception} is generated because failure detection timeout has been reached.
     *
     * @param e Exception.
     * @return {@code true} if failure detection timeout is reached, {@code false} otherwise.
     */
    public boolean checkFailureTimeoutReached(Exception e) {
        if (!timeoutEnabled)
            return false;

        if (X.hasCause(e, IgniteSpiOperationTimeoutException.class, SocketTimeoutException.class, SocketException.class))
            return true;

        return lastOperStartNanos + timeout < System.nanoTime() &&
            (absolteThreshold <= 0 || lastOperStartNanos + timeout < absolteThreshold);
    }
}
