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
    /** Flag whether to use timeout. */
    private final boolean timeoutEnabled;

    /** Time in nanos which cannot be reached for current operation. */
    private final long timeoutThreshold;

    /**
     * Constructor.
     *
     * @param adapter SPI adapter.
     * @param srvOp {@code True} if communicates with server node.
     */
    public IgniteSpiOperationTimeoutHelper(IgniteSpiAdapter adapter, boolean srvOp) {
        this(adapter, srvOp, -1, -1);
    }

    /**
     * Creates timeout helper based on time of last related operation.
     *
     * @param adapter SPI adapter.
     * @param srvOp {@code True} if communicates with server node.
     * @param lastRelatedOperationTime Time of last related operation in nanos. Ignored if negative, 0 or
     * {@code adapter.failureDetectionTimeoutEnabled()} is false.
     * @param absoluteThreshold Absolute time threshold (nanos) which must not be reached. Ignored if negative or 0.
     */
    public IgniteSpiOperationTimeoutHelper(IgniteSpiAdapter adapter, boolean srvOp, long lastRelatedOperationTime,
        long absoluteThreshold) {
        timeoutEnabled = adapter.failureDetectionTimeoutEnabled();

        if (timeoutEnabled) {
            long timeout = (lastRelatedOperationTime > 0 ? lastRelatedOperationTime : System.nanoTime()) +
                U.millisToNanos(srvOp ? adapter.failureDetectionTimeout() : adapter.clientFailureDetectionTimeout());

            if (absoluteThreshold > 0 && timeout > absoluteThreshold)
                timeout = absoluteThreshold;

            timeoutThreshold = timeout;
        } else {
            // Save absolute threshold if it is set.
            timeoutThreshold = absoluteThreshold > 0 ? absoluteThreshold : 0;
        }
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
        long now = System.nanoTime();

        long left;

        if (timeoutEnabled)
            left = timeoutThreshold - now;
        else {
            left = U.millisToNanos(dfltTimeout);

            if (timeoutThreshold > 0 && now + left >= timeoutThreshold)
                left = timeoutThreshold - now;
        }

        if (left <= 0)
            throw new IgniteSpiOperationTimeoutException("Network operation timed out.");

        return U.nanosToMillis(left);
    }

    /**
     * Checks whether the given {@link Exception} is a timeout.
     *
     * @param e Exception to check.
     * @return {@code True} if given exception is a timeout. {@code False} otherwise.
     */
    public boolean checkFailureTimeoutReached(Exception e) {
        return X.hasCause(e, IgniteSpiOperationTimeoutException.class, SocketTimeoutException.class, SocketException.class);
    }
}
