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
 */
public class IgniteSpiOperationTimeoutHelper {
    /** */
    private long lastOperStartTs;

    /** */
    private long timeout;

    /** */
    private final boolean failureDetectionTimeoutEnabled;

    /** */
    private final long failureDetectionTimeout;

    /**
     * Constructor.
     *
     * @param adapter SPI adapter.
     */
    public IgniteSpiOperationTimeoutHelper(IgniteSpiAdapter adapter) {
        failureDetectionTimeoutEnabled = adapter.failureDetectionTimeoutEnabled();
        failureDetectionTimeout = adapter.failureDetectionTimeout();
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
        if (!failureDetectionTimeoutEnabled)
            return dfltTimeout;

        if (lastOperStartTs == 0) {
            timeout = failureDetectionTimeout;
            lastOperStartTs = U.currentTimeMillis();
        }
        else {
            long curTs = U.currentTimeMillis();

            timeout = timeout - (curTs - lastOperStartTs);

            lastOperStartTs = curTs;

            if (timeout <= 0)
                throw new IgniteSpiOperationTimeoutException("Network operation timed out. Increase " +
                    "'failureDetectionTimeout' configuration property [failureDetectionTimeout="
                    + failureDetectionTimeout + ']');
        }

        return timeout;
    }

    /**
     * Checks whether the given {@link Exception} is generated because failure detection timeout has been reached.
     *
     * @param e Exception.
     * @return {@code true} if failure detection timeout is reached, {@code false} otherwise.
     */
    public boolean checkFailureTimeoutReached(Exception e) {
        if (!failureDetectionTimeoutEnabled)
            return false;

        return e instanceof IgniteSpiOperationTimeoutException || e instanceof SocketTimeoutException ||
            X.hasCause(e, IgniteSpiOperationTimeoutException.class, SocketException.class);
    }
}