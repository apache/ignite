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

package org.apache.ignite.internal.util.nio;

import java.nio.channels.Selector;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgnitePredicate;

/**
 *
 */
interface GridNioWorker {
    /**
     * @param req Change request.
     */
    void offer(SessionChangeRequest req);

    /**
     * @param ses Session to register write interest for.
     */
    void registerWrite(GridNioSession ses);

    /**
     * Resets the worker counters.
     */
    void reset();

    /**
     * @return Worker sessions.
     */
    <T extends GridNioSession> Set<T> sessions();

    /**
     * Bytes received since rebalancing.
     */
    long bytesReceivedSinceRebalancing();

    /**
     * Bytes sent since rebalancing.
     */
    long bytesSentSinceRebalancing();

    /**
     * Bytes received total.
     */
    long bytesReceivedTotal();

    /**
     * Bytes sent since total.
     */
    long bytesSentTotal();

    /**
     * @return Selector, used by this worker.
     */
    Selector selector();

    /**
     * Registers a new session.
     * @param future Connection Operation future.
     */
    void register(ConnectionOperationFuture future);

    /**
     * Dumps worker statistics.
     * @param sb String builder
     * @param p Predicate which determines should Session info be included to output or not.
     * @param shortInfo True if short format should be used.
     */
    void dumpStats(StringBuilder sb, IgnitePredicate<GridNioSession> p, boolean shortInfo);

    /**
     * Closes the session and all associated resources, then notifies the listener.
     *
     * @param ses Session to be closed.
     * @param reason Exception to be passed to the listener, if any.
     * @return {@code True} if this call closed the ses.
     */
    boolean closeSession(GridNioSession ses, IgniteCheckedException reason);

    /**
     * @return Worker index.
     */
    int idx();
}
